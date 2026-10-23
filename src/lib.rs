#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![allow(clippy::test_attr_in_doctest)]
#![allow(clippy::needless_doctest_main)]
#![doc = include_str!("../README.md")]

mod error;
mod index;
mod page;

use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
    io::Write,
    path::{Path, PathBuf},
};

pub use error::*;
use index::{ProbableIndex, ZeboIndex};
use page::{DOCUMENT_INDEX_OFFSET, ZeboPage, ZeboPageHeader};

pub use crate::page::{CompactPageStats, ZeboPageReservedSpace};

#[derive(Debug)]
pub struct CompactStats {
    pub pages_compacted: u32,
    pub pages_skipped: u32,
    pub total_bytes_reclaimed: u64,
}

#[derive(Debug, PartialEq)]
pub struct ZeboInfo {
    pub max_doc_per_page: u32,
    pub page_size: u64,
    pub document_count: u64,
    pub page_headers: Vec<ZeboPageHeader>,
}

pub trait DocumentId: Ord + Eq + std::hash::Hash + Debug + Copy {
    fn as_u64(&self) -> u64;
    fn from_u64(id: u64) -> Self;
}

/// Zebo is a document storage system that stores documents in pages inside a directory:
/// - Each page is a file that contains maximum `MAX_DOC_PER_PAGE` number of documents.
/// - Each page holds at most `PAGE_SIZE` bytes of document payload; the fixed-size index
///   at the start of the page is not counted.
/// - Each page file stays below 4 GiB, since payload offsets are stored as `u32`.
///
/// In case a document is larger than `PAGE_SIZE`, a new page is created.
/// In that case, the page size will higher than `PAGE_SIZE` bytes.
pub struct Zebo<const MAX_DOC_PER_PAGE: u32, const PAGE_SIZE: u64, DocId> {
    base_dir: PathBuf,
    next_page_id: u64,
    index: ZeboIndex<DocId>,
    current_page: Option<CurrentPage>,
    remove_documents_cache: HashMap<PageId, Vec<(u64, ProbableIndex)>>,
}

/// The page new batches are reserved on.
struct CurrentPage {
    page: ZeboPage,
    /// Highest document id reserved on the page, or `None` while it is empty.
    max_document_id: Option<u64>,
}

impl<const MAX_DOC_PER_PAGE: u32, const PAGE_SIZE: u64, DocId: DocumentId>
    Zebo<MAX_DOC_PER_PAGE, PAGE_SIZE, DocId>
{
    pub fn try_new<P: AsRef<Path>>(base_dir: P) -> Result<Self> {
        assert!(
            MAX_DOC_PER_PAGE > 0,
            "MAX_DOC_PER_PAGE must be greater than 0"
        );
        assert!(
            PAGE_SIZE > DOCUMENT_INDEX_OFFSET,
            "PAGE_SIZE must be greater than {DOCUMENT_INDEX_OFFSET}"
        );

        let base_dir: PathBuf = base_dir.as_ref().to_path_buf();

        if let Err(error) = std::fs::create_dir(&base_dir)
            && error.kind() != std::io::ErrorKind::AlreadyExists
        {
            return Err(ZeboError::CannotCreateBaseDir {
                inner_error: error,
                base_dir,
            });
        }

        if !base_dir.is_dir() {
            return Err(ZeboError::BaseDirIsNotDir { base_dir });
        }

        let index_dir_path = base_dir.join("index");
        let metadata = std::fs::metadata(&index_dir_path);
        let (index, page_id, current_page) = match metadata {
            Ok(metadata) => {
                if !metadata.is_dir() {
                    return Err(ZeboError::BaseDirIsNotDir { base_dir });
                }
                let index = ZeboIndex::try_load(index_dir_path)?;

                match index.get_page_ids()?.into_iter().max() {
                    Some(page_id) => {
                        let page = load_page(&base_dir, page_id, Mode::Change)?;
                        let max_document_id = page.get_max_document_id()?;

                        (
                            index,
                            Some(page_id),
                            Some(CurrentPage {
                                page,
                                max_document_id,
                            }),
                        )
                    }
                    None => (index, None, None),
                }
            }
            Err(error) => {
                if error.kind() == std::io::ErrorKind::NotFound {
                    let index = ZeboIndex::try_new(index_dir_path)?;
                    (index, None, None)
                } else {
                    return Err(ZeboError::BaseDirIsNotDir {
                        base_dir: index_dir_path,
                    });
                }
            }
        };

        Ok(Zebo {
            base_dir,
            index,
            next_page_id: page_id.map(|id| id.0 + 1).unwrap_or(0),
            current_page,
            remove_documents_cache: HashMap::new(),
        })
    }

    /// Reserves space for `docs` in the current page, first rotating to a new page when
    /// the current one is out of document slots, would hold more than `PAGE_SIZE` payload
    /// bytes (the fixed-size index is not counted), would push its `u32` payload offset
    /// past `u32::MAX`, or has inconsistent offsets. A fresh page accepts its first batch
    /// even when the batch alone exceeds `PAGE_SIZE`.
    ///
    /// Lookups route an id to the last page whose starting id is `<=` it, so ids must
    /// arrive in ascending order:
    /// - `docs` must be strictly ascending by id (no duplicates). Otherwise the batch is
    ///   refused with `UnsortedBatch { position }` and nothing is written; sorting and
    ///   deduplicating is left to the caller.
    /// - A batch whose first id is below the current page's starting id, or not above
    ///   the highest id already reserved on it, is still accepted, so that replaying
    ///   earlier inserts keeps working, and a warning is logged. Without a rotation it
    ///   goes into the current page; ids below the page's starting id are then routed to
    ///   an earlier page, which returns its older copy, or nothing below the first page.
    ///   If the page must rotate, the new page starts just above the highest id on the
    ///   closing page instead of at the batch's first id, keeping page starts strictly
    ///   ascending; batch ids below that start are routed to earlier pages the same way.
    ///   If a greater start cannot be represented by `DocId`, the rotation is refused
    ///   with `PageStartOverflow { last_id }` before any file is changed.
    pub fn reserve_space_for<'docs, Doc: Document>(
        &mut self,
        docs: &'docs [(DocId, Doc)],
    ) -> Result<ZeboPageReservedSpace<'docs, DocId, Doc>> {
        let document_count = u32::try_from(docs.len()).unwrap_or(u32::MAX);
        if document_count > MAX_DOC_PER_PAGE {
            return Err(ZeboError::TooManyDocuments {
                max: MAX_DOC_PER_PAGE,
                got: document_count,
            });
        }
        if docs.is_empty() {
            return Err(ZeboError::NoDocToAdd);
        }
        if let Some(position) = docs.windows(2).position(|w| w[0].0 >= w[1].0) {
            return Err(ZeboError::UnsortedBatch {
                position: position + 1,
            });
        }
        let first_id = docs[0].0.as_u64();
        let last_id = docs[docs.len() - 1].0.as_u64();

        let batch_bytes: u64 = docs.iter().map(|(_, doc)| doc.len() as u64).sum();
        // Payload offsets are u32: a batch that does not fit even in a fresh page is refused.
        if ZeboPage::header_size(MAX_DOC_PER_PAGE).saturating_add(batch_bytes) > u32::MAX as u64 {
            return Err(ZeboError::NotEnoughSpace {
                limit: u32::MAX,
                new_allocation_requested: u32::try_from(batch_bytes).unwrap_or(u32::MAX),
            });
        }

        let current_page_id = self.next_page_id.saturating_sub(1);
        let mut next_page_start = first_id;
        if let Some(CurrentPage {
            page,
            max_document_id,
        }) = self.current_page.as_mut()
        {
            let used_slots = page.next_available_header_offset;
            // A page created with a smaller limit runs out of slots at its own limit.
            let remain = MAX_DOC_PER_PAGE
                .min(page.document_limit())
                .saturating_sub(used_slots);
            let header_size = ZeboPage::header_size(page.document_limit());
            let next_offset = page.get_next_available_document_offset()? as u64;
            let batch_end = next_offset.saturating_add(batch_bytes);
            let payload_end = next_offset
                .saturating_sub(header_size)
                .saturating_add(batch_bytes);

            let out_of_slots = remain < document_count;
            let over_page_size = used_slots > 0 && payload_end > PAGE_SIZE;
            let over_offset_limit = batch_end > u32::MAX as u64;
            let inconsistent = Self::has_inconsistent_offsets(current_page_id, page, next_offset)?;

            let page_start = page.starting_document_id;
            if out_of_slots || over_page_size || over_offset_limit || inconsistent {
                // The new page must start above every id routed to this page or earlier
                // ones, or `ZeboIndex::get_pages` could no longer binary-search the starts.
                let floor = max_document_id.map_or(page_start, |max| max.max(page_start));
                if first_id <= floor {
                    let start = floor
                        .checked_add(1)
                        .filter(|start| DocId::from_u64(*start).as_u64() == *start)
                        .ok_or(ZeboError::PageStartOverflow { last_id: floor })?;
                    next_page_start = start;
                    tracing::warn!(
                        first_id,
                        new_page_start = next_page_start,
                        ids_below_new_page_start =
                            docs.partition_point(|(id, _)| id.as_u64() < next_page_start),
                        "Batch starts at or below ids already stored; the new page starts above them"
                    );
                }
                page.close()?;
                self.current_page = None;
            } else if first_id < page_start || max_document_id.is_some_and(|max| first_id <= max) {
                tracing::warn!(
                    first_id,
                    last_id,
                    page_start,
                    page_max_id = ?max_document_id,
                    "Batch starts below the current page or at or below an id already on it"
                );
            }
        }

        if self.current_page.is_none() {
            self.build_next_page(DocId::from_u64(next_page_start))?;
        }

        let current_page = self.current_page.as_mut().expect("Set above");
        let reserved_page = current_page.page.reserve(docs)?;
        current_page.max_document_id = Some(
            current_page
                .max_document_id
                .map_or(last_id, |max| max.max(last_id)),
        );

        Ok(reserved_page)
    }

    /// Whether the page's next-write offset wrapped (see
    /// `ZeboPage::has_inconsistent_offsets`): writing there would overwrite existing
    /// payloads, so such a page must never be written to again.
    fn has_inconsistent_offsets(page_id: u64, page: &ZeboPage, next_offset: u64) -> Result<bool> {
        let file_len = page.file_len()?;
        let inconsistent = page.has_inconsistent_offsets(next_offset, file_len);
        if inconsistent {
            tracing::warn!(
                page_id,
                next_offset,
                file_len,
                header_size = ZeboPage::header_size(page.document_limit()),
                "Page next-write offset is inconsistent with its file length; rotating to a new page"
            );
        }
        Ok(inconsistent)
    }

    /// Removes documents associated to the given ids
    pub fn remove_documents(&mut self, doc_ids: Vec<DocId>, clean_data: bool) -> Result<u32> {
        self.remove_documents_cache.clear();
        self.index
            .get_pages(doc_ids, &mut self.remove_documents_cache)?;

        let mut removed = 0;
        for (page_id, documents_to_delete) in &self.remove_documents_cache {
            let mut page = load_page(&self.base_dir, *page_id, Mode::Change)?;
            removed += page.delete_documents(documents_to_delete, clean_data)?;
        }

        Ok(removed)
    }

    /// Compacts all pages except the current one by rewriting them to reclaim
    /// space from deleted documents. The data region is compacted while header
    /// slots (including deleted sentinel entries) are preserved.
    pub fn compact(&mut self) -> Result<CompactStats> {
        let page_ids = self.index.get_page_ids()?;
        let current_page_id = if self.current_page.is_some() {
            Some(PageId(self.next_page_id - 1))
        } else {
            None
        };

        let mut stats = CompactStats {
            pages_compacted: 0,
            pages_skipped: 0,
            total_bytes_reclaimed: 0,
        };

        let mut buf = Vec::new();

        for page_id in page_ids {
            if Some(page_id) == current_page_id {
                stats.pages_skipped += 1;
                continue;
            }

            let page = load_page(&self.base_dir, page_id, Mode::Read)?;

            // Check if compaction would reclaim space: compare actual file size
            // against header size + total live document data
            let file_size =
                std::fs::metadata(self.base_dir.join(format!("page_{}.zebo", page_id.0)))
                    .map_err(ZeboError::OperationError)?
                    .len();
            let header_size = ZeboPage::header_size(page.document_limit());
            let live_data_size = page.live_data_size()?;
            let compacted_size = header_size + live_data_size;
            if file_size <= compacted_size {
                stats.pages_skipped += 1;
                continue;
            }

            let temp_path = self
                .base_dir
                .join(format!("page_{}.zebo.compact_tmp", page_id.0));
            let original_path = self.base_dir.join(format!("page_{}.zebo", page_id.0));

            let result = (|| -> Result<Option<CompactPageStats>> {
                let mut temp_file = std::fs::File::options()
                    .create(true)
                    .write(true)
                    .read(true)
                    .truncate(true)
                    .open(&temp_path)
                    .map_err(ZeboError::OperationError)?;

                let page_stats = page.compact_to_file(&mut temp_file, &mut buf)?;
                Ok(page_stats)
            })();

            match result {
                Ok(None) => {
                    // The page cannot be compacted safely: leave it as it is.
                    let _ = std::fs::remove_file(&temp_path);
                    stats.pages_skipped += 1;
                }
                Ok(Some(page_stats)) => {
                    std::fs::rename(&temp_path, &original_path)
                        .map_err(ZeboError::OperationError)?;

                    #[cfg(unix)]
                    {
                        let dir = std::fs::File::open(&self.base_dir)
                            .map_err(ZeboError::OperationError)?;
                        dir.sync_all().map_err(ZeboError::OperationError)?;
                    }

                    stats.total_bytes_reclaimed += page_stats
                        .bytes_before
                        .saturating_sub(page_stats.bytes_after);
                    stats.pages_compacted += 1;
                }
                Err(e) => {
                    let _ = std::fs::remove_file(&temp_path);
                    return Err(e);
                }
            }
        }

        Ok(stats)
    }

    /// Returns an iterator with the order guarantees
    pub fn get_documents<I: IntoIterator<Item = DocId> + Clone>(
        &self,
        doc_ids: I,
    ) -> Result<impl Iterator<Item = Result<(DocId, Vec<u8>)>>> {
        let mut results: HashMap<PageId, Vec<(u64, ProbableIndex)>> = Default::default();
        self.index.get_pages(doc_ids, &mut results)?;

        // Sort pages which guarantee the order of documents
        let mut pages: Vec<_> = results.into_iter().collect();
        pages.sort_by_key(|(page_id, _)| page_id.0);

        let page_iterator = ZeboPageIterator::new(self, pages.into_iter());
        Ok(ZeboDocumentIterator {
            iter: page_iterator,
            current_v: None,
        })
    }

    /// Returns the highest document ID that was ever inserted, regardless of deletion status.
    ///
    /// This method returns the maximum document ID across all pages, including documents
    /// that have been deleted. It is useful for determining the ID range or the next
    /// available ID for new documents.
    ///
    /// # Returns
    /// - `Ok(None)` if no documents have ever been inserted
    /// - `Ok(Some(id))` with the highest document ID, even if that document is deleted
    pub fn get_last_inserted_document_id(&self) -> Result<Option<DocId>> {
        let mut page_ids = self.index.get_page_ids()?;
        page_ids.sort();
        page_ids.reverse();

        // Iterate through pages from highest to lowest to find the maximum document ID
        for page_id in page_ids {
            let page = load_page(&self.base_dir, page_id, Mode::Read)?;
            if let Some(max_id) = page.get_max_document_id()? {
                return Ok(Some(DocId::from_u64(max_id)));
            }
        }

        // No documents found in any page
        Ok(None)
    }

    /// Returns the document associated to the given id
    pub fn get_document(&self, doc_id: DocId) -> Result<Option<Vec<u8>>> {
        let output = self.get_documents(vec![doc_id])?.next();

        match output {
            Some(Ok((_, doc))) => Ok(Some(doc)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    /// Returns an iterator over all documents without order guarantees
    pub fn get_all_documents(&self) -> Result<impl Iterator<Item = Result<(DocId, Vec<u8>)>>> {
        let mut results: HashMap<PageId, Vec<(u64, ProbableIndex)>> = Default::default();

        // Get all page IDs
        let page_ids = self.index.get_page_ids()?;

        let page_ids = page_ids.into_iter().collect::<HashSet<_>>();
        let mut page_ids: Vec<_> = page_ids.into_iter().collect();
        page_ids.sort_by_key(|page_id| page_id.0);

        // For each page, get all document IDs
        for page_id in page_ids {
            let page = load_page(&self.base_dir, page_id, Mode::Read)?;
            let header = page.get_header()?;

            // Extract all document IDs and their probable indices from this page
            let mut page_documents = Vec::with_capacity(header.index.len());
            for (doc_id, _, _) in header.index {
                // Ids below the page start (replayed batches) estimate slot 0 instead of underflowing.
                let probable_index =
                    ProbableIndex(doc_id.saturating_sub(page.starting_document_id));
                page_documents.push((doc_id, probable_index));
            }

            if !page_documents.is_empty() {
                results.insert(page_id, page_documents);
            }
        }

        let page_iterator = ZeboPageIterator::new(self, results.into_iter());
        Ok(ZeboDocumentIterator {
            iter: page_iterator,
            current_v: None,
        })
    }

    /// Returns the total document count
    pub fn get_document_count(&self) -> Result<u32> {
        let mut total_count = 0;
        for page_id in 0..self.next_page_id {
            let page = load_page(&self.base_dir, PageId(page_id), Mode::Read)?;
            total_count += page.get_document_count()?;
        }

        Ok(total_count)
    }

    pub fn get_all_pages(&self) -> Result<Vec<(u64, PageId)>> {
        self.index.get_all_pages()
    }

    /// Returns information related the this Zebo instance
    pub fn get_info(&self) -> Result<ZeboInfo> {
        let mut page_headers = Vec::with_capacity(self.next_page_id as usize);

        let mut document_count: u64 = 0;

        let pages = self.index.get_page_ids()?;
        for page_id in pages {
            let page = load_page(&self.base_dir, page_id, Mode::Read)?;
            let header = page.get_header()?;
            document_count += header.document_count as u64;
            page_headers.push(header);
        }

        Ok(ZeboInfo {
            page_headers,
            max_doc_per_page: MAX_DOC_PER_PAGE,
            page_size: PAGE_SIZE,
            document_count,
        })
    }

    fn build_next_page(&mut self, doc_id: DocId) -> Result<()> {
        if let Some(CurrentPage { mut page, .. }) = self.current_page.take() {
            // Close previous page
            page.close()?;
        }

        let new_page_id = PageId(self.next_page_id);
        self.index.new_page(doc_id, new_page_id)?;

        let page_file_path = self.base_dir.join(format!("page_{}.zebo", new_page_id.0));
        let page_file = std::fs::File::options()
            .create(true)
            .write(true)
            .read(true)
            // This is a new page, so it is safe
            .truncate(true)
            .open(&page_file_path);
        let page_file = match page_file {
            Ok(file) => file,
            Err(error) => {
                return Err(ZeboError::OpenPageFileError {
                    inner_error: error,
                    page_file_path,
                });
            }
        };

        let page = ZeboPage::try_new(MAX_DOC_PER_PAGE, doc_id.as_u64(), page_file)?;
        self.current_page = Some(CurrentPage {
            page,
            max_document_id: None,
        });
        self.next_page_id += 1;

        Ok(())
    }

    #[cfg_attr(coverage_nightly, coverage(off))]
    #[allow(clippy::too_many_arguments)]
    pub fn debug_content_with_options(
        &self,
        page_id: u64,
        formatter: &mut dyn Write,
        skip_content_checks: bool,
        skip_document_content: bool,
        skip_header_info: bool,
        doc_id: Option<u64>,
        starting_doc_id: Option<u64>,
    ) -> Result<()> {
        let page = load_page(&self.base_dir, PageId(page_id), Mode::Read)?;
        page.debug_content_with_options(
            formatter,
            skip_content_checks,
            skip_document_content,
            skip_header_info,
            doc_id,
            starting_doc_id,
        )?;
        Ok(())
    }
}

fn load_page(base_dir: &Path, page_id: PageId, mode: Mode) -> Result<ZeboPage> {
    // close page and create a new one
    let page_file_path = base_dir.join(format!("page_{}.zebo", page_id.0));

    let mut options = std::fs::File::options();
    match mode {
        Mode::Read => {
            // Same of File::open
            options.read(true);
        }
        Mode::Change => {
            options.write(true).read(true);
        }
    };

    let page_file = match options.open(&page_file_path) {
        Ok(file) => file,
        Err(error) => {
            return Err(ZeboError::OpenPageFileError {
                inner_error: error,
                page_file_path,
            });
        }
    };

    ZeboPage::try_load(page_file)
}

#[cfg_attr(coverage_nightly, coverage(off))]
impl<const MAX_DOC_PER_PAGE: u32, const PAGE_SIZE: u64, DocId: DocumentId> Debug
    for Zebo<MAX_DOC_PER_PAGE, PAGE_SIZE, DocId>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut headers = Vec::new();
        for page_id in 0..self.next_page_id {
            let page =
                load_page(&self.base_dir, PageId(page_id), Mode::Read).expect("Cannot open page");
            headers.push(page.get_header().expect("Cannot get page header"));
        }

        f.debug_struct("Zebo")
            .field("base_dir", &self.base_dir)
            .field("info", &self.get_info().expect("Cannot get info"))
            .finish()
    }
}

pub struct ZeboDocumentIterator<
    'zebo,
    const MAX_DOC_PER_PAGE: u32,
    const PAGE_SIZE: u64,
    DocId: DocumentId,
    I: Iterator<Item = (PageId, Vec<(u64, ProbableIndex)>)>,
> {
    iter: ZeboPageIterator<'zebo, MAX_DOC_PER_PAGE, PAGE_SIZE, DocId, Vec<(u64, ProbableIndex)>, I>,
    current_v: Option<Vec<(DocId, Vec<u8>)>>,
}

impl<
    const MAX_DOC_PER_PAGE: u32,
    const PAGE_SIZE: u64,
    DocId: DocumentId,
    I: Iterator<Item = (PageId, Vec<(u64, ProbableIndex)>)>,
> Iterator for ZeboDocumentIterator<'_, MAX_DOC_PER_PAGE, PAGE_SIZE, DocId, I>
{
    type Item = Result<(DocId, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(docs) = self.current_v.as_ref()
            && docs.is_empty()
        {
            self.current_v = None;
        }

        if self.current_v.is_none() {
            let next = self.iter.next();
            match next {
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
                Some(Ok((page, d))) => {
                    // This allocates a vector for each page
                    // but, conceptually, we could avoid this using streams
                    // and iterating over the documents
                    // Anyway, that approach requires lifetimes handling
                    // TODO: use streams

                    match page.get_documents(&d) {
                        Ok(v) => {
                            self.current_v = Some(v);
                        }
                        Err(e) => {
                            return Some(Err(e));
                        }
                    };
                }
            }
        }

        let docs = self.current_v.as_mut().expect("Set above");
        let (doc_id, doc) = match docs.pop() {
            None => {
                self.current_v = None;
                return self.next();
            }
            Some(a) => a,
        };
        Some(Ok((doc_id, doc)))
    }
}

pub struct ZeboPageIterator<
    'zebo,
    const MAX_DOC_PER_PAGE: u32,
    const PAGE_SIZE: u64,
    DocId: DocumentId,
    V,
    I: Iterator<Item = (PageId, V)>,
> {
    zebo: &'zebo Zebo<MAX_DOC_PER_PAGE, PAGE_SIZE, DocId>,
    document_offsets_per_page: I,
}

impl<
    'zebo,
    const MAX_DOC_PER_PAGE: u32,
    const PAGE_SIZE: u64,
    DocId: DocumentId,
    V,
    I: Iterator<Item = (PageId, V)>,
> ZeboPageIterator<'zebo, MAX_DOC_PER_PAGE, PAGE_SIZE, DocId, V, I>
{
    fn new(
        zebo: &'zebo Zebo<MAX_DOC_PER_PAGE, PAGE_SIZE, DocId>,
        document_offsets_per_page: I,
    ) -> Self {
        Self {
            zebo,
            document_offsets_per_page,
        }
    }
}

impl<
    const MAX_DOC_PER_PAGE: u32,
    const PAGE_SIZE: u64,
    DocId: DocumentId,
    V,
    I: Iterator<Item = (PageId, V)>,
> Iterator for ZeboPageIterator<'_, MAX_DOC_PER_PAGE, PAGE_SIZE, DocId, V, I>
{
    type Item = Result<(ZeboPage, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        let (page_id, v): (PageId, V) = self.document_offsets_per_page.next()?;

        let page = match load_page(&self.zebo.base_dir, page_id, Mode::Read) {
            Ok(page) => page,
            Err(e) => return Some(Err(e)),
        };
        Some(Ok((page, v)))
    }
}

pub trait Document {
    fn as_bytes(&self, v: &mut Vec<u8>);
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T: AsRef<[u8]>> Document for T {
    fn as_bytes(&self, v: &mut Vec<u8>) {
        v.extend(self.as_ref());
    }

    fn len(&self) -> usize {
        self.as_ref().len()
    }
}

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug, PartialOrd, Ord)]
pub struct PageId(u64);

enum Mode {
    Read,
    Change,
}

enum Version {
    V1,
}

impl From<Version> for u8 {
    fn from(val: Version) -> Self {
        match val {
            Version::V1 => 1,
        }
    }
}

impl DocumentId for u32 {
    fn as_u64(&self) -> u64 {
        *self as u64
    }
    fn from_u64(id: u64) -> Self {
        id as u32
    }
}
impl DocumentId for u64 {
    fn as_u64(&self) -> u64 {
        *self
    }
    fn from_u64(id: u64) -> Self {
        id
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    pub fn prepare_test_dir() -> PathBuf {
        let num1 = vec![2, 3];
        let address1 = &num1 as *const Vec<i32>;
        let number1 = address1 as i32;

        let test_dir = std::env::temp_dir().join(format!("zebo_test_{number1}"));
        // let test_dir = std::env::current_dir().unwrap().join("zebo_test");
        if test_dir.exists() {
            std::fs::remove_dir_all(&test_dir).unwrap();
        }
        std::fs::create_dir_all(&test_dir).unwrap();
        test_dir
    }

    #[test]
    fn test_zebo_simple() {
        let test_dir = prepare_test_dir();

        let mut zebo: Zebo<5, 2048, u32> = Zebo::<5, 2048, _>::try_new(test_dir.clone()).unwrap();

        let docs = vec![(1, "Hello")];
        let space = zebo.reserve_space_for(&docs).unwrap();

        // Drop Zebo instance
        drop(zebo);

        // I can write to file without having a reference to Zebo
        space.write_all().unwrap();

        let zebo: Zebo<5, 2048, u32> = Zebo::<5, 2048, _>::try_new(test_dir.clone()).unwrap();

        let doc = zebo.get_document(1).unwrap().unwrap();
        assert_eq!(doc, b"Hello");

        let docs: Result<Vec<_>> = zebo.get_documents(vec![1]).unwrap().collect();
        let docs = docs.unwrap();
        assert_eq!(docs, vec![(1, b"Hello".to_vec())]);

        assert_eq!(zebo.get_document_count().unwrap(), 1);
    }

    #[test]
    fn test_zebo_concurrent() {
        let test_dir = prepare_test_dir();

        let mut zebo: Zebo<5, 2048, u32> = Zebo::<5, 2048, _>::try_new(test_dir.clone()).unwrap();

        let docs1 = vec![(1, "Hello")];
        let docs2 = vec![(2, "World")];
        let space1 = zebo.reserve_space_for(&docs1).unwrap();
        let space2 = zebo.reserve_space_for(&docs2).unwrap();

        // Drop Zebo instance
        drop(zebo);

        space1.write_all().unwrap();
        space2.write_all().unwrap();

        let zebo: Zebo<5, 2048, u32> = Zebo::<5, 2048, _>::try_new(test_dir.clone()).unwrap();

        let doc = zebo.get_document(1).unwrap().unwrap();
        assert_eq!(doc, b"Hello");
        let doc = zebo.get_document(2).unwrap().unwrap();
        assert_eq!(doc, b"World");

        let docs: Result<Vec<_>> = zebo.get_documents(vec![2, 1]).unwrap().collect();
        let mut docs = docs.unwrap();
        docs.sort_by_key(|(id, _)| *id);
        assert_eq!(docs, vec![(1, b"Hello".to_vec()), (2, b"World".to_vec())]);

        assert_eq!(zebo.get_document_count().unwrap(), 2);
    }

    #[test]
    fn test_zebo_multi() {
        let test_dir = prepare_test_dir();

        let mut zebo: Zebo<5, 2048, u32> = Zebo::<5, 2048, _>::try_new(test_dir.clone()).unwrap();

        let docs = vec![
            (1, "Hello".to_string()),
            (2, "World".to_string()),
            (3, "This".to_string()),
            (4, "Is".to_string()),
            (5, "Zebo".to_string()),
        ];
        let space = zebo.reserve_space_for(&docs).unwrap();

        // Drop Zebo instance
        drop(zebo);

        // I can write to file without having a reference to Zebo
        space.write_all().unwrap();

        let zebo: Zebo<5, 2048, u32> = Zebo::<5, 2048, _>::try_new(test_dir.clone()).unwrap();

        for i in 1..=5 {
            let expected = docs.iter().find(|(id, _)| *id == i).unwrap().1.as_bytes();
            let doc = zebo.get_document(i).unwrap().unwrap();
            assert_eq!(doc, expected);

            let docs: Result<Vec<_>> = zebo.get_documents(vec![i]).unwrap().collect();
            let docs = docs.unwrap();
            assert_eq!(docs, vec![(i, expected.to_vec())]);
        }

        let found_docs: Result<Vec<_>> = zebo.get_documents(vec![1, 2, 3, 4, 5]).unwrap().collect();
        let mut found_docs = found_docs.unwrap();
        found_docs.sort_by_key(|(id, _)| *id);
        assert_eq!(
            docs,
            found_docs
                .iter()
                .map(|(id, text)| (*id, String::from_utf8(text.clone()).unwrap()))
                .collect::<Vec<_>>()
        );

        assert_eq!(zebo.get_document_count().unwrap(), 5);
    }

    #[test]
    fn test_zebo_new_page() {
        let test_dir = prepare_test_dir();

        let mut zebo: Zebo<5, 2048, u32> = Zebo::<5, 2048, _>::try_new(test_dir.clone()).unwrap();

        let docs1 = vec![
            (1, "Hello".to_string()),
            (2, "World".to_string()),
            (3, "This".to_string()),
            (4, "Is".to_string()),
        ];
        let docs2 = vec![
            (5, "Zebo".to_string()),
            (6, "New".to_string()),
            (7, "Page".to_string()),
        ];
        let space1 = zebo.reserve_space_for(&docs1).unwrap();
        let space2 = zebo.reserve_space_for(&docs2).unwrap();

        drop(zebo);

        space1.write_all().unwrap();
        space2.write_all().unwrap();

        let zebo: Zebo<5, 2048, u32> = Zebo::<5, 2048, _>::try_new(test_dir.clone()).unwrap();

        for i in 1..=7 {
            let docs = if i > 4 { &docs2 } else { &docs1 };
            let expected = docs.iter().find(|(id, _)| *id == i).unwrap().1.as_bytes();
            let doc = zebo.get_document(i).unwrap().unwrap();
            assert_eq!(doc, expected);

            let docs: Result<Vec<_>> = zebo.get_documents(vec![i]).unwrap().collect();
            let docs = docs.unwrap();
            assert_eq!(docs, vec![(i, expected.to_vec())]);
        }

        let found_docs: Result<Vec<_>> = zebo
            .get_documents(vec![1, 2, 3, 4, 5, 6, 7])
            .unwrap()
            .collect();
        let mut found_docs = found_docs.unwrap();
        found_docs.sort_by_key(|(id, _)| *id);
        assert_eq!(
            docs1
                .into_iter()
                .chain(docs2.into_iter())
                .collect::<Vec<_>>(),
            found_docs
                .iter()
                .map(|(id, text)| (*id, String::from_utf8(text.clone()).unwrap()))
                .collect::<Vec<_>>()
        );

        assert_eq!(zebo.get_document_count().unwrap(), 7);
    }

    #[test]
    fn test_zebo_empty_docs() {
        let test_dir = prepare_test_dir();

        let mut zebo: Zebo<5, 2048, u32> = Zebo::<5, 2048, _>::try_new(test_dir.clone()).unwrap();

        let docs1 = vec![(1, "".to_string()), (2, "".to_string())];
        let space1 = zebo.reserve_space_for(&docs1).unwrap();

        drop(zebo);

        space1.write_all().unwrap();

        let zebo: Zebo<5, 2048, u32> = Zebo::<5, 2048, _>::try_new(test_dir.clone()).unwrap();

        for i in 1..=2 {
            let expected = docs1.iter().find(|(id, _)| *id == i).unwrap().1.as_bytes();
            let doc = zebo.get_document(i).unwrap().unwrap();
            assert_eq!(doc, expected);

            let docs: Result<Vec<_>> = zebo.get_documents(vec![i]).unwrap().collect();
            let docs = docs.unwrap();
            assert_eq!(docs, vec![(i, expected.to_vec())]);
        }

        let found_docs: Result<Vec<_>> = zebo.get_documents(vec![1, 2]).unwrap().collect();
        let mut found_docs = found_docs.unwrap();
        found_docs.sort_by_key(|(id, _)| *id);
        assert_eq!(
            docs1.into_iter().collect::<Vec<_>>(),
            found_docs
                .iter()
                .map(|(id, text)| (*id, String::from_utf8(text.clone()).unwrap()))
                .collect::<Vec<_>>()
        );

        assert_eq!(zebo.get_document_count().unwrap(), 2);
    }

    #[test]
    fn test_zebo_delete() {
        let test_dir = prepare_test_dir();

        let mut zebo: Zebo<5, 2048, u32> = Zebo::<5, 2048, _>::try_new(test_dir.clone()).unwrap();

        let docs = vec![
            (1, "01234567890".to_string()),
            (2, "ABCFE".to_string()),
            (3, "FGHIL".to_string()),
        ];
        zebo.reserve_space_for(&docs).unwrap().write_all().unwrap();

        let info = zebo.get_info().unwrap();
        assert_eq!(info.document_count, 3);

        zebo.remove_documents(vec![1, 2], true).unwrap();

        let err = zebo.get_document(1).unwrap();
        assert!(err.is_none());
        let err = zebo.get_document(2).unwrap();
        assert!(err.is_none());
        let doc = zebo.get_document(3).unwrap().unwrap();
        assert_eq!(doc, b"FGHIL");

        // Ignore unknown document
        zebo.remove_documents(vec![55], true).unwrap();

        let err = zebo.get_document(1).unwrap();
        assert!(err.is_none());
        let err = zebo.get_document(2).unwrap();
        assert!(err.is_none());
        let doc = zebo.get_document(3).unwrap().unwrap();
        assert_eq!(doc, b"FGHIL");

        // Ignore already deleted document
        zebo.remove_documents(vec![1, 2], true).unwrap();

        let err = zebo.get_document(1).unwrap();
        assert!(err.is_none());
        let err = zebo.get_document(2).unwrap();
        assert!(err.is_none());
        let doc = zebo.get_document(3).unwrap().unwrap();
        assert_eq!(doc, b"FGHIL");

        let info = zebo.get_info().unwrap();
        assert_eq!(info.document_count, 1);

        let docs = vec![
            (4, "MNOP".to_string()),
            (5, "QRTU".to_string()),
            (6, "VWXYZ".to_string()),
        ];
        zebo.reserve_space_for(&docs).unwrap().write_all().unwrap();

        let mut docs: Vec<_> = zebo
            .get_documents([1, 2, 3, 4, 5, 6])
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        docs.sort_by_key(|(id, _)| *id);
        assert_eq!(
            docs,
            vec![
                (3, b"FGHIL".to_vec()),
                (4, b"MNOP".to_vec()),
                (5, b"QRTU".to_vec()),
                (6, b"VWXYZ".to_vec()),
            ]
        );

        assert_eq!(zebo.get_document_count().unwrap(), 4);
    }

    #[test]
    fn test_zebo_get_all_documents() {
        let test_dir = prepare_test_dir();

        let mut zebo: Zebo<3, 2048, u32> = Zebo::<3, 2048, _>::try_new(test_dir.clone()).unwrap();

        zebo.reserve_space_for(&[(1, "first"), (2, "second"), (3, "third")])
            .unwrap()
            .write_all()
            .unwrap();
        zebo.reserve_space_for(&[(4, "fourth"), (5, "fifth"), (6, "sixth")])
            .unwrap()
            .write_all()
            .unwrap();

        // Get all documents and verify they're all returned
        let mut all_docs = zebo
            .get_all_documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        // Sort for comparison since order is not guaranteed
        all_docs.sort_by_key(|d| d.0);

        assert_eq!(all_docs.len(), 6);
        assert_eq!(
            all_docs,
            vec![
                (1, b"first".to_vec()),
                (2, b"second".to_vec()),
                (3, b"third".to_vec()),
                (4, b"fourth".to_vec()),
                (5, b"fifth".to_vec()),
                (6, b"sixth".to_vec()),
            ]
        );

        // Test with empty storage
        let test_dir_empty = prepare_test_dir();
        let zebo_empty: Zebo<2, 2048, u32> = Zebo::<2, 2048, _>::try_new(test_dir_empty).unwrap();

        let empty_docs: Vec<_> = zebo_empty
            .get_all_documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(empty_docs.len(), 0);
    }

    #[test]
    fn test_zebo_get_all_documents_with_deletions() {
        let test_dir = prepare_test_dir();

        let mut zebo: Zebo<5, 2048, u32> = Zebo::<5, 2048, _>::try_new(test_dir.clone()).unwrap();

        zebo.reserve_space_for(&[(1, "first"), (2, "second"), (3, "third"), (4, "fourth")])
            .unwrap()
            .write_all()
            .unwrap();

        // Remove some documents
        zebo.remove_documents(vec![2, 4], true).unwrap();

        let mut remaining_docs = zebo
            .get_all_documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        remaining_docs.sort_by_key(|d| d.0);

        assert_eq!(remaining_docs.len(), 2);
        assert_eq!(
            remaining_docs,
            vec![(1, b"first".to_vec()), (3, b"third".to_vec()),]
        );
    }

    #[test]
    fn test_zebo_with_gap() -> Result<()> {
        let test_dir = prepare_test_dir();

        let mut zebo: Zebo<4, 2048, u32> = Zebo::try_new(test_dir.clone()).unwrap();
        assert_eq!(zebo.next_page_id, 0);

        zebo.reserve_space_for(&[(1, "1"), (3, "3"), (5, "5")])
            .unwrap()
            .write_all()
            .unwrap();
        zebo.reserve_space_for(&[(7, "7"), (9, "9"), (11, "11")])
            .unwrap()
            .write_all()
            .unwrap();

        let info = zebo.get_info().unwrap();
        assert_eq!(info.document_count, 6);

        let output: Result<Vec<_>> = zebo
            .get_documents(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
            .unwrap()
            .collect();
        let mut output = output.unwrap();
        output.sort_by_key(|d| d.0);

        let info = zebo.get_info().unwrap();
        assert_eq!(info.page_headers.len(), 2);

        let docs = zebo
            .get_documents(vec![1, 3, 5, 6])
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(docs.len(), 3);

        let removed = zebo.remove_documents(vec![1, 2, 3, 4, 5], false).unwrap();
        assert_eq!(removed, 3);

        let docs = zebo
            .get_documents(vec![1, 3, 5, 6])
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(docs.len(), 0);

        let info = zebo.get_info().unwrap();
        assert_eq!(info.document_count, 3);

        zebo.reserve_space_for(&[(13, "13"), (15, "15")])
            .unwrap()
            .write_all()
            .unwrap();

        let info = zebo.get_info().unwrap();
        assert_eq!(info.document_count, 5);

        let mut docs = zebo
            .get_documents(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(docs.len(), 5);
        docs.sort_by_key(|d| d.0);
        assert_eq!(
            docs,
            vec![
                (7, b"7".to_vec()),
                (9, b"9".to_vec()),
                (11, b"11".to_vec()),
                (13, b"13".to_vec()),
                (15, b"15".to_vec())
            ]
        );

        Ok(())
    }

    #[test]
    fn test_real_example_3() {
        let test_dir = prepare_test_dir();

        // 1GB
        const PAGE_SIZE: u64 = 1024 * 1024 * 1024;

        let mut zebo = Zebo::<5, PAGE_SIZE, u64>::try_new(test_dir.clone())
            .expect("Failed to create Zebo instance");

        zebo.reserve_space_for(&[(
            1,
            MyDoc {
                id: "Document 1".to_string(),
                text: "This is the content of document 1.".to_string(),
            },
        )])
        .unwrap()
        .write_all()
        .unwrap();

        let info_before = zebo.get_info().unwrap();
        assert_eq!(info_before.page_headers.len(), 1);
        drop(zebo);

        let mut zebo = Zebo::<5, PAGE_SIZE, u64>::try_new(test_dir.clone())
            .expect("Failed to create Zebo instance");

        // Test realod of Zebo instance doesn't change the info
        let info_after = zebo.get_info().unwrap();
        assert_eq!(info_before, info_after);

        zebo.reserve_space_for(&[(
            4,
            MyDoc {
                id: "Document 4".to_string(),
                text: "This is the content of document 4.".to_string(),
            },
        )])
        .unwrap()
        .write_all()
        .unwrap();

        // Reload doesn't create a new page
        let info = zebo.get_info().unwrap();
        assert_eq!(info.page_headers.len(), 1);

        drop(zebo);

        let mut zebo =
            Zebo::<5, PAGE_SIZE, u64>::try_new(test_dir).expect("Failed to create Zebo instance");

        zebo.reserve_space_for(&[(
            5,
            MyDoc {
                id: "Document 5".to_string(),
                text: "This is the content of document 5.".to_string(),
            },
        )])
        .unwrap()
        .write_all()
        .unwrap();

        // Reload doesn't create a new page
        let info = zebo.get_info().unwrap();
        assert_eq!(info.page_headers.len(), 1);
        let docs: Vec<_> = info
            .page_headers
            .into_iter()
            .flat_map(|header| header.index.into_iter().map(|(doc_id, _, _)| doc_id))
            .collect();
        assert_eq!(docs, vec![1, 4, 5]);
    }

    #[test]
    fn test_empty() {
        let test_dir = prepare_test_dir();

        // 1GB
        const PAGE_SIZE: u64 = 1024 * 1024 * 1024;

        let zebo = Zebo::<5, PAGE_SIZE, u64>::try_new(test_dir.clone())
            .expect("Failed to create Zebo instance");

        let info_before = zebo.get_info().unwrap();
        assert_eq!(info_before.page_headers.len(), 0);
        drop(zebo);

        let zebo = Zebo::<5, PAGE_SIZE, u64>::try_new(test_dir.clone())
            .expect("Failed to create Zebo instance");

        // Test realod of Zebo instance doesn't change the info
        let info_after = zebo.get_info().unwrap();
        assert_eq!(info_before, info_after);
    }

    #[test]
    fn test_add_mized() {
        let test_dir = prepare_test_dir();

        let mut zebo = Zebo::<10, 128, u64>::try_new(test_dir.clone())
            .expect("Failed to create Zebo instance");

        zebo.reserve_space_for(&[
            (1, "text 1"),
            (2, "text 2"),
            (3, "text 3"),
            (4, "text 4"),
            (5, "text 5"),
            (6, "text 6"),
        ])
        .unwrap()
        .write_all()
        .unwrap();

        zebo.reserve_space_for(&[(7, "text 7")])
            .unwrap()
            .write_all()
            .unwrap();

        let mut output: Vec<_> = zebo
            .get_documents(vec![1, 2, 3, 4, 5, 6, 7])
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        output.sort_by_key(|(id, _)| *id);

        assert_eq!(
            &output,
            &[
                (1_u64, "text 1".as_bytes().to_vec()),
                (2_u64, "text 2".as_bytes().to_vec()),
                (3_u64, "text 3".as_bytes().to_vec()),
                (4_u64, "text 4".as_bytes().to_vec()),
                (5_u64, "text 5".as_bytes().to_vec()),
                (6_u64, "text 6".as_bytes().to_vec()),
                (7_u64, "text 7".as_bytes().to_vec()),
            ]
        );

        zebo.reserve_space_for(&[
            (8, "text 8"),
            (9, "text 9"),
            (10, "text 10"),
            (11, "text 11"),
            (12, "text 12"),
            (13, "text 13"),
        ])
        .unwrap()
        .write_all()
        .unwrap();

        let info = zebo.get_info().unwrap();
        assert_eq!(info.page_headers.len(), 2);
        assert_eq!(info.page_headers[0].document_count, 7);
        assert_eq!(info.page_headers[1].document_count, 6);
    }

    #[test]
    fn test_get_all_pages() {
        let test_dir = prepare_test_dir();

        let mut zebo = Zebo::<10, 128, u64>::try_new(test_dir.clone())
            .expect("Failed to create Zebo instance");

        zebo.reserve_space_for(&[
            (1, "text 1"),
            (2, "text 2"),
            (3, "text 3"),
            (4, "text 4"),
            (5, "text 5"),
            (6, "text 6"),
        ])
        .unwrap()
        .write_all()
        .unwrap();

        zebo.reserve_space_for(&[(7, "text 7")])
            .unwrap()
            .write_all()
            .unwrap();

        zebo.reserve_space_for(&[
            (8, "text 8"),
            (9, "text 9"),
            (10, "text 10"),
            (11, "text 11"),
            (12, "text 12"),
            (13, "text 13"),
        ])
        .unwrap()
        .write_all()
        .unwrap();

        let pages = zebo.get_all_pages().unwrap();
        assert_eq!(pages, vec![(1, PageId(0)), (8, PageId(1))]);
    }

    #[test]
    fn test_not_enough_max_doc_per_page() {
        let test_dir = prepare_test_dir();

        let mut zebo =
            Zebo::<1, 128, u64>::try_new(test_dir.clone()).expect("Failed to create Zebo instance");

        let output = zebo.reserve_space_for(&[(1, "text 1"), (2, "text 2")]);

        let err = output.unwrap_err();
        assert!(matches!(err, ZeboError::TooManyDocuments { .. }));
    }

    #[test]
    fn test_get_last_inserted_document_id_empty() {
        let test_dir = prepare_test_dir();
        let zebo: Zebo<5, 2048, u32> = Zebo::<5, 2048, _>::try_new(test_dir).unwrap();

        let result = zebo.get_last_inserted_document_id().unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_last_inserted_document_id_single_document() {
        let test_dir = prepare_test_dir();
        let mut zebo: Zebo<5, 2048, u32> = Zebo::<5, 2048, _>::try_new(test_dir).unwrap();

        zebo.reserve_space_for(&[(42, "test")])
            .unwrap()
            .write_all()
            .unwrap();

        let result = zebo.get_last_inserted_document_id().unwrap();
        assert_eq!(result, Some(42));
    }

    #[test]
    fn test_get_last_inserted_document_id_multiple_pages() {
        let test_dir = prepare_test_dir();
        let mut zebo: Zebo<3, 2048, u32> = Zebo::<3, 2048, _>::try_new(test_dir).unwrap();

        zebo.reserve_space_for(&[(1, "a"), (2, "b"), (3, "c")])
            .unwrap()
            .write_all()
            .unwrap();
        zebo.reserve_space_for(&[(4, "d"), (5, "e"), (6, "f")])
            .unwrap()
            .write_all()
            .unwrap();
        zebo.reserve_space_for(&[(7, "g"), (8, "h"), (9, "i")])
            .unwrap()
            .write_all()
            .unwrap();

        let result = zebo.get_last_inserted_document_id().unwrap();
        assert_eq!(result, Some(9));
    }

    #[test]
    fn test_get_last_inserted_document_id_with_gaps() {
        let test_dir = prepare_test_dir();
        let mut zebo: Zebo<5, 2048, u32> = Zebo::<5, 2048, _>::try_new(test_dir).unwrap();

        zebo.reserve_space_for(&[(1, "a"), (100, "b"), (500, "c"), (1000, "d")])
            .unwrap()
            .write_all()
            .unwrap();

        let result = zebo.get_last_inserted_document_id().unwrap();
        assert_eq!(result, Some(1000));
    }

    #[test]
    fn test_get_last_inserted_document_id_deleted_document() {
        let test_dir = prepare_test_dir();
        let mut zebo: Zebo<5, 2048, u32> = Zebo::<5, 2048, _>::try_new(test_dir).unwrap();

        zebo.reserve_space_for(&[(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")])
            .unwrap()
            .write_all()
            .unwrap();

        zebo.remove_documents(vec![5], true).unwrap();

        // Verify document 5 is actually deleted
        assert!(zebo.get_document(5).unwrap().is_none());

        // Should still return 5 as the last inserted ID
        let result = zebo.get_last_inserted_document_id().unwrap();
        assert_eq!(result, Some(5));
    }

    #[test]
    fn test_get_last_inserted_document_id_all_deleted() {
        let test_dir = prepare_test_dir();
        let mut zebo: Zebo<5, 2048, u32> = Zebo::<5, 2048, _>::try_new(test_dir).unwrap();

        zebo.reserve_space_for(&[(1, "a"), (2, "b"), (3, "c")])
            .unwrap()
            .write_all()
            .unwrap();

        zebo.remove_documents(vec![1, 2, 3], true).unwrap();

        let result = zebo.get_last_inserted_document_id().unwrap();
        assert_eq!(result, Some(3));
    }

    #[test]
    fn test_get_last_inserted_document_id_mixed_deleted_active() {
        let test_dir = prepare_test_dir();
        let mut zebo: Zebo<10, 2048, u32> = Zebo::<10, 2048, _>::try_new(test_dir).unwrap();

        zebo.reserve_space_for(&[(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f")])
            .unwrap()
            .write_all()
            .unwrap();

        zebo.remove_documents(vec![2, 4, 6], true).unwrap();

        let result = zebo.get_last_inserted_document_id().unwrap();
        assert_eq!(result, Some(6));
    }

    #[test]
    fn test_get_last_inserted_document_id_persistence() {
        let test_dir = prepare_test_dir();

        {
            let mut zebo: Zebo<5, 2048, u32> =
                Zebo::<5, 2048, _>::try_new(test_dir.clone()).unwrap();
            zebo.reserve_space_for(&[(1, "a"), (2, "b"), (3, "c")])
                .unwrap()
                .write_all()
                .unwrap();
        }

        // Reload from disk
        let zebo: Zebo<5, 2048, u32> = Zebo::<5, 2048, _>::try_new(test_dir).unwrap();
        let result = zebo.get_last_inserted_document_id().unwrap();
        assert_eq!(result, Some(3));
    }

    #[test]
    fn test_get_last_inserted_document_id_after_add_delete_add() {
        let test_dir = prepare_test_dir();
        let mut zebo: Zebo<5, 2048, u32> = Zebo::<5, 2048, _>::try_new(test_dir).unwrap();

        zebo.reserve_space_for(&[(1, "a"), (2, "b"), (3, "c")])
            .unwrap()
            .write_all()
            .unwrap();

        zebo.remove_documents(vec![3], true).unwrap();

        zebo.reserve_space_for(&[(4, "d"), (5, "e")])
            .unwrap()
            .write_all()
            .unwrap();

        let result = zebo.get_last_inserted_document_id().unwrap();
        assert_eq!(result, Some(5));
    }

    #[test]
    fn test_get_last_inserted_document_id_multiple_pages_with_deletions() {
        let test_dir = prepare_test_dir();
        let mut zebo: Zebo<3, 2048, u32> = Zebo::<3, 2048, _>::try_new(test_dir).unwrap();

        zebo.reserve_space_for(&[(1, "a"), (2, "b"), (3, "c")])
            .unwrap()
            .write_all()
            .unwrap();
        zebo.reserve_space_for(&[(4, "d"), (5, "e"), (6, "f")])
            .unwrap()
            .write_all()
            .unwrap();

        zebo.remove_documents(vec![5, 6], true).unwrap();

        let result = zebo.get_last_inserted_document_id().unwrap();
        assert_eq!(result, Some(6));
    }

    #[test]
    fn test_compact_basic() {
        let test_dir = prepare_test_dir();

        let mut zebo: Zebo<3, 2048, u32> = Zebo::<3, 2048, _>::try_new(test_dir.clone()).unwrap();

        // Fill page 0 (3 docs), then page 1 (3 docs), then page 2 (current)
        zebo.reserve_space_for(&[(1, "aaa"), (2, "bbb"), (3, "ccc")])
            .unwrap()
            .write_all()
            .unwrap();
        zebo.reserve_space_for(&[(4, "ddd"), (5, "eee"), (6, "fff")])
            .unwrap()
            .write_all()
            .unwrap();
        zebo.reserve_space_for(&[(7, "ggg")])
            .unwrap()
            .write_all()
            .unwrap();

        // Delete some docs from page 0 and page 1
        zebo.remove_documents(vec![1, 2, 5], true).unwrap();

        // Get file sizes before compaction
        let page0_size_before = std::fs::metadata(test_dir.join("page_0.zebo"))
            .unwrap()
            .len();
        let page2_size_before = std::fs::metadata(test_dir.join("page_2.zebo"))
            .unwrap()
            .len();

        let stats = zebo.compact().unwrap();

        assert_eq!(stats.pages_compacted, 2);
        // current page (page 2) should be skipped
        assert_eq!(stats.pages_skipped, 1);
        assert!(stats.total_bytes_reclaimed > 0);

        // Page 0 should be smaller (2 docs deleted out of 3)
        let page0_size_after = std::fs::metadata(test_dir.join("page_0.zebo"))
            .unwrap()
            .len();
        assert!(page0_size_after < page0_size_before);

        // Current page (page 2) should be unchanged
        let page2_size_after = std::fs::metadata(test_dir.join("page_2.zebo"))
            .unwrap()
            .len();
        assert_eq!(page2_size_after, page2_size_before);

        // All live docs should still be retrievable
        assert!(zebo.get_document(1).unwrap().is_none());
        assert!(zebo.get_document(2).unwrap().is_none());
        assert_eq!(zebo.get_document(3).unwrap().unwrap(), b"ccc");
        assert_eq!(zebo.get_document(4).unwrap().unwrap(), b"ddd");
        assert!(zebo.get_document(5).unwrap().is_none());
        assert_eq!(zebo.get_document(6).unwrap().unwrap(), b"fff");
        assert_eq!(zebo.get_document(7).unwrap().unwrap(), b"ggg");
    }

    #[test]
    fn test_compact_idempotent() {
        let test_dir = prepare_test_dir();

        let mut zebo: Zebo<3, 2048, u32> = Zebo::<3, 2048, _>::try_new(test_dir.clone()).unwrap();

        zebo.reserve_space_for(&[(1, "aaa"), (2, "bbb"), (3, "ccc")])
            .unwrap()
            .write_all()
            .unwrap();
        zebo.reserve_space_for(&[(4, "ddd")])
            .unwrap()
            .write_all()
            .unwrap();

        zebo.remove_documents(vec![2], true).unwrap();

        let stats1 = zebo.compact().unwrap();
        assert_eq!(stats1.pages_compacted, 1);

        // Second compact should find nothing to do
        let stats2 = zebo.compact().unwrap();
        assert_eq!(stats2.pages_compacted, 0);
        assert_eq!(stats2.total_bytes_reclaimed, 0);

        // Docs still accessible
        assert_eq!(zebo.get_document(1).unwrap().unwrap(), b"aaa");
        assert_eq!(zebo.get_document(3).unwrap().unwrap(), b"ccc");
    }

    #[test]
    fn test_compact_all_deleted_page() {
        let test_dir = prepare_test_dir();

        let mut zebo: Zebo<3, 2048, u32> = Zebo::<3, 2048, _>::try_new(test_dir.clone()).unwrap();

        zebo.reserve_space_for(&[(1, "aaa"), (2, "bbb"), (3, "ccc")])
            .unwrap()
            .write_all()
            .unwrap();
        zebo.reserve_space_for(&[(4, "ddd")])
            .unwrap()
            .write_all()
            .unwrap();

        // Delete all docs from page 0
        zebo.remove_documents(vec![1, 2, 3], true).unwrap();

        let page0_size_before = std::fs::metadata(test_dir.join("page_0.zebo"))
            .unwrap()
            .len();

        let stats = zebo.compact().unwrap();
        assert_eq!(stats.pages_compacted, 1);

        // Page should now be header-only (no data region)
        let page0_size_after = std::fs::metadata(test_dir.join("page_0.zebo"))
            .unwrap()
            .len();
        assert!(page0_size_after < page0_size_before);

        // Page 1 (current) doc should still work
        assert_eq!(zebo.get_document(4).unwrap().unwrap(), b"ddd");
    }

    #[test]
    fn test_compact_then_insert() {
        let test_dir = prepare_test_dir();

        let mut zebo: Zebo<3, 2048, u32> = Zebo::<3, 2048, _>::try_new(test_dir.clone()).unwrap();

        zebo.reserve_space_for(&[(1, "aaa"), (2, "bbb"), (3, "ccc")])
            .unwrap()
            .write_all()
            .unwrap();
        zebo.reserve_space_for(&[(4, "ddd")])
            .unwrap()
            .write_all()
            .unwrap();

        zebo.remove_documents(vec![2], true).unwrap();
        zebo.compact().unwrap();

        // Insert more docs after compaction
        zebo.reserve_space_for(&[(5, "eee"), (6, "fff")])
            .unwrap()
            .write_all()
            .unwrap();

        assert_eq!(zebo.get_document(1).unwrap().unwrap(), b"aaa");
        assert!(zebo.get_document(2).unwrap().is_none());
        assert_eq!(zebo.get_document(3).unwrap().unwrap(), b"ccc");
        assert_eq!(zebo.get_document(4).unwrap().unwrap(), b"ddd");
        assert_eq!(zebo.get_document(5).unwrap().unwrap(), b"eee");
        assert_eq!(zebo.get_document(6).unwrap().unwrap(), b"fff");
    }

    #[test]
    fn test_compact_reload() {
        let test_dir = prepare_test_dir();

        let mut zebo: Zebo<3, 2048, u32> = Zebo::<3, 2048, _>::try_new(test_dir.clone()).unwrap();

        zebo.reserve_space_for(&[(1, "aaa"), (2, "bbb"), (3, "ccc")])
            .unwrap()
            .write_all()
            .unwrap();
        zebo.reserve_space_for(&[(4, "ddd"), (5, "eee"), (6, "fff")])
            .unwrap()
            .write_all()
            .unwrap();
        zebo.reserve_space_for(&[(7, "ggg")])
            .unwrap()
            .write_all()
            .unwrap();

        zebo.remove_documents(vec![1, 2, 5], true).unwrap();
        zebo.compact().unwrap();

        // Drop and reload from disk
        drop(zebo);
        let mut zebo: Zebo<3, 2048, u32> = Zebo::<3, 2048, _>::try_new(test_dir.clone()).unwrap();

        // All live docs should be retrievable after reload
        assert!(zebo.get_document(1).unwrap().is_none());
        assert!(zebo.get_document(2).unwrap().is_none());
        assert_eq!(zebo.get_document(3).unwrap().unwrap(), b"ccc");
        assert_eq!(zebo.get_document(4).unwrap().unwrap(), b"ddd");
        assert!(zebo.get_document(5).unwrap().is_none());
        assert_eq!(zebo.get_document(6).unwrap().unwrap(), b"fff");
        assert_eq!(zebo.get_document(7).unwrap().unwrap(), b"ggg");

        // Info should reflect correct counts
        let info = zebo.get_info().unwrap();
        assert_eq!(info.document_count, 4);

        // New inserts after reload should work
        zebo.reserve_space_for(&[(8, "hhh"), (9, "iii")])
            .unwrap()
            .write_all()
            .unwrap();

        assert_eq!(zebo.get_document(8).unwrap().unwrap(), b"hhh");
        assert_eq!(zebo.get_document(9).unwrap().unwrap(), b"iii");

        // Second compact after reload should be idempotent
        let stats = zebo.compact().unwrap();
        assert_eq!(stats.pages_compacted, 0);
    }

    fn page_file_count(dir: &Path) -> usize {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
            .filter(|name| name.starts_with("page_") && name.ends_with(".zebo"))
            .count()
    }

    fn read_prefix(path: &Path, len: u64) -> Vec<u8> {
        let file = std::fs::File::open(path).unwrap();
        let mut buf = vec![0; len as usize];
        positioned_io::ReadAt::read_exact_at(&file, 0, &mut buf).unwrap();
        buf
    }

    fn seed_next_available_offset(path: &Path, offset: u32) {
        let mut file = std::fs::File::options().write(true).open(path).unwrap();
        positioned_io::WriteAt::write_all_at(
            &mut file,
            page::NEXT_AVAILABLE_OFFSET,
            &offset.to_be_bytes(),
        )
        .unwrap();
    }

    #[test]
    fn page_rotates_on_byte_size() {
        let test_dir = prepare_test_dir();
        let doc = vec![b'x'; 1024];

        let mut zebo = Zebo::<1000, 4096, u64>::try_new(&test_dir).unwrap();
        for id in 1..=5_u64 {
            zebo.reserve_space_for(&[(id, doc.clone())])
                .unwrap()
                .write_all()
                .unwrap();
        }
        // Four 1 KiB payloads fill PAGE_SIZE; the fifth goes to a new page.
        assert_eq!(page_file_count(&test_dir), 2);
        for id in 1..=5_u64 {
            assert_eq!(zebo.get_document(id).unwrap().unwrap(), doc);
        }
    }

    #[test]
    fn oversized_first_batch_fits_fresh_page() {
        let test_dir = prepare_test_dir();
        let big = vec![b'b'; 8 * 1024];

        let mut zebo = Zebo::<1000, 1024, u64>::try_new(&test_dir).unwrap();
        zebo.reserve_space_for(&[(1_u64, big.clone())])
            .unwrap()
            .write_all()
            .unwrap();
        assert_eq!(page_file_count(&test_dir), 1);
        assert_eq!(zebo.get_document(1).unwrap().unwrap(), big);

        // The next batch no longer fits that page.
        zebo.reserve_space_for(&[(2_u64, "small")])
            .unwrap()
            .write_all()
            .unwrap();
        assert_eq!(page_file_count(&test_dir), 2);
        assert_eq!(zebo.get_document(1).unwrap().unwrap(), big);
        assert_eq!(zebo.get_document(2).unwrap().unwrap(), b"small");
    }

    // Wrapping only happens without overflow checks: run under `cargo test --release` too.
    #[test]
    fn offset_never_wraps() {
        let test_dir = prepare_test_dir();
        let page_path = test_dir.join("page_0.zebo");
        let header_size = ZeboPage::header_size(10);

        // PAGE_SIZE never triggers here: only the u32 offset limit can rotate the page.
        let mut zebo = Zebo::<10, { u64::MAX }, u64>::try_new(&test_dir).unwrap();
        zebo.reserve_space_for(&[(1_u64, "first")])
            .unwrap()
            .write_all()
            .unwrap();

        // Sparse file: the page looks like it holds ~4 GiB without writing them.
        let seeded = u32::MAX - 100;
        std::fs::File::options()
            .write(true)
            .open(&page_path)
            .unwrap()
            .set_len(seeded as u64)
            .unwrap();
        seed_next_available_offset(&page_path, seeded);
        let header_before = read_prefix(&page_path, header_size);

        let doc = vec![7_u8; 200];
        zebo.reserve_space_for(&[(2_u64, doc.clone())])
            .unwrap()
            .write_all()
            .unwrap();

        assert_eq!(page_file_count(&test_dir), 2);
        assert_eq!(read_prefix(&page_path, header_size), header_before);
        assert_eq!(std::fs::metadata(&page_path).unwrap().len(), seeded as u64);
        assert_eq!(zebo.get_document(1).unwrap().unwrap(), b"first");
        assert_eq!(zebo.get_document(2).unwrap().unwrap(), doc);

        std::fs::remove_dir_all(&test_dir).unwrap();
    }

    #[test]
    fn batch_too_large_for_any_page_is_refused() {
        let test_dir = prepare_test_dir();
        let mut zebo = Zebo::<1_000_000, 1024, u64>::try_new(&test_dir).unwrap();

        // header (16,000,025 bytes) + this batch overflows a u32 offset even on a fresh page.
        struct Huge;
        impl Document for Huge {
            fn as_bytes(&self, _v: &mut Vec<u8>) {
                unreachable!("never written")
            }
            fn len(&self) -> usize {
                u32::MAX as usize
            }
        }

        let result = zebo.reserve_space_for(&[(1_u64, Huge)]);
        assert!(matches!(
            result,
            Err(ZeboError::NotEnoughSpace {
                limit: u32::MAX,
                ..
            })
        ));
        assert_eq!(page_file_count(&test_dir), 0);
    }

    #[test]
    fn page_with_inconsistent_offset_is_never_written() {
        let test_dir = prepare_test_dir();
        let page_path = test_dir.join("page_0.zebo");
        let header_size = ZeboPage::header_size(10);

        let mut zebo = Zebo::<10, { 1024 * 1024 }, u64>::try_new(&test_dir).unwrap();
        zebo.reserve_space_for(&[(1_u64, vec![b'a'; 1000])])
            .unwrap()
            .write_all()
            .unwrap();
        zebo.reserve_space_for(&[(2_u64, vec![b'b'; 1000])])
            .unwrap()
            .write_all()
            .unwrap();
        drop(zebo);

        // A wrapped offset: below the file length, pointing into existing payloads.
        seed_next_available_offset(&page_path, header_size as u32 + 10);
        let page_before = std::fs::read(&page_path).unwrap();

        // Reload, as a restarted process would, then write.
        let mut zebo = Zebo::<10, { 1024 * 1024 }, u64>::try_new(&test_dir).unwrap();
        zebo.reserve_space_for(&[(3_u64, "new")])
            .unwrap()
            .write_all()
            .unwrap();

        assert_eq!(page_file_count(&test_dir), 2);
        assert_eq!(std::fs::read(&page_path).unwrap(), page_before);
        assert_eq!(zebo.get_document(1).unwrap().unwrap(), vec![b'a'; 1000]);
        assert_eq!(zebo.get_document(2).unwrap().unwrap(), vec![b'b'; 1000]);
        assert_eq!(zebo.get_document(3).unwrap().unwrap(), b"new");
    }

    #[test]
    fn compact_skips_page_with_corrupt_entry() {
        let test_dir = prepare_test_dir();
        let page_path = test_dir.join("page_0.zebo");

        let mut zebo = Zebo::<2, { 1024 * 1024 }, u64>::try_new(&test_dir).unwrap();
        for id in 1..=3_u64 {
            zebo.reserve_space_for(&[(id, "doc")])
                .unwrap()
                .write_all()
                .unwrap();
        }
        // Delete one document so page 0 is a compaction candidate.
        zebo.remove_documents(vec![1], false).unwrap();

        // Point doc 2's entry past the end of the file.
        let mut file = std::fs::File::options()
            .write(true)
            .open(&page_path)
            .unwrap();
        let mut entry = [0_u8; 16];
        entry[0..8].copy_from_slice(&2_u64.to_be_bytes());
        entry[8..12].copy_from_slice(&(ZeboPage::header_size(2) as u32).to_be_bytes());
        entry[12..16].copy_from_slice(&(u32::MAX - 1).to_be_bytes());
        positioned_io::WriteAt::write_all_at(&mut file, DOCUMENT_INDEX_OFFSET + 16, &entry)
            .unwrap();
        drop(file);
        let page_before = std::fs::read(&page_path).unwrap();

        let stats = zebo.compact().unwrap();
        assert_eq!(stats.pages_compacted, 0);
        assert_eq!(std::fs::read(&page_path).unwrap(), page_before);
        assert!(!test_dir.join("page_0.zebo.compact_tmp").exists());
        assert_eq!(zebo.get_document(2).unwrap(), None);
        assert_eq!(zebo.get_document(3).unwrap().unwrap(), b"doc");
    }

    struct MyDoc {
        id: String,
        text: String,
    }
    impl Document for MyDoc {
        fn as_bytes(&self, v: &mut Vec<u8>) {
            static ZERO: &[u8] = b"\0";
            v.extend(self.id.as_bytes());
            v.extend(ZERO);
            v.extend(self.text.as_bytes());
        }

        fn len(&self) -> usize {
            self.id.len() + 1 + self.text.len()
        }
    }

    #[test]
    fn rotates_when_page_limit_is_below_const() {
        let test_dir = prepare_test_dir();

        let mut zebo = Zebo::<4, { 1024 * 1024 }, u64>::try_new(&test_dir).unwrap();
        for id in 1..=4_u64 {
            zebo.reserve_space_for(&[(id, format!("doc {id}"))])
                .unwrap()
                .write_all()
                .unwrap();
        }
        drop(zebo);

        // Reopened with a larger limit: the full 4-slot page must rotate, not fail.
        let mut zebo = Zebo::<10, { 1024 * 1024 }, u64>::try_new(&test_dir).unwrap();
        zebo.reserve_space_for(&[(5_u64, "doc 5")])
            .unwrap()
            .write_all()
            .unwrap();

        assert_eq!(page_file_count(&test_dir), 2);
        for id in 1..=5_u64 {
            assert_eq!(
                zebo.get_document(id).unwrap().unwrap(),
                format!("doc {id}").into_bytes()
            );
        }
    }

    #[test]
    fn crash_between_reserve_and_write_recovers() {
        let test_dir = prepare_test_dir();

        let mut zebo = Zebo::<10, { 1024 * 1024 }, u64>::try_new(&test_dir).unwrap();
        for id in 1..=3_u64 {
            zebo.reserve_space_for(&[(id, format!("doc {id}"))])
                .unwrap()
                .write_all()
                .unwrap();
        }
        // Reserved but never written: slot 3 stays uninitialized and the next-write
        // offset is ahead of the file length.
        let reserved = zebo.reserve_space_for(&[(4_u64, "doc 4")]).unwrap();
        drop(reserved);
        drop(zebo);

        let mut zebo = Zebo::<10, { 1024 * 1024 }, u64>::try_new(&test_dir).unwrap();
        zebo.reserve_space_for(&[(5_u64, "doc 5")])
            .unwrap()
            .write_all()
            .unwrap();

        for id in [1, 2, 3, 5_u64] {
            assert_eq!(
                zebo.get_document(id).unwrap().unwrap(),
                format!("doc {id}").into_bytes()
            );
        }
        assert_eq!(zebo.get_document(4).unwrap(), None);
        zebo.get_last_inserted_document_id().unwrap();
    }

    #[test]
    fn no_rotation_churn_after_wrapped_page() {
        let test_dir = prepare_test_dir();
        let page_path = test_dir.join("page_0.zebo");

        let mut zebo = Zebo::<100, { 1024 * 1024 }, u64>::try_new(&test_dir).unwrap();
        for id in 1..=2_u64 {
            zebo.reserve_space_for(&[(id, format!("doc {id}"))])
                .unwrap()
                .write_all()
                .unwrap();
        }
        drop(zebo);

        // A wrapped offset: below the file length, pointing into existing payloads.
        seed_next_available_offset(&page_path, ZeboPage::header_size(100) as u32 + 2);

        let mut zebo = Zebo::<100, { 1024 * 1024 }, u64>::try_new(&test_dir).unwrap();
        for id in 3..=22_u64 {
            if id == 12 {
                // Reload, as a restarted process would: the new page is the current one.
                drop(zebo);
                zebo = Zebo::<100, { 1024 * 1024 }, u64>::try_new(&test_dir).unwrap();
            }
            zebo.reserve_space_for(&[(id, format!("doc {id}"))])
                .unwrap()
                .write_all()
                .unwrap();
            assert_eq!(page_file_count(&test_dir), 2, "after inserting doc {id}");
        }

        for id in 1..=22_u64 {
            assert_eq!(
                zebo.get_document(id).unwrap().unwrap(),
                format!("doc {id}").into_bytes()
            );
        }
    }

    /// Every file under `dir`, with its contents, sorted by path.
    fn dir_snapshot(dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
        let mut files = Vec::new();
        let mut dirs = vec![dir.to_path_buf()];
        while let Some(dir) = dirs.pop() {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    dirs.push(path);
                } else {
                    let bytes = std::fs::read(&path).unwrap();
                    files.push((path, bytes));
                }
            }
        }
        files.sort();
        files
    }

    /// Counts the warnings `f` logs on the current thread.
    fn count_warnings(f: impl FnOnce()) -> usize {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use tracing_subscriber::layer::SubscriberExt;

        struct WarnCounter(Arc<AtomicUsize>);
        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WarnCounter {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _: tracing_subscriber::layer::Context<'_, S>,
            ) {
                if *event.metadata().level() == tracing::Level::WARN {
                    self.0.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        let count = Arc::new(AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry().with(WarnCounter(count.clone()));
        tracing::subscriber::with_default(subscriber, f);
        count.load(Ordering::Relaxed)
    }

    fn insert<const N: u32, const S: u64>(zebo: &mut Zebo<N, S, u64>, docs: &[(u64, String)]) {
        zebo.reserve_space_for(docs).unwrap().write_all().unwrap();
    }

    fn docs(ids: impl IntoIterator<Item = u64>, version: &str) -> Vec<(u64, String)> {
        ids.into_iter()
            .map(|id| (id, format!("{version} {id}")))
            .collect()
    }

    fn assert_page_starts_ascending<const N: u32, const S: u64>(zebo: &Zebo<N, S, u64>) {
        let pages = zebo.get_all_pages().unwrap();
        assert!(
            pages.windows(2).all(|w| w[0].0 < w[1].0),
            "page starts not strictly ascending: {pages:?}"
        );
    }

    #[test]
    fn unsorted_batch_is_refused() {
        let test_dir = prepare_test_dir();
        let mut zebo = Zebo::<100, { 1024 * 1024 }, u64>::try_new(&test_dir).unwrap();

        // On an empty store no page is created.
        let empty = dir_snapshot(&test_dir);
        let err = zebo
            .reserve_space_for(&[(7, "a"), (5, "b"), (6, "c")])
            .unwrap_err();
        assert!(matches!(err, ZeboError::UnsortedBatch { position: 1 }));
        assert_eq!(page_file_count(&test_dir), 0);
        assert_eq!(dir_snapshot(&test_dir), empty);

        insert(&mut zebo, &docs(1..=3, "v1"));
        let before = dir_snapshot(&test_dir);
        let err = zebo
            .reserve_space_for(&[(7, "a"), (5, "b"), (6, "c")])
            .unwrap_err();
        assert!(matches!(err, ZeboError::UnsortedBatch { position: 1 }));
        assert_eq!(dir_snapshot(&test_dir), before);
    }

    #[test]
    fn duplicate_ids_in_batch_are_refused() {
        let test_dir = prepare_test_dir();
        let mut zebo = Zebo::<100, { 1024 * 1024 }, u64>::try_new(&test_dir).unwrap();
        insert(&mut zebo, &docs(1..=3, "v1"));
        let before = dir_snapshot(&test_dir);

        let err = zebo
            .reserve_space_for(&[(4, "a"), (5, "b"), (5, "c")])
            .unwrap_err();
        assert!(matches!(err, ZeboError::UnsortedBatch { position: 2 }));
        assert_eq!(dir_snapshot(&test_dir), before);
    }

    #[test]
    fn batch_below_current_page_is_inserted_with_warning() {
        let test_dir = prepare_test_dir();
        let mut zebo = Zebo::<10, { 1024 * 1024 }, u64>::try_new(&test_dir).unwrap();
        insert(&mut zebo, &docs(1..=9, "v1"));
        // Out of slots: the second page starts at 10.
        insert(&mut zebo, &docs(10..=12, "v1"));
        assert_eq!(page_file_count(&test_dir), 2);

        // 9 is below the current page start: inserted there, with one warning.
        let warnings = count_warnings(|| insert(&mut zebo, &docs([9, 13], "v2")));
        assert_eq!(warnings, 1);
        assert_eq!(page_file_count(&test_dir), 2);
        assert_page_starts_ascending(&zebo);

        // 9 routes to the first page and returns its earlier copy.
        assert_eq!(zebo.get_document(9).unwrap().unwrap(), b"v1 9");
        assert_eq!(zebo.get_document(13).unwrap().unwrap(), b"v2 13");
        for id in (1..=8).chain(10..=12) {
            assert_eq!(
                zebo.get_document(id).unwrap().unwrap(),
                format!("v1 {id}").into_bytes()
            );
        }
        // The copy of 9 on the second page is shadowed by the one it routes to.
        let all_ids: std::collections::BTreeSet<u64> = zebo
            .get_all_documents()
            .unwrap()
            .map(|r| r.unwrap().0)
            .collect();
        assert_eq!(all_ids, (1..=13).collect());
    }

    #[test]
    fn batch_below_closing_page_starts_new_page_above_it() {
        let test_dir = prepare_test_dir();
        let mut zebo = Zebo::<4, { 1024 * 1024 }, u64>::try_new(&test_dir).unwrap();
        insert(&mut zebo, &docs(1..=4, "v1"));
        insert(&mut zebo, &docs(5..=7, "v1"));

        // The second page has one slot left, so this batch rotates. Its first id is
        // below the closing page's highest id (7), so the new page starts at 8.
        let warnings = count_warnings(|| insert(&mut zebo, &docs([3, 6, 8, 9], "v2")));
        assert_eq!(warnings, 1);
        drop(zebo);

        let mut zebo = Zebo::<4, { 1024 * 1024 }, u64>::try_new(&test_dir).unwrap();
        assert_page_starts_ascending(&zebo);
        let starts: Vec<u64> = zebo
            .get_all_pages()
            .unwrap()
            .into_iter()
            .map(|(start, _)| start)
            .collect();
        assert_eq!(starts, [1, 5, 8]);

        // Ids above the new start read from the new page; the others return the copy
        // on the page they route to.
        for id in 1..=7 {
            assert_eq!(
                zebo.get_document(id).unwrap().unwrap(),
                format!("v1 {id}").into_bytes()
            );
        }
        for id in 8..=9 {
            assert_eq!(
                zebo.get_document(id).unwrap().unwrap(),
                format!("v2 {id}").into_bytes()
            );
        }

        // Later batches still route correctly, with no warning.
        let warnings = count_warnings(|| {
            insert(&mut zebo, &docs(10..=10, "v3"));
            insert(&mut zebo, &docs(11..=13, "v3"));
        });
        assert_eq!(warnings, 0);
        assert_page_starts_ascending(&zebo);
        for id in 10..=13 {
            assert_eq!(
                zebo.get_document(id).unwrap().unwrap(),
                format!("v3 {id}").into_bytes()
            );
        }
    }

    #[test]
    fn sorted_batches_route_across_pages() {
        let test_dir = prepare_test_dir();
        let mut zebo = Zebo::<1000, 4096, u64>::try_new(&test_dir).unwrap();
        let doc = |id: u64| format!("{id:>1024}");

        let warnings = count_warnings(|| {
            for batch in (1..=30_u64).collect::<Vec<_>>().chunks(3) {
                let batch: Vec<_> = batch.iter().map(|id| (*id, doc(*id))).collect();
                insert(&mut zebo, &batch);
            }
        });
        assert_eq!(warnings, 0);
        assert!(page_file_count(&test_dir) > 5);
        assert_page_starts_ascending(&zebo);

        let found: Vec<_> = zebo
            .get_documents(1..=30_u64)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(found.len(), 30);
        for (id, bytes) in found {
            assert_eq!(bytes, doc(id).into_bytes());
        }
    }

    #[test]
    fn replay_rotation_uses_maximum_instead_of_last_reserved_id() {
        let test_dir = prepare_test_dir();
        let mut zebo = Zebo::<4, { 1024 * 1024 }, u64>::try_new(&test_dir).unwrap();
        insert(&mut zebo, &docs(10..=12, "v1"));
        insert(&mut zebo, &docs([10], "replay"));
        // The most recent id is 10, but the closing page's maximum is still 12.
        insert(&mut zebo, &docs([10, 13], "v2"));
        assert_eq!(
            zebo.get_all_pages()
                .unwrap()
                .iter()
                .map(|p| p.0)
                .collect::<Vec<_>>(),
            [10, 13]
        );
        assert_eq!(zebo.get_document(12).unwrap().unwrap(), b"v1 12");
        assert_eq!(zebo.get_document(13).unwrap().unwrap(), b"v2 13");
    }

    #[test]
    fn replay_rotation_accounts_for_reservations_not_yet_written() {
        let test_dir = prepare_test_dir();
        let mut zebo = Zebo::<2, { 1024 * 1024 }, u64>::try_new(&test_dir).unwrap();
        let original = docs([10, 20], "v1");
        let pending = zebo.reserve_space_for(&original).unwrap();
        let replay = docs([10, 21], "v2");
        let next = zebo.reserve_space_for(&replay).unwrap();
        // Reservation handles can write after the Zebo lock is released and the
        // page has rotated; routing must account for the pending ids already.
        next.write_all().unwrap();
        pending.write_all().unwrap();
        assert_page_starts_ascending(&zebo);
        assert_eq!(zebo.get_all_pages().unwrap()[1].0, 21);
        assert_eq!(zebo.get_document(20).unwrap().unwrap(), b"v1 20");
        assert_eq!(zebo.get_document(21).unwrap().unwrap(), b"v2 21");
    }

    #[test]
    fn replay_only_page_keeps_ascending_starts_after_reopen() {
        let test_dir = prepare_test_dir();
        let mut zebo = Zebo::<1, { 1024 * 1024 }, u64>::try_new(&test_dir).unwrap();
        insert(&mut zebo, &docs([10], "v1"));
        insert(&mut zebo, &docs([10], "replay"));
        drop(zebo);

        let mut zebo = Zebo::<1, { 1024 * 1024 }, u64>::try_new(&test_dir).unwrap();
        insert(&mut zebo, &docs([10], "replay"));
        insert(&mut zebo, &docs([13], "v2"));
        assert_eq!(
            zebo.get_all_pages()
                .unwrap()
                .iter()
                .map(|p| p.0)
                .collect::<Vec<_>>(),
            [10, 11, 12, 13]
        );
        assert_eq!(zebo.get_document(10).unwrap().unwrap(), b"v1 10");
        assert_eq!(zebo.get_document(13).unwrap().unwrap(), b"v2 13");
        // Replayed ids below a page's start also remain safe to enumerate.
        assert!(zebo.get_all_documents().unwrap().all(|doc| doc.is_ok()));
    }

    fn assert_page_start_overflow<Id: DocumentId>(penultimate_id: Id) {
        let test_dir = prepare_test_dir();
        let mut zebo = Zebo::<1, { 1024 * 1024 }, Id>::try_new(&test_dir).unwrap();
        let batch = [(penultimate_id, "original")];
        zebo.reserve_space_for(&batch).unwrap().write_all().unwrap();
        // A replay-only page starts at the largest representable id, while its
        // stored document remains below that start.
        zebo.reserve_space_for(&batch).unwrap().write_all().unwrap();
        let before = dir_snapshot(&test_dir);
        for reopen in [false, true] {
            if reopen {
                drop(zebo);
                zebo = Zebo::try_new(&test_dir).unwrap();
            }
            let error = zebo.reserve_space_for(&batch).unwrap_err();
            assert!(matches!(error, ZeboError::PageStartOverflow { last_id }
                if last_id == penultimate_id.as_u64() + 1));
            assert_eq!(dir_snapshot(&test_dir), before);
            assert_eq!(
                zebo.get_document(penultimate_id).unwrap().unwrap(),
                b"original"
            );
        }
    }

    #[test]
    fn u32_page_start_overflow_leaves_files_unchanged() {
        assert_page_start_overflow(u32::MAX - 1);
    }

    #[test]
    fn u64_page_start_overflow_leaves_files_unchanged() {
        assert_page_start_overflow(u64::MAX - 1);
    }
}
