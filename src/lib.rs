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
/// - Zebo also tries to limit the page size up to `PAGE_SIZE` bytes.
///
/// In case a document is larger than `PAGE_SIZE`, a new page is created.
/// In that case, the page size will higher than `PAGE_SIZE` bytes.
pub struct Zebo<const MAX_DOC_PER_PAGE: u32, const PAGE_SIZE: u64, DocId> {
    base_dir: PathBuf,
    next_page_id: u64,
    index: ZeboIndex<DocId>,
    current_page: Option<(DocId, ZeboPage)>,
    remove_documents_cache: HashMap<PageId, Vec<(u64, ProbableIndex)>>,
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
                        let starting_document_id = DocId::from_u64(page.starting_document_id);

                        (index, Some(page_id), Some((starting_document_id, page)))
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

    /// Add documents to Zebo
    pub fn add_documents<W: Document, I: IntoIterator<Item = (DocId, W)>>(
        &mut self,
        docs: I,
    ) -> Result<()> {
        let (mut doc_count_remain, mut page_size_remain) = self.get_initial_remains()?;

        let mut v = Vec::new();
        for (doc_id, doc) in docs {
            doc.as_bytes(&mut v);
            let doc_len = v.len() as u32;

            if doc_count_remain == 0 || page_size_remain < doc_len as u64 {
                self.current_page = None;
            }

            let (_, page) = match self.current_page.as_mut() {
                Some(page) => page,
                None => {
                    self.build_next_page(doc_id)?;
                    doc_count_remain = MAX_DOC_PER_PAGE;
                    page_size_remain = PAGE_SIZE;
                    self.current_page.as_mut().expect("Page should be created")
                }
            };

            // For each document, reserve and write
            page.reserve_space(doc_id.as_u64(), doc_len)?.write(&v)?;
            v.clear();

            doc_count_remain -= 1;
            page_size_remain = page_size_remain.saturating_sub(doc_len as u64)
        }

        Ok(())
    }

    /// Add documents to Zebo in batch
    pub fn add_documents_batch<W: Document, I: IntoIterator<Item = (DocId, W)>>(
        &mut self,
        docs: I,
        batch_size: usize,
        expected_size_per_document: usize,
    ) -> Result<()> {
        let (mut doc_count_remain, mut page_size_remain) = self.get_initial_remains()?;

        let mut batch = Vec::with_capacity(batch_size);
        let mut batch_doc: Vec<u8> = Vec::with_capacity(batch_size * expected_size_per_document);
        let mut bytes: u64 = 0;
        let mut v = Vec::new();
        for (doc_id, doc) in docs {
            doc.as_bytes(&mut v);
            let doc_len = v.len() as u32;

            if doc_count_remain == 0 || page_size_remain < bytes {
                self.current_page = None;
            }
            if self.current_page.is_none() {
                self.build_next_page(doc_id)?;
                doc_count_remain = MAX_DOC_PER_PAGE;
                page_size_remain = PAGE_SIZE;
            }

            batch.push((doc_id.as_u64(), doc_len));
            batch_doc.extend(&v);
            v.clear();

            doc_count_remain -= 1;
            bytes = page_size_remain.saturating_sub(doc_len as u64);

            // When capacity is reached, we eventully create a page and insert the documents there
            if batch.len() == batch_size {
                let (_, page) = match self.current_page.as_mut() {
                    Some(page) => page,
                    None => {
                        let doc_id = batch.first().expect("").0;
                        self.build_next_page(DocId::from_u64(doc_id))?;
                        doc_count_remain = MAX_DOC_PER_PAGE;
                        page_size_remain = PAGE_SIZE;
                        self.current_page.as_mut().expect("Page should be created")
                    }
                };

                let reserved_spaces = page.reserve_multiple_space(batch.len(), batch.iter())?;
                reserved_spaces.write(batch_doc.clone())?;
                page.close()?;
                batch.clear();
                batch_doc.clear();
            }
        }

        if !batch.is_empty() {
            let (_, page) = match self.current_page.as_mut() {
                Some(page) => page,
                None => {
                    let doc_id = batch.first().expect("").0;
                    self.build_next_page(DocId::from_u64(doc_id))?;
                    self.current_page.as_mut().expect("Page should be created")
                }
            };

            let reserved_spaces = page.reserve_multiple_space(batch.len(), batch.iter())?;
            reserved_spaces.write(batch_doc)?;
            page.close()?;
        }

        Ok(())
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
                let probable_index = ProbableIndex(doc_id - page.starting_document_id);
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

    fn get_initial_remains(&mut self) -> Result<(u32, u64)> {
        let mut doc_count_remain = 0;
        let mut page_size_remain = 0;

        if let Some((_, page)) = self.current_page.as_mut() {
            let document_count = page.next_available_header_offset;
            let page_size = page.current_file_size()?;

            // If the current page is full, we need to create a new one
            // Otherwise, we can use the current page till it is full
            if page_size > PAGE_SIZE {
                self.current_page = None;
            } else {
                page_size_remain = PAGE_SIZE - page_size;
            }

            // If the current page reaches the document limit, we need to create a new one
            // Otherwise, we can use the current page till it reaches the document limit
            if document_count >= MAX_DOC_PER_PAGE {
                self.current_page = None;
            } else {
                doc_count_remain = MAX_DOC_PER_PAGE - document_count;
            }
        }

        Ok((doc_count_remain, page_size_remain))
    }

    fn build_next_page(&mut self, doc_id: DocId) -> Result<()> {
        if let Some((_, mut page)) = self.current_page.take() {
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
        self.current_page = Some((doc_id, page));
        self.next_page_id += 1;

        Ok(())
    }

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
}

impl<T: AsRef<[u8]>> Document for T {
    fn as_bytes(&self, v: &mut Vec<u8>) {
        v.extend(self.as_ref());
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

    mod single {
        use super::*;

        #[test]
        fn test_zebo_simple() {
            let test_dir = prepare_test_dir();

            let mut zebo: Zebo<2, 2048, u32> =
                Zebo::<2, 2048, _>::try_new(test_dir.clone()).unwrap();
            assert_eq!(zebo.next_page_id, 0);

            let docs = vec![(1, "Hello")];
            zebo.add_documents(docs).unwrap();

            let doc = zebo.get_document(1).unwrap().unwrap();
            assert_eq!(doc, b"Hello");

            let docs: Result<Vec<_>> = zebo.get_documents(vec![1]).unwrap().collect();
            let docs = docs.unwrap();
            assert_eq!(docs, vec![(1, b"Hello".to_vec())]);
        }

        #[test]
        fn test_zebo_pub() {
            let test_dir = prepare_test_dir();

            let mut zebo: Zebo<6, 2048, u32> =
                Zebo::<6, 2048, _>::try_new(test_dir.clone()).unwrap();
            assert_eq!(zebo.next_page_id, 0);

            let docs = vec![(1, "Hello"), (2, "World"), (3, "Zebo")];
            zebo.add_documents(docs).unwrap();

            let doc = zebo.get_document(1).unwrap().unwrap();
            assert_eq!(doc, b"Hello");
            let doc = zebo.get_document(2).unwrap().unwrap();
            assert_eq!(doc, b"World");
            let doc = zebo.get_document(3).unwrap().unwrap();
            assert_eq!(doc, b"Zebo");

            let docs = vec![(4, "Test"), (5, "Document"), (6, "Storage")];
            zebo.add_documents(docs).unwrap();

            let doc = zebo.get_document(4).unwrap().unwrap();
            assert_eq!(doc, b"Test");
            let doc = zebo.get_document(5).unwrap().unwrap();
            assert_eq!(doc, b"Document");
            let doc = zebo.get_document(6).unwrap().unwrap();
            assert_eq!(doc, b"Storage");

            let document_count = zebo.get_document_count().unwrap();
            assert_eq!(document_count, 6);

            let res = zebo.get_document(7).unwrap();
            assert!(res.is_none());
        }

        #[test]
        fn test_zebo_paging_on_doc_size() {
            let test_dir = prepare_test_dir();

            let mut zebo: Zebo<10, 128, u32> =
                Zebo::<10, 128, _>::try_new(test_dir.clone()).unwrap();
            assert_eq!(zebo.next_page_id, 0);

            let docs = vec![
                (1, "1".repeat(100)),
                (2, "2".repeat(100)),
                (3, "3".repeat(100)),
            ];
            zebo.add_documents(docs).unwrap();

            let doc = zebo.get_document(1).unwrap().unwrap();
            assert_eq!(doc, b"1".repeat(100).to_vec());

            let doc = zebo.get_document(2).unwrap().unwrap();
            assert_eq!(doc, b"2".repeat(100).to_vec());

            let doc = zebo.get_document(3).unwrap().unwrap();
            assert_eq!(doc, b"3".repeat(100).to_vec());

            let document_count = zebo.get_document_count().unwrap();
            assert_eq!(document_count, 3);

            let info = zebo.get_info().unwrap();
            assert_eq!(info.page_headers.len(), 3);
            assert_eq!(info.page_headers[0].document_count, 1);
            assert_eq!(info.page_headers[1].document_count, 1);
            assert_eq!(info.page_headers[2].document_count, 1);
        }

        #[test]
        fn doc_size_limit_is_overcome_if_only_one() {
            let test_dir = prepare_test_dir();

            let mut zebo: Zebo<10, 128, u32> =
                Zebo::<10, 128, _>::try_new(test_dir.clone()).unwrap();

            let docs = vec![
                (1, "1".repeat(200)),
                (2, "2".repeat(100)),
                (3, "3".repeat(200)),
            ];
            zebo.add_documents(docs).unwrap();

            let doc = zebo.get_document(1).unwrap().unwrap();
            assert_eq!(doc, b"1".repeat(200).to_vec());

            let doc = zebo.get_document(2).unwrap().unwrap();
            assert_eq!(doc, b"2".repeat(100).to_vec());

            let doc = zebo.get_document(3).unwrap().unwrap();
            assert_eq!(doc, b"3".repeat(200).to_vec());

            let document_count = zebo.get_document_count().unwrap();
            assert_eq!(document_count, 3);

            let info = zebo.get_info().unwrap();
            assert_eq!(info.page_headers.len(), 3);
            assert_eq!(info.page_headers[0].document_count, 1);
            assert_eq!(info.page_headers[1].document_count, 1);
            assert_eq!(info.page_headers[2].document_count, 1);

            let test_dir = prepare_test_dir();
            let mut zebo: Zebo<10, 128, u32> =
                Zebo::<10, 128, _>::try_new(test_dir.clone()).unwrap();
            zebo.add_documents(vec![
                (1, "1".repeat(40)),
                (2, "2".repeat(40)),
                (3, "3".repeat(200)),
            ])
            .unwrap();

            let info = zebo.get_info().unwrap();
            assert_eq!(info.page_headers.len(), 2);
            assert_eq!(info.page_headers[0].document_count, 2);
            assert_eq!(info.page_headers[1].document_count, 1);

            let test_dir = prepare_test_dir();
            let mut zebo: Zebo<10, 128, u32> =
                Zebo::<10, 128, _>::try_new(test_dir.clone()).unwrap();
            zebo.add_documents(vec![
                (1, "1".repeat(200)),
                (2, "2".repeat(40)),
                (3, "3".repeat(40)),
            ])
            .unwrap();

            let info = zebo.get_info().unwrap();
            assert_eq!(info.page_headers.len(), 2);
            assert_eq!(info.page_headers[0].document_count, 1);
            assert_eq!(info.page_headers[1].document_count, 2);
        }

        #[test]
        fn test_zebo_paging_on_doc_count() {
            let test_dir = prepare_test_dir();

            let mut zebo: Zebo<2, 2048, u32> =
                Zebo::<2, 2048, _>::try_new(test_dir.clone()).unwrap();
            assert_eq!(zebo.next_page_id, 0);

            let docs = vec![(1, ""), (2, ""), (3, "")];
            zebo.add_documents(docs).unwrap();

            let doc = zebo.get_document(1).unwrap().unwrap();
            assert_eq!(doc, b"");

            let doc = zebo.get_document(2).unwrap().unwrap();
            assert_eq!(doc, b"");

            let doc = zebo.get_document(3).unwrap().unwrap();
            assert_eq!(doc, b"");

            let document_count = zebo.get_document_count().unwrap();
            assert_eq!(document_count, 3);

            let info = zebo.get_info().unwrap();
            assert_eq!(info.page_headers.len(), 2);
            assert_eq!(info.page_headers[0].document_count, 2);
            assert_eq!(info.page_headers[1].document_count, 1);

            let docs = vec![(4, ""), (5, ""), (6, "")];
            zebo.add_documents(docs).unwrap();

            let document_count = zebo.get_document_count().unwrap();
            assert_eq!(document_count, 6);

            let info = zebo.get_info().unwrap();
            assert_eq!(info.page_headers.len(), 3);
        }

        #[test]
        fn test_zebo_empty_doc() {
            let test_dir = prepare_test_dir();

            let mut zebo: Zebo<100, 2048, u32> =
                Zebo::<100, 2048, _>::try_new(test_dir.clone()).unwrap();
            assert_eq!(zebo.next_page_id, 0);

            let docs = vec![(1, ""), (2, ""), (3, "")];
            zebo.add_documents(docs).unwrap();

            let doc = zebo.get_document(1).unwrap().unwrap();
            assert_eq!(doc, b"");

            let doc = zebo.get_document(2).unwrap().unwrap();
            assert_eq!(doc, b"");

            let doc = zebo.get_document(3).unwrap().unwrap();
            assert_eq!(doc, b"");

            let document_count = zebo.get_document_count().unwrap();
            assert_eq!(document_count, 3);

            let docs = vec![(4, ""), (5, ""), (6, "")];
            zebo.add_documents(docs).unwrap();

            let document_count = zebo.get_document_count().unwrap();
            assert_eq!(document_count, 6);
        }

        #[test]
        fn test_zebo_delete() {
            let test_dir = prepare_test_dir();

            let mut zebo: Zebo<5, 2048, u32> =
                Zebo::<5, 2048, _>::try_new(test_dir.clone()).unwrap();
            zebo.add_documents(vec![(1, "01234567890"), (2, "ABCFE"), (3, "FGHIL")])
                .unwrap();

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
        }

        #[test]
        fn test_zebo_get_documents() {
            let test_dir = prepare_test_dir();

            let mut zebo: Zebo<2, 2048, u32> =
                Zebo::<2, 2048, _>::try_new(test_dir.clone()).unwrap();
            assert_eq!(zebo.next_page_id, 0);

            zebo.add_documents(vec![(1, "1"), (2, "2"), (3, "3")])
                .unwrap();
            zebo.add_documents(vec![(4, "4"), (5, "5"), (6, "6")])
                .unwrap();

            let output = zebo.get_document(1).unwrap().unwrap();
            assert_eq!(output, b"1".to_vec());

            let info = zebo.get_info().unwrap();
            assert_eq!(info.page_headers.len(), 3);

            let mut docs = zebo
                .get_documents(vec![1, 3, 5, 6])
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();
            docs.sort_by_key(|d| d.0);
            assert_eq!(
                docs,
                vec![
                    (1, b"1".to_vec()),
                    (3, b"3".to_vec()),
                    (5, b"5".to_vec()),
                    (6, b"6".to_vec()),
                ]
            );
        }

        #[test]
        fn test_zebo_get_all_documents() {
            let test_dir = prepare_test_dir();

            let mut zebo: Zebo<2, 2048, u32> =
                Zebo::<2, 2048, _>::try_new(test_dir.clone()).unwrap();

            // Add documents across multiple pages
            zebo.add_documents(vec![(1, "first"), (2, "second"), (3, "third")])
                .unwrap();
            zebo.add_documents(vec![(4, "fourth"), (5, "fifth"), (6, "sixth")])
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
            let zebo_empty: Zebo<2, 2048, u32> =
                Zebo::<2, 2048, _>::try_new(test_dir_empty).unwrap();

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

            let mut zebo: Zebo<5, 2048, u32> =
                Zebo::<5, 2048, _>::try_new(test_dir.clone()).unwrap();

            // Add documents to a single page to avoid the page file issue
            zebo.add_documents(vec![
                (1, "first"),
                (2, "second"),
                (3, "third"),
                (4, "fourth"),
            ])
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
        fn test_zebo_re_create() {
            let test_dir = prepare_test_dir();

            let mut zebo: Zebo<2, 2048, u32> =
                Zebo::<2, 2048, _>::try_new(test_dir.clone()).unwrap();
            assert_eq!(zebo.next_page_id, 0);

            zebo.add_documents(vec![(1, "1"), (2, "2"), (3, "3")])
                .unwrap();
            zebo.add_documents(vec![(4, "4"), (5, "5"), (6, "6")])
                .unwrap();

            // Create a new instance that points to the same directory
            // In this case we expect to reload the pages correctly
            let zebo: Zebo<2, 2048, u32> = Zebo::<2, 2048, _>::try_new(test_dir.clone()).unwrap();

            let output = zebo.get_document(1).unwrap().unwrap();
            assert_eq!(output, b"1".to_vec());

            let info = zebo.get_info().unwrap();
            assert_eq!(info.page_headers.len(), 3);

            let mut docs = zebo
                .get_documents(vec![1, 3, 5, 6])
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();
            docs.sort_by_key(|d| d.0);
            assert_eq!(
                docs,
                vec![
                    (1, b"1".to_vec()),
                    (3, b"3".to_vec()),
                    (5, b"5".to_vec()),
                    (6, b"6".to_vec()),
                ]
            );
        }

        #[test]
        fn test_zebo_with_gap() -> Result<()> {
            let test_dir = prepare_test_dir();

            let mut zebo: Zebo<2, 2048, u32> =
                Zebo::<2, 2048, _>::try_new(test_dir.clone()).unwrap();
            assert_eq!(zebo.next_page_id, 0);

            zebo.add_documents(vec![(1, "1"), (3, "3"), (5, "5")])
                .unwrap();
            zebo.add_documents(vec![(7, "7"), (9, "9"), (11, "11")])
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
            assert_eq!(info.page_headers.len(), 3);

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

            zebo.add_documents(vec![(13, "13"), (15, "15")]).unwrap();
            assert_eq!(docs.len(), 0);

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
        fn test_real_example_1() {
            let test_dir = prepare_test_dir();

            // 1GB
            const PAGE_SIZE: u64 = 1024 * 1024 * 1024;

            let mut zebo: Zebo<1_000_000, PAGE_SIZE, u32> =
                Zebo::try_new(test_dir.clone()).unwrap();
            assert_eq!(zebo.next_page_id, 0);

            zebo.add_documents(vec![(
                1,
                MyDoc {
                    id: "1".to_string(),
                    text: r#"{"id"\:"1","text":"avvocata"}"#.to_string(),
                },
            )])
            .unwrap();

            let a = zebo.get_document(1).unwrap();
            assert!(a.is_some());
        }

        #[test]
        fn test_real_example_2() {
            let test_dir = prepare_test_dir();

            // 1GB
            const PAGE_SIZE: u64 = 1024 * 1024 * 1024;

            let mut zebo: Zebo<1_000_000, PAGE_SIZE, u32> =
                Zebo::try_new(test_dir.clone()).unwrap();
            assert_eq!(zebo.next_page_id, 0);

            zebo.add_documents(vec![
                (
                    0,
                    MyDoc {
                        id: "1".to_string(),
                        text: r#"{"id"\:"1","text":"foo"}"#.to_string(),
                    },
                ),
                (
                    1,
                    MyDoc {
                        id: "2".to_string(),
                        text: r#"{"id"\:"1","text":"bar"}"#.to_string(),
                    },
                ),
            ])
            .unwrap();

            let a = zebo.get_document(0).unwrap();
            assert!(a.is_some());

            let a = zebo.get_document(1).unwrap();
            assert!(a.is_some());

            zebo.remove_documents(vec![0], true).unwrap();
        }

        #[test]
        fn test_real_example_3() {
            let test_dir = prepare_test_dir();

            // 1GB
            const PAGE_SIZE: u64 = 1024 * 1024 * 1024;

            let mut zebo = Zebo::<5, PAGE_SIZE, u64>::try_new(test_dir.clone())
                .expect("Failed to create Zebo instance");

            zebo.add_documents(vec![(
                1,
                MyDoc {
                    id: "Document 1".to_string(),
                    text: "This is the content of document 1.".to_string(),
                },
            )])
            .expect("Failed to add documents");

            let info_before = zebo.get_info().unwrap();
            assert_eq!(info_before.page_headers.len(), 1);
            drop(zebo);

            let mut zebo = Zebo::<5, PAGE_SIZE, u64>::try_new(test_dir.clone())
                .expect("Failed to create Zebo instance");

            // Test realod of Zebo instance doesn't change the info
            let info_after = zebo.get_info().unwrap();
            assert_eq!(info_before, info_after);

            zebo.add_documents(vec![(
                4,
                MyDoc {
                    id: "Document 4".to_string(),
                    text: "This is the content of document 4.".to_string(),
                },
            )])
            .expect("Failed to add documents");

            // Reload doesn't create a new page
            let info = zebo.get_info().unwrap();
            assert_eq!(info.page_headers.len(), 1);

            drop(zebo);

            let mut zebo = Zebo::<5, PAGE_SIZE, u64>::try_new(test_dir)
                .expect("Failed to create Zebo instance");

            zebo.add_documents(vec![(
                5,
                MyDoc {
                    id: "Document 5".to_string(),
                    text: "This is the content of document 5.".to_string(),
                },
            )])
            .expect("Failed to add documents");

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
    }

    mod multi {
        use super::*;

        #[test]
        fn test_add_multi_single_page() {
            let test_dir = prepare_test_dir();

            let mut zebo = Zebo::<5, 128, u64>::try_new(test_dir.clone())
                .expect("Failed to create Zebo instance");

            zebo.add_documents_batch(vec![(1, "foo"), (2, "barr")], 5, 5)
                .unwrap();

            let output: Vec<_> = zebo
                .get_documents(vec![1, 2])
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();
            let first = output.iter().find(|(id, _)| id == &1).unwrap();
            let second = output.iter().find(|(id, _)| id == &2).unwrap();

            assert_eq!(&first.1, "foo".as_bytes());
            assert_eq!(&second.1, "barr".as_bytes());

            let info = zebo.get_info().unwrap();
            assert_eq!(info.page_headers.len(), 1);
            assert_eq!(info.page_headers[0].document_count, 2);
        }

        #[test]
        fn test_add_multi_exact_batch_size() {
            let test_dir = prepare_test_dir();

            let mut zebo = Zebo::<5, 128, u64>::try_new(test_dir.clone())
                .expect("Failed to create Zebo instance");

            zebo.add_documents_batch(vec![(1, "foo"), (2, "barr")], 2, 5)
                .unwrap();

            let output: Vec<_> = zebo
                .get_documents(vec![1, 2])
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();
            let first = output.iter().find(|(id, _)| id == &1).unwrap();
            let second = output.iter().find(|(id, _)| id == &2).unwrap();

            assert_eq!(&first.1, "foo".as_bytes());
            assert_eq!(&second.1, "barr".as_bytes());

            let info = zebo.get_info().unwrap();
            assert_eq!(info.page_headers.len(), 1);
            assert_eq!(info.page_headers[0].document_count, 2);
        }

        #[test]
        fn test_add_multi_paged_1() {
            let test_dir = prepare_test_dir();

            let mut zebo = Zebo::<4, 128, u64>::try_new(test_dir.clone())
                .expect("Failed to create Zebo instance");

            zebo.add_documents_batch(
                vec![
                    (1, "text 1"),
                    (2, "text 2"),
                    (3, "text 3"),
                    (4, "text 4"),
                    (5, "text 5"),
                    (6, "text 6"),
                ],
                2,
                5,
            )
            .unwrap();

            let output: Vec<_> = zebo
                .get_documents(vec![1, 2, 3, 4, 5, 6])
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();

            let docs: Vec<_> = (1..=6)
                .map(|i| output.iter().find(|(id, _)| id == &i).unwrap().clone())
                .collect();
            assert_eq!(
                &docs,
                &[
                    (1_u64, "text 1".as_bytes().to_vec()),
                    (2_u64, "text 2".as_bytes().to_vec()),
                    (3_u64, "text 3".as_bytes().to_vec()),
                    (4_u64, "text 4".as_bytes().to_vec()),
                    (5_u64, "text 5".as_bytes().to_vec()),
                    (6_u64, "text 6".as_bytes().to_vec())
                ]
            );

            let info = zebo.get_info().unwrap();
            assert_eq!(info.page_headers.len(), 2);
            assert_eq!(info.page_headers[0].document_count, 4);
            assert_eq!(info.page_headers[1].document_count, 2);
        }

        #[test]
        fn test_add_multi_paged2() {
            let test_dir = prepare_test_dir();

            let mut zebo = Zebo::<2, 128, u64>::try_new(test_dir.clone())
                .expect("Failed to create Zebo instance");

            zebo.add_documents_batch(
                vec![
                    (1, "text 1"),
                    (2, "text 2"),
                    (3, "text 3"),
                    (4, "text 4"),
                    (5, "text 5"),
                    (6, "text 6"),
                ],
                2,
                5,
            )
            .unwrap();

            let output: Vec<_> = zebo
                .get_documents(vec![1, 2, 3, 4, 5, 6])
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();

            let docs: Vec<_> = (1..=6)
                .map(|i| output.iter().find(|(id, _)| id == &i).unwrap().clone())
                .collect();
            assert_eq!(
                &docs,
                &[
                    (1_u64, "text 1".as_bytes().to_vec()),
                    (2_u64, "text 2".as_bytes().to_vec()),
                    (3_u64, "text 3".as_bytes().to_vec()),
                    (4_u64, "text 4".as_bytes().to_vec()),
                    (5_u64, "text 5".as_bytes().to_vec()),
                    (6_u64, "text 6".as_bytes().to_vec())
                ]
            );

            let info = zebo.get_info().unwrap();
            assert_eq!(info.page_headers.len(), 3);
            assert_eq!(info.page_headers[0].document_count, 2);
            assert_eq!(info.page_headers[1].document_count, 2);
            assert_eq!(info.page_headers[2].document_count, 2);
        }
    }

    #[test]
    fn test_add_mized() {
        let test_dir = prepare_test_dir();

        let mut zebo =
            Zebo::<2, 128, u64>::try_new(test_dir.clone()).expect("Failed to create Zebo instance");

        zebo.add_documents_batch(
            vec![
                (1, "text 1"),
                (2, "text 2"),
                (3, "text 3"),
                (4, "text 4"),
                (5, "text 5"),
                (6, "text 6"),
            ],
            2,
            5,
        )
        .unwrap();

        zebo.add_documents(vec![(7, "text 7")]).unwrap();

        let output: Vec<_> = zebo
            .get_documents(vec![1, 2, 3, 4, 5, 6, 7])
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        let docs: Vec<_> = (1..=7)
            .map(|i| output.iter().find(|(id, _)| id == &i).unwrap().clone())
            .collect();
        assert_eq!(
            &docs,
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

        let info = zebo.get_info().unwrap();
        assert_eq!(info.page_headers.len(), 4);
        assert_eq!(info.page_headers[0].document_count, 2);
        assert_eq!(info.page_headers[1].document_count, 2);
        assert_eq!(info.page_headers[2].document_count, 2);
        assert_eq!(info.page_headers[3].document_count, 1);
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
    }
}
