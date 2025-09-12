#![feature(coverage_attribute)]
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

pub use crate::page::ZeboPageReservedSpace;

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

    pub fn reserve_space_for<'docs, Doc: Document>(
        &mut self,
        docs: &'docs [(DocId, Doc)],
    ) -> Result<ZeboPageReservedSpace<'docs, DocId, Doc>> {
        let document_count = docs.len() as u32;
        if document_count > MAX_DOC_PER_PAGE {
            return Err(ZeboError::TooManyDocuments {
                max: MAX_DOC_PER_PAGE,
                got: document_count,
            });
        }

        if let Some((_, page)) = self.current_page.as_mut() {
            let total_document_count_in_page = page.next_available_header_offset;
            let remain = MAX_DOC_PER_PAGE.saturating_sub(total_document_count_in_page);

            if remain < document_count {
                page.close()?;
                self.current_page = None;
            }
        }

        if self.current_page.is_none() {
            self.build_next_page(docs[0].0)?;
        }

        let (_, current_page) = self.current_page.as_mut().expect("Set above");

        let reserved_page = current_page.reserve(docs)?;

        Ok(reserved_page)
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
}
