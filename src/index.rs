use std::{collections::HashMap, io::Write, os::unix::fs::FileExt, path::PathBuf};

use crate::{DocumentId, PageId, Result, Version, ZeboError};

pub struct ZeboIndex<DocId> {
    offset: u64,
    index_file: std::fs::File,
    #[cfg(test)]
    #[allow(dead_code)]
    index_file_path: PathBuf,
    p: std::marker::PhantomData<DocId>,
}

impl<DocId: DocumentId> ZeboIndex<DocId> {
    pub fn try_new(index_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&index_dir).map_err(ZeboError::OperationError)?;

        let index_file_path = index_dir.join("index.index");
        let index_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&index_file_path)
            .map_err(ZeboError::OperationError)?;

        index_file
            .write_all_at(&[Version::V1.into()], 0)
            .map_err(ZeboError::OperationError)?;

        // offset as u64
        let initial_offset = 9_u64.to_be_bytes();
        index_file
            .write_all_at(&initial_offset, 1)
            .map_err(ZeboError::OperationError)?;

        Ok(Self {
            offset: 8 + 1,
            index_file,
            p: std::marker::PhantomData,
            #[cfg(test)]
            index_file_path,
        })
    }

    pub fn try_load(index_dir: PathBuf) -> Result<Self> {
        let index_file_path = index_dir.join("index.index");
        let index_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&index_file_path)
            .map_err(ZeboError::OperationError)?;

        let mut buf = [0; 1];
        index_file
            .read_exact_at(&mut buf, 0)
            .map_err(ZeboError::OperationError)?;
        let version = buf[0];
        if version != Version::V1.into() {
            return Err(ZeboError::UnsupportedVersion {
                version,
                wanted: Version::V1.into(),
            });
        }

        let mut offset = [0; 8];
        index_file
            .read_exact_at(&mut offset, 1)
            .map_err(ZeboError::OperationError)?;
        let offset = u64::from_be_bytes(offset);

        Ok(Self {
            offset,
            index_file,
            p: std::marker::PhantomData,
            #[cfg(test)]
            index_file_path,
        })
    }

    pub fn new_page(&mut self, start_doc_id: DocId, page_id: PageId) -> Result<()> {
        // page_id -> u64 = 8 bytes
        // starting_doc_id -> u64 = 8 bytes
        // total = 8 + 8 = 16 bytes
        let mut buf = [0; 8 + 8];
        buf[0..8].copy_from_slice(&page_id.0.to_be_bytes());
        buf[8..16].copy_from_slice(&start_doc_id.as_u64().to_be_bytes());

        self.index_file
            .write_all_at(&buf, self.offset)
            .map_err(ZeboError::OperationError)?;

        self.offset += 8 + 8; // buf.len()
        self.index_file
            .write_all_at(&self.offset.to_be_bytes(), 1)
            .map_err(ZeboError::OperationError)?;

        self.index_file.flush().map_err(ZeboError::OperationError)?;
        self.index_file
            .sync_all()
            .map_err(ZeboError::OperationError)?;

        Ok(())
    }

    pub fn get_page_ids(&self) -> Result<Vec<PageId>> {
        let mut buf = vec![0; (self.offset - 8 - 1) as usize];
        self.index_file
            .read_exact_at(&mut buf, 9)
            .map_err(ZeboError::OperationError)?;

        let expected_page_size = (self.offset - 8 - 1) / (8 + 8);

        let mut pages = Vec::with_capacity(expected_page_size as usize);
        for chunk in buf.chunks_exact(16) {
            let page_id = u64::from_be_bytes(chunk[0..8].try_into().unwrap());
            pages.push(PageId(page_id));
        }

        Ok(pages)
    }

    pub fn get_pages<I: IntoIterator<Item = DocId>>(
        &self,
        doc_ids: I,
        result: &mut HashMap<PageId, Vec<(u64, ProbableIndex)>>,
    ) -> Result<()> {
        let mut buf = vec![0; (self.offset - 8 - 1) as usize];
        self.index_file
            .read_exact_at(&mut buf, 9)
            .map_err(ZeboError::OperationError)?;

        let expected_page_size = (self.offset - 8 - 1) / (8 + 8);

        // Pages are ordered by construction
        let mut pages = Vec::with_capacity(expected_page_size as usize);
        for chunk in buf.chunks_exact(16) {
            let page_id = u64::from_be_bytes(chunk[0..8].try_into().unwrap());
            let starting_doc_id = u64::from_be_bytes(chunk[8..16].try_into().unwrap());
            pages.push((starting_doc_id, PageId(page_id)));
        }

        for doc_id in doc_ids {
            let (starting_doc_id, page_id) =
                match pages.binary_search_by_key(&doc_id.as_u64(), |(s, _)| *s) {
                    Ok(index) => pages[index],
                    Err(index) => {
                        // This means it is previous of the first document
                        // Anyway, the developer inserts document using incremental doc id
                        // This means, the first page should start with the lowest doc id
                        // If the `binary_search_by_key` returns `Err(0)`, the current
                        // doc id is smallest than the first inserted document
                        // That should not happen, but if happen, we ignore it and thread it
                        // as "not found"
                        if index == 0 {
                            continue;
                        } else {
                            pages[index - 1]
                        }
                    }
                };

            let d = doc_id.as_u64();

            // Ensure the binary search above works correctly
            debug_assert!(starting_doc_id <= d);

            result
                .entry(page_id)
                .or_default()
                .push((d, ProbableIndex(d - starting_doc_id)));
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct ProbableIndex(pub u64);
