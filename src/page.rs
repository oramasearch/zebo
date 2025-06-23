use std::{io::Write, os::unix::fs::FileExt};

use crate::{DocumentId, Result, Version, ZeboError, index::ProbableIndex};

pub const VERSION_OFFSET: u64 = 0;
pub const DOCUMENT_COUNT_LIMIT_OFFSET: u64 = VERSION_OFFSET + 1;
pub const DOCUMENT_COUNT_OFFSET: u64 = DOCUMENT_COUNT_LIMIT_OFFSET + 4;
pub const NEXT_AVAILABLE_OFFSET: u64 = DOCUMENT_COUNT_OFFSET + 4;
pub const NEXT_AVAILABLE_HEADER_OFFSET: u64 = NEXT_AVAILABLE_OFFSET + 4;
pub const STARTING_DOCUMENT_ID_OFFSET: u64 = NEXT_AVAILABLE_HEADER_OFFSET + 4;
pub const DOCUMENT_INDEX_OFFSET: u64 = STARTING_DOCUMENT_ID_OFFSET + 8;

// 1
// 12234556
// 3
// ......
// 0
// (0, 50, 2) // "ab"
// (u64::MAX, u32::MAX, u32::MAX) // "cd" // deleted
// (2, 54, 2) // "ef"
// (0, 0, 0) // not set yet
// (0, 0, 0) // not set yet
// (0, 0, 0) // not set yet
// (0, 0, 0) // not set yet
// (0, 0, 0) // not set yet
// (0, 0, 0) // not set yet
// (0, 0, 0) // not set yet
// ab
// cd
// ef

/// The page file is a file that contains the documents.
///
/// The file is structured as follows:
/// | Structure | content
/// |--|--
/// | `1 bytes` | version
/// | `4 bytes` | document limit
/// | `4 bytes` | number of documents
/// | `4 bytes` | next available offset
/// | `4 bytes` | next available header offset
/// | `8 bytes` | starting document id
/// | `(8 bytes, 4 bytes, 4 bytes) * MAX_DOC_PER_PAGE` | doc_id.as_u64(), starting offset, bytes length
/// | `variable length`  | documents
pub struct ZeboPage {
    document_limit: u32,
    #[allow(dead_code)]
    pub(crate) starting_document_id: u64,
    page_file: std::fs::File,
    next_available_header_offset: u32,
}

impl ZeboPage {
    pub fn try_new(
        document_limit: u32,
        starting_document_id: u64,
        mut page_file: std::fs::File,
    ) -> Result<Self> {
        // 8 bytes: doc_id.as_u64()
        // 4 bytes: starting offset
        // 4 bytes: bytes length
        let document_header_size = (4 + 4 + 8) * (document_limit as u64);
        // We shrink the file to contain at least the document header
        // this because we store documents *after* the header
        page_file
            .set_len(DOCUMENT_INDEX_OFFSET + document_header_size)
            .map_err(ZeboError::OperationError)?;

        // Version on first byte
        page_file
            .write_all_at(&[Version::V1.into()], VERSION_OFFSET)
            .map_err(ZeboError::OperationError)?;
        // document count limit
        page_file
            .write_all_at(&document_limit.to_be_bytes(), DOCUMENT_COUNT_LIMIT_OFFSET)
            .map_err(ZeboError::OperationError)?;
        // Number of documents
        page_file
            .write_all_at(&[0; 4], DOCUMENT_COUNT_OFFSET)
            .map_err(ZeboError::OperationError)?;
        // Next available offset
        let initial_available_offset = (DOCUMENT_INDEX_OFFSET + document_header_size) as u32;
        page_file
            .write_all_at(
                &initial_available_offset.to_be_bytes(),
                NEXT_AVAILABLE_OFFSET,
            )
            .map_err(ZeboError::OperationError)?;
        // Starting document id
        page_file
            .write_all_at(
                &starting_document_id.to_be_bytes(),
                STARTING_DOCUMENT_ID_OFFSET,
            )
            .map_err(ZeboError::OperationError)?;

        page_file.flush().map_err(ZeboError::OperationError)?;
        page_file.sync_all().map_err(ZeboError::OperationError)?;

        Ok(Self {
            document_limit,
            starting_document_id,
            page_file,
            next_available_header_offset: 0,
        })
    }

    pub fn try_load(page_file: std::fs::File) -> Result<Self> {
        let mut buf = [0; 1];
        page_file
            .read_exact_at(&mut buf, VERSION_OFFSET)
            .map_err(ZeboError::OperationError)?;
        let version = buf[0];

        if version != Version::V1.into() {
            return Err(ZeboError::UnsupportedVersion {
                version,
                wanted: Version::V1.into(),
            });
        }

        let mut buf = [0; 4];
        page_file
            .read_exact_at(&mut buf, DOCUMENT_COUNT_LIMIT_OFFSET)
            .map_err(ZeboError::OperationError)?;
        let document_limit = u32::from_be_bytes(buf);

        page_file
            .read_exact_at(&mut buf, NEXT_AVAILABLE_HEADER_OFFSET)
            .map_err(ZeboError::OperationError)?;
        let next_available_header_offset = u32::from_be_bytes(buf);

        let mut buf = [0; 8];
        page_file
            .read_exact_at(&mut buf, STARTING_DOCUMENT_ID_OFFSET)
            .map_err(ZeboError::OperationError)?;
        let starting_document_id = u64::from_be_bytes(buf);

        Ok(Self {
            page_file,
            document_limit,
            starting_document_id,
            next_available_header_offset,
        })
    }

    pub fn get_document_count(&self) -> Result<u32> {
        let mut buf = [0; 4];
        self.page_file
            .read_exact_at(&mut buf, DOCUMENT_COUNT_OFFSET)
            .map_err(ZeboError::OperationError)?;
        let document_count = u32::from_be_bytes(buf);

        Ok(document_count)
    }

    fn get_next_available_offset(&self) -> Result<u32> {
        let mut buf = [0; 4];
        self.page_file
            .read_exact_at(&mut buf, NEXT_AVAILABLE_OFFSET)
            .map_err(ZeboError::OperationError)?;
        let next_available_offset = u32::from_be_bytes(buf);

        Ok(next_available_offset)
    }

    pub fn current_file_size(&self) -> Result<u64> {
        let metadata = self
            .page_file
            .metadata()
            .map_err(ZeboError::OperationError)?;
        Ok(metadata.len())
    }

    pub fn get_header(&self) -> Result<ZeboPageHeader> {
        let document_count = self.get_document_count()?;
        let next_available_offset = self.get_next_available_offset()?;

        let mut doc_index = Vec::with_capacity(document_count as usize);
        let mut found = 0;
        let mut i: u64 = 0;
        while found < document_count {
            if i > (self.document_limit as u64) {
                break;
            }

            if let Some((doc_id, document_offset, document_len)) = self.get_at(i)? {
                // Reach the end of the data
                if document_offset == 0 {
                    break;
                }
                // this document is deleted
                if document_offset == u32::MAX {
                    i += 1;
                    continue;
                }

                doc_index.push((doc_id, document_offset, document_len));
                found += 1;
            }

            i += 1;
        }

        let header = ZeboPageHeader {
            document_limit: self.document_limit,
            document_count,
            next_available_offset,
            index: doc_index,
        };

        Ok(header)
    }

    pub fn get_documents<DocId: DocumentId>(
        &self,
        doc_id_with_index: &[(u64, ProbableIndex)],
    ) -> Result<Vec<(DocId, Vec<u8>)>> {
        let header = self.get_header()?;

        let mut r = Vec::with_capacity(doc_id_with_index.len());
        for (doc_id, _) in doc_id_with_index {
            let found = header.index.iter().find(|(d, _, _)| d == doc_id);
            if let Some((_, document_offset, document_len)) = found {
                let mut doc_buf = vec![0; *document_len as usize];
                // document_len == 0 is an edge but valid case.
                // It means that the document is empty.
                // In this case, we don't need to read the document from the file
                if document_len > &0 {
                    self.page_file
                        .read_exact_at(&mut doc_buf, *document_offset as u64)
                        .map_err(ZeboError::OperationError)?;
                }

                r.push((DocId::from_u64(*doc_id), doc_buf));
            }
        }

        Ok(r)
    }

    pub fn reserve_space(&mut self, doc_id: u64, len: u32) -> Result<ZeboReservedSpace> {
        let next_available_offset = self.get_next_available_offset()?;
        let document_count = self.get_document_count()?;

        let available_header_offset = self.next_available_header_offset;

        // increment the next available header offset by len
        {
            self.next_available_header_offset += 1;
            let buf = self.next_available_header_offset.to_be_bytes();
            self.page_file
                .write_all_at(&buf, NEXT_AVAILABLE_HEADER_OFFSET)
                .map_err(ZeboError::OperationError)?;
        }

        // increment the next available offset by len
        {
            let next_available_offset = next_available_offset + len;
            let buf = next_available_offset.to_be_bytes();
            self.page_file
                .write_all_at(&buf, NEXT_AVAILABLE_OFFSET)
                .map_err(ZeboError::OperationError)?;
        }

        // increment the document count by 1
        {
            let document_count = document_count + 1;
            let buf = document_count.to_be_bytes();
            self.page_file
                .write_all_at(&buf, DOCUMENT_COUNT_OFFSET)
                .map_err(ZeboError::OperationError)?;
        }

        // write the document index as (doc_id, document_offset, document_len)
        {
            let document_offset = next_available_offset;
            let mut buf = [0; 16];
            buf[0..8].copy_from_slice(&doc_id.to_be_bytes());
            buf[8..12].copy_from_slice(&document_offset.to_be_bytes());
            buf[12..16].copy_from_slice(&len.to_be_bytes());
            self.page_file
                .write_all_at(
                    &buf,
                    DOCUMENT_INDEX_OFFSET + (available_header_offset * (4 + 4 + 8)) as u64,
                )
                .map_err(ZeboError::OperationError)?;
        }

        Ok(ZeboReservedSpace {
            page: self,
            document_offset: next_available_offset,
            len,
        })
    }

    pub fn delete_documents(
        &mut self,
        documents_to_delete: &[(u64, ProbableIndex)],
        clean_data: bool,
    ) -> Result<u32> {
        // Should we sort documents_to_delete?
        // I have no idea if accessing to the page in a random way is slower than
        // ordered way. Probably yes.
        // TODO: make some tests

        // fill with 0 if requested
        if clean_data {
            let header = self.get_header()?;

            // Allocate the buffer only once
            let mut v: Vec<u8> = vec![];
            for (doc_id, _) in documents_to_delete {
                let found = header.index.iter().find(|(d, _, _)| d == doc_id);
                if let Some((_, document_offset, document_len)) = found {
                    let len = *document_len as usize;
                    if v.len() < len {
                        // Expand the vector if needed
                        v.resize(len, 0);
                    }

                    self.page_file
                        .write_all_at(&v[0..len], *document_offset as u64)
                        .map_err(ZeboError::OperationError)?;
                }
            }
        }

        let mut found = 0_u32;
        let mut buf = [0; 16];
        // Iterate over the header and erase the index
        // High inefficiency, but we don't care
        for i in 0..self.document_limit {
            let (doc_id, offset, _) = match self.get_at(i as u64)? {
                Some(x) => x,
                None => continue,
            };
            // No data in the header
            if offset == 0 {
                continue;
            }
            if documents_to_delete.iter().any(|(d, _)| *d == doc_id) {
                // Erase the document index
                buf[0..8].copy_from_slice(&u64::MAX.to_be_bytes());
                buf[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
                buf[12..16].copy_from_slice(&u32::MAX.to_be_bytes());
                self.page_file
                    .write_all_at(&buf, DOCUMENT_INDEX_OFFSET + (i * (4 + 4 + 8)) as u64)
                    .map_err(ZeboError::OperationError)?;

                found += 1;
            }
        }

        if found > 0 {
            // Update the document count
            let document_count = self.get_document_count()?;
            let new_document_count = document_count - found;
            self.page_file
                .write_all_at(&new_document_count.to_be_bytes(), DOCUMENT_COUNT_OFFSET)
                .map_err(ZeboError::OperationError)?;
        }

        self.page_file.flush().map_err(ZeboError::OperationError)?;
        self.page_file
            .sync_all()
            .map_err(ZeboError::OperationError)?;

        Ok(found)
    }

    fn get_at(&self, document_index: u64) -> Result<Option<(u64, u32, u32)>> {
        if (self.document_limit as u64) < document_index {
            return Ok(None);
        }

        let mut buf = [0; 16];

        self.page_file
            .read_exact_at(
                &mut buf,
                DOCUMENT_INDEX_OFFSET + (document_index * (4 + 4 + 8)),
            )
            .map_err(ZeboError::OperationError)?;

        let doc_id = u64::from_be_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);

        let document_offset = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
        let document_len = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);

        Ok(Some((doc_id, document_offset, document_len)))
    }

    pub fn close(&mut self) -> Result<()> {
        self.page_file.flush().map_err(ZeboError::OperationError)?;
        self.page_file
            .sync_all()
            .map_err(ZeboError::OperationError)?;

        Ok(())
    }

    /// Reserves space for multiple documents, batching disk writes for efficiency.
    pub fn reserve_multiple_space<'a, I: Iterator<Item = &'a (u64, u32)>>(
        &mut self,
        len: usize,
        docs: I,
    ) -> Result<ZeboMultiReservedSpace> {
        // Step 1: Read current state
        let mut next_available_offset = self.get_next_available_offset()?;
        let mut document_count = self.get_document_count()?;
        let mut available_header_offset = self.next_available_header_offset;
        let initial_available_header_offset = available_header_offset;

        // Put here because otherwise we cannot update self if it is captured by `ZeboReservedSpace`
        self.next_available_header_offset += len as u32;

        let mut total_len = 0;
        let initial_next_available_offset = next_available_offset;

        // Step 2: Prepare index entries and reserved spaces
        let mut index_buf = Vec::with_capacity(len * 16);
        for &(doc_id, len) in docs {
            // Prepare index entry
            let mut buf = [0u8; 16];
            buf[0..8].copy_from_slice(&doc_id.to_be_bytes());
            buf[8..12].copy_from_slice(&next_available_offset.to_be_bytes());
            buf[12..16].copy_from_slice(&len.to_be_bytes());
            index_buf.extend_from_slice(&buf);

            total_len += len;

            // Update counters for next doc
            next_available_offset =
                next_available_offset
                    .checked_add(len)
                    .ok_or(ZeboError::NotEnoughSpace {
                        limit: u32::MAX,
                        new_allocation_requested: len,
                    })?;
            available_header_offset =
                available_header_offset
                    .checked_add(1)
                    .ok_or(ZeboError::NotEnoughSpace {
                        limit: u32::MAX,
                        new_allocation_requested: 1,
                    })?;
            document_count = document_count
                .checked_add(1)
                .ok_or(ZeboError::NotEnoughSpace {
                    limit: u32::MAX,
                    new_allocation_requested: 1,
                })?;
        }

        // Step 3: Write all index entries in one go
        let start_index_offset =
            DOCUMENT_INDEX_OFFSET + (initial_available_header_offset as u64) * 16;

        self.page_file
            .write_all_at(&index_buf, start_index_offset)
            .map_err(ZeboError::OperationError)?;

        // Step 4: Write updated header/offset/count values
        let buf = self.next_available_header_offset.to_be_bytes();
        self.page_file
            .write_all_at(&buf, NEXT_AVAILABLE_HEADER_OFFSET)
            .map_err(ZeboError::OperationError)?;

        let buf = next_available_offset.to_be_bytes();
        self.page_file
            .write_all_at(&buf, NEXT_AVAILABLE_OFFSET)
            .map_err(ZeboError::OperationError)?;

        let buf = document_count.to_be_bytes();
        self.page_file
            .write_all_at(&buf, DOCUMENT_COUNT_OFFSET)
            .map_err(ZeboError::OperationError)?;

        Ok(ZeboMultiReservedSpace {
            page: self,
            len: total_len,
            document_offset: initial_next_available_offset,
        })
    }
}

pub struct ZeboMultiReservedSpace<'page> {
    page: &'page ZeboPage,
    len: u32,
    document_offset: u32,
}

impl ZeboMultiReservedSpace<'_> {
    pub fn write(self, data: Vec<u8>) -> Result<()> {
        if data.len() != self.len as usize {
            return Err(ZeboError::WrongReservedSpace {
                wanted: data.len(),
                reserved: self.len,
            });
        }

        self.page
            .page_file
            .write_all_at(&data, self.document_offset as u64)
            .map_err(ZeboError::OperationError)?;

        Ok(())
    }
}

pub struct ZeboReservedSpace<'page> {
    page: &'page ZeboPage,
    len: u32,
    document_offset: u32,
}

impl ZeboReservedSpace<'_> {
    pub fn write(self, data: &[u8]) -> Result<()> {
        let data_len = data.len() as u32;
        if data_len > self.len {
            return Err(ZeboError::NotEnoughReservedSpace {
                wanted: data.len(),
                reserved: self.len,
            });
        }

        self.page
            .page_file
            .write_all_at(data, self.document_offset as u64)
            .map_err(ZeboError::OperationError)?;

        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub struct ZeboPageHeader {
    pub document_limit: u32,
    pub document_count: u32,
    pub next_available_offset: u32,
    pub index: Vec<(u64, u32, u32)>,
}

#[cfg(test)]
mod tests {
    use crate::tests::prepare_test_dir;

    use super::*;

    #[test]
    fn test_zebo_page_check_internals_empty() {
        let test_dir = prepare_test_dir();

        let file_path = test_dir.join("page_0.zebo");
        let zebo_page_file = std::fs::File::options()
            .create(true)
            .truncate(false)
            // .append(true)
            .read(true)
            .write(true)
            .open(&file_path)
            .unwrap();
        let page = ZeboPage::try_new(2, 0, zebo_page_file).unwrap();

        assert_eq!(page.document_limit, 2);
        assert_eq!(page.get_document_count().unwrap(), 0);
        assert_eq!(page.get_next_available_offset().unwrap(), 57);
        let header = page.get_header().unwrap();
        assert_eq!(header.document_limit, 2);
        assert_eq!(header.document_count, 0);
        assert_eq!(header.next_available_offset, 57);
        assert_eq!(header.index.len(), 0);

        drop(page);

        let file_content = std::fs::read(&file_path).unwrap();

        // Version
        assert_eq!(file_content[0], Version::V1.into());
        // Limit
        assert_eq!(
            u32::from_be_bytes([
                file_content[1],
                file_content[2],
                file_content[3],
                file_content[4]
            ]),
            2
        );
        // Document count
        assert_eq!(
            u32::from_be_bytes([
                file_content[5],
                file_content[6],
                file_content[7],
                file_content[8]
            ]),
            0
        );
        // Next available offset
        assert_eq!(
            u32::from_be_bytes([
                file_content[9],
                file_content[10],
                file_content[11],
                file_content[12]
            ]),
            DOCUMENT_INDEX_OFFSET as u32 + (4 + 4 + 8) * 2
        );

        // Document index is preallocated for all documents
        for i in 0..2 {
            let offset = (DOCUMENT_INDEX_OFFSET + (i * (4 + 4 + 8))) as usize;
            // Document id
            assert_eq!(
                u64::from_be_bytes([
                    file_content[offset],
                    file_content[offset + 1],
                    file_content[offset + 2],
                    file_content[offset + 3],
                    file_content[offset + 4],
                    file_content[offset + 5],
                    file_content[offset + 6],
                    file_content[offset + 7]
                ]),
                0
            );
            // Document offset
            assert_eq!(
                u32::from_be_bytes([
                    file_content[offset + 8],
                    file_content[offset + 9],
                    file_content[offset + 10],
                    file_content[offset + 11]
                ]),
                0
            );
            // Document length
            assert_eq!(
                u32::from_be_bytes([
                    file_content[offset + 12],
                    file_content[offset + 13],
                    file_content[offset + 14],
                    file_content[offset + 15]
                ]),
                0
            );
        }
    }

    #[test]
    fn test_zebo_page_check_internals_add_doc() {
        let test_dir = prepare_test_dir();

        let file_path = test_dir.join("page_0.zebo");
        let zebo_page_file = std::fs::File::options()
            .create(true)
            .truncate(false)
            // .append(true)
            .read(true)
            .write(true)
            .open(&file_path)
            .unwrap();
        let mut page = ZeboPage::try_new(2, 0, zebo_page_file).unwrap();

        assert_eq!(page.document_limit, 2);
        assert_eq!(page.get_document_count().unwrap(), 0);
        assert_eq!(page.get_next_available_offset().unwrap(), 57);
        let header = page.get_header().unwrap();
        assert_eq!(header.document_limit, 2);
        assert_eq!(header.document_count, 0);
        assert_eq!(header.next_available_offset, 57);
        assert_eq!(header.index.len(), 0);

        let reserved_space = page.reserve_space(1, 2).unwrap();
        reserved_space.write("ab".as_bytes()).unwrap();

        drop(page);

        let file_content = std::fs::read(&file_path).unwrap();

        // Version
        assert_eq!(file_content[0], Version::V1.into());
        // Limit
        assert_eq!(
            u32::from_be_bytes([
                file_content[1],
                file_content[2],
                file_content[3],
                file_content[4]
            ]),
            2
        );
        // Document count
        assert_eq!(
            u32::from_be_bytes([
                file_content[5],
                file_content[6],
                file_content[7],
                file_content[8]
            ]),
            1
        );
        // Next available offset
        assert_eq!(
            u32::from_be_bytes([
                file_content[9],
                file_content[10],
                file_content[11],
                file_content[12]
            ]),
            DOCUMENT_INDEX_OFFSET as u32 + (4 + 4 + 8) * 2 + 2
        );

        // Document index is preallocated for all documents
        let i = 0;
        let offset = (DOCUMENT_INDEX_OFFSET + (i * (4 + 4 + 8))) as usize;
        // Document id
        assert_eq!(
            u64::from_be_bytes([
                file_content[offset],
                file_content[offset + 1],
                file_content[offset + 2],
                file_content[offset + 3],
                file_content[offset + 4],
                file_content[offset + 5],
                file_content[offset + 6],
                file_content[offset + 7]
            ]),
            1
        );
        // Document offset
        assert_eq!(
            u32::from_be_bytes([
                file_content[offset + 8],
                file_content[offset + 9],
                file_content[offset + 10],
                file_content[offset + 11]
            ]),
            57
        );
        // Document length
        assert_eq!(
            u32::from_be_bytes([
                file_content[offset + 12],
                file_content[offset + 13],
                file_content[offset + 14],
                file_content[offset + 15]
            ]),
            2
        );
    }

    #[test]
    fn test_zebo_page_check_internals_add_remove_add_doc() {
        let test_dir = prepare_test_dir();

        let file_path = test_dir.join("page_0.zebo");
        let zebo_page_file = std::fs::File::options()
            .create(true)
            .truncate(false)
            // .append(true)
            .read(true)
            .write(true)
            .open(&file_path)
            .unwrap();
        let mut page = ZeboPage::try_new(10, 0, zebo_page_file).unwrap();

        let reserved_space = page.reserve_space(1, 2).unwrap();
        reserved_space.write("ab".as_bytes()).unwrap();

        let reserved_space = page.reserve_space(2, 2).unwrap();
        reserved_space.write("cd".as_bytes()).unwrap();

        let reserved_space = page.reserve_space(3, 2).unwrap();
        reserved_space.write("ef".as_bytes()).unwrap();

        page.delete_documents(&[(2, ProbableIndex(0))], true)
            .unwrap();

        let reserved_space = page.reserve_space(4, 2).unwrap();
        reserved_space.write("ef".as_bytes()).unwrap();

        drop(page);

        let file_content = std::fs::read(&file_path).unwrap();

        assert_eq!(
            &file_content[41..89],
            &[
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0,
                0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 189, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0,
                191, 0, 0, 0, 2
            ]
        );
    }
}
