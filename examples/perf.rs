use std::time::Instant;

use zebo::Zebo;

#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
struct DocumentId(u64);

impl zebo::DocumentId for DocumentId {
    fn as_u64(&self) -> u64 {
        self.0
    }

    fn from_u64(id: u64) -> Self {
        DocumentId(id)
    }
}

static ZERO: &[u8] = b"\0";

#[derive(Clone)]
struct Document {
    id: String,
    data: String,
}
impl zebo::Document for Document {
    fn as_bytes(&self, v: &mut Vec<u8>) {
        v.extend(self.id.as_bytes());
        v.extend(ZERO);
        v.extend(self.data.as_bytes());
    }
}

// 1 GB
static PAGE_SIZE: u64 = 1024 * 5;

fn main() {
    const MAX_DOC_PER_PAGE: u32 = 100000;
    const LEN: usize = 64_000;

    // Prepare docs
    let docs_single: Vec<_> = (0..LEN)
        .map(|i| {
            (
                DocumentId(1),
                Document {
                    id: format!("Document {i}"),
                    data: format!("This is the content of document {i}."),
                },
            )
        })
        .collect();
    let docs_multi = docs_single.clone();

    // Run single

    println!("Run single");
    let data_dir = "./zebo_data_dir/perf/1/single";
    let _ = std::fs::remove_dir_all(data_dir);
    std::fs::create_dir_all(data_dir).unwrap();
    let mut zebo = Zebo::<MAX_DOC_PER_PAGE, PAGE_SIZE, DocumentId>::try_new(data_dir)
        .expect("Failed to create Zebo instance");
    let start = Instant::now();

    zebo.add_documents(docs_single)
        .expect("Failed to add documents");
    println!("Elapsed {:?}", start.elapsed());
    drop(zebo);

    // Run multi

    println!("Run multi");
    let data_dir = "./zebo_data_dir/perf/1/multi";
    let _ = std::fs::remove_dir_all(data_dir);
    std::fs::create_dir_all(data_dir).unwrap();
    let mut zebo = Zebo::<MAX_DOC_PER_PAGE, PAGE_SIZE, DocumentId>::try_new(data_dir)
        .expect("Failed to create Zebo instance");
    let start = Instant::now();
    zebo.add_documents_batch(docs_multi, 200, 512)
        .expect("Failed to add documents");
    println!("Elapsed {:?}", start.elapsed());
    drop(zebo);
}
