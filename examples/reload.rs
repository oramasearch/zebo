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
    let data_dir = "./zebo_data_dir";
    let mut zebo = Zebo::<5, PAGE_SIZE, DocumentId>::try_new(data_dir)
        .expect("Failed to create Zebo instance");

    zebo.add_documents(vec![(
        DocumentId(1),
        Document {
            id: "Document 1".to_string(),
            data: "This is the content of document 1.".to_string(),
        },
    )])
    .expect("Failed to add documents");

    let info_before = zebo.get_info().unwrap();
    drop(zebo);

    let mut zebo = Zebo::<5, PAGE_SIZE, DocumentId>::try_new(data_dir)
        .expect("Failed to create Zebo instance");

    let info_after = zebo.get_info().unwrap();
    assert_eq!(info_before, info_after);

    zebo.add_documents(vec![(
        DocumentId(4),
        Document {
            id: "Document 4".to_string(),
            data: "This is the content of document 4.".to_string(),
        },
    )])
    .expect("Failed to add documents");

    let info = zebo.get_info();
    println!("Zebo Info: {info:#?}");

    drop(zebo);
    let mut zebo = Zebo::<5, PAGE_SIZE, DocumentId>::try_new(data_dir)
        .expect("Failed to create Zebo instance");

    zebo.add_documents(vec![(
        DocumentId(5),
        Document {
            id: "Document 5".to_string(),
            data: "This is the content of document 5.".to_string(),
        },
    )])
    .expect("Failed to add documents");

    let info = zebo.get_info();
    println!("Zebo Info: {info:#?}");
}
