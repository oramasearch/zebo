use zebo::Zebo;

fn main() {
    let data_dir = "./zebo_data_dir_simple";
    let mut zebo = Zebo::<
        // Max number of documents per file
        10,
        // Max number of bytes per file
        1024,
        // Document ID type
        u32,
    >::try_new(data_dir)
    .expect("Failed to create Zebo instance");

    // Batch insertions
    let space = zebo
        .reserve_space_for(&[(1, "Document 1"), (2, "Document 2"), (3, "Document 3")])
        .expect("Failed to reserve space for documents");
    space.write_all().expect("Failed to write documents");

    let mut docs = zebo
        // We found 1 and 3, but not 5 (does not exist)
        .get_documents(vec![1, 3, 5])
        .expect("Failed to get documents");
    while let Some(Ok((doc_id, doc))) = docs.next() {
        println!("Document ({doc_id}): {:?}", String::from_utf8(doc));
    }
}
