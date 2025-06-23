use zebo::Zebo;

fn main() {
    let data_dir = "./zebo_data_dir";
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
    zebo.add_documents_batch(
        vec![
            (1, b"Document 1".to_vec()),
            (2, b"Document 2".to_vec()),
            (3, b"Document 3".to_vec()),
        ],
        200,
        1024,
    )
    .expect("Failed to add documents");

    zebo.add_documents(vec![
        (4, b"Document 4".repeat(100)),
        (5, b"Document 5".to_vec()),
    ])
    .expect("Failed to add documents");

    let mut docs = zebo
        .get_documents(vec![1, 3, 5])
        .expect("Failed to get documents");
    while let Some(Ok((doc_id, doc))) = docs.next() {
        println!("Document ({doc_id}): {:?}", String::from_utf8(doc));
    }
}
