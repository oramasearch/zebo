# Zebo

[![Rust](https://github.com/oramasearch/zebo/actions/workflows/ci.yml/badge.svg)](https://github.com/oramasearch/zebo/actions/workflows/ci.yml)

Zebo is a simple library that help you to write data on FS effectively.
It groups your data in files according to the limitations you imposed.

## Usage

To use this crate, add the following to your `Cargo.toml`:

```toml
[dev-dependencies]
zebo = "*"
```

```rust
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
    let space = zebo.reserve_space_for(
        &[
            (1, "Document 1"),
            (2, "Document 2"),
            (3, "Document 3"),
        ],
    )
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
```

## Design choice

You may wonder why there's no "insert_document" method.
Instead, we have "reserve_space_for" method that reserves space for multiple documents at once.
This is because reserving space for multiple documents at once allows Zebo to optimize file usage and minimize fragmentation.

But, there's another important reason.
Consider you have `RwLock<Zebo>` and you want to insert a lot of documents.
Based on the current design, you can do:

```rust
use zebo::Zebo;
use std::sync::RwLock;

let zebo = Zebo::<50, 1024, u32>::try_new("./my-folder").unwrap();
let zebo = RwLock::new(zebo);

let mut lock = zebo.write().unwrap();
let space = lock.reserve_space_for(&[(1_u32, "my content")]).unwrap();
drop(lock); // Release the lock ASAP
space.write_all().unwrap(); // Write the content outside the lock
```

So you can write documents without holding the lock.
This because `space` points to a specific location in a specific file, and writing to that location does not require access to the `Zebo` instance itself.
