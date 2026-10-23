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

`reserve_space_for` requires strictly ascending document IDs within each batch.
Unsorted or duplicate IDs return `ZeboError::UnsortedBatch { position }` before
any file is changed; `position` is the first zero-based index whose ID is not
greater than the preceding ID. Callers must sort and deduplicate before reserving.

Across batches, IDs should also increase. For replay compatibility, a batch
starting at or below an earlier reserved ID is accepted with a warning. When
that batch rotates the page, the new page starts above the closing page's
maximum ID, keeping page starts ascending. Replayed IDs below the new start
still route to an earlier page and return its older copy, or nothing if no copy
exists there. This API does not provide last-write-wins updates across batches.
If no greater page start fits the document ID type, rotation returns
`ZeboError::PageStartOverflow { last_id }` before any file is changed.

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

## Wrapped pages and deletions

Pages whose next-write offset lies inside their index or below their file length
are read-only. Inserts rotate to a fresh page; deletion records go to
`page_N.deleted`, an append-only sequence of big-endian `u64` document IDs.
Every lookup, iteration, count and inspection consults that sidecar. Deletes sync
the sidecar before returning, and the next append discards an incomplete trailing
record left by an interrupted write. `clean_data` cannot erase payloads on these
pages. Keep the sidecar with the page when copying or backing up storage.

Compaction skips wrapped pages. Once no readable documents remain, wrapped pages
and their sidecars are removed. Before removal, an atomically replaced and synced
`watermark` file preserves the maximum document ID and next page number, so a
restart cannot reuse IDs or reopen an earlier page for inserts. Older stores
without a watermark remain readable. Include `watermark` in backups.
