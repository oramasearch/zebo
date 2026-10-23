# Storage implementation

`lib.rs` routes ordered IDs to pages and rotates pages without changing earlier
payloads. `page.rs` bounds and chunks index scans. Wrapped pages are read-only:
lookups filter persistent deletion sidecars, and compaction skips them.

Before removing an empty wrapped page, the store atomically writes and syncs
its document-ID and page-number watermark, then removes the index entry and
unlinks the page and sidecar. A failed watermark write keeps the page indexed
for retry; a crash after index replacement may leave unreferenced files.
See the root README for file formats and backup requirements.
