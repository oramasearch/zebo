# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Zebo is a Rust library for efficient filesystem-based document storage. It groups documents into fixed-size pages with configurable constraints on both document count per page and page size. The library is designed for high-performance data storage and retrieval with minimal memory overhead.

## Key Architecture Concepts

### Core Structure
- **Zebo**: Main struct with generic const parameters `MAX_DOC_PER_PAGE`, `PAGE_SIZE`, and document ID type
- **ZeboPage**: Represents a single page file containing multiple documents with binary layout
- **ZeboIndex**: Manages page mapping and document location tracking
- **DocumentId trait**: Abstraction for document identifiers (u32, u64)

### Storage Layout
- Each page is a binary file (`page_N.zebo`) with structured header and document data
- Index directory contains `index.index` file mapping document IDs to pages
- Page headers contain document count, limits, offsets, and document index entries
- Documents are stored contiguously after the page header

### Page Management
- Pages automatically created when current page exceeds document count or size limits
- Page size limit can be exceeded for single large documents
- Document deletion marks entries with sentinel values (u64::MAX, u32::MAX)
- Batch insertion API for improved performance with multiple documents

## Development Commands

### Building and Testing
```bash
# Check code compilation
cargo check

# Run all tests
cargo test

# Format code (must pass in CI)
cargo fmt

# Run linter (must pass in CI)
cargo clippy

# Build release
cargo build --release
```

### Running Examples
```bash
# Simple usage example
cargo run --example simple

# Performance testing example
cargo run --example perf

# Reload functionality example  
cargo run --example reload
```

## Testing Strategy

- Comprehensive unit tests in `src/lib.rs` covering single and batch operations
- Tests for page management, document retrieval, deletion, and persistence
- Page-level internal tests in `src/page.rs` for binary format validation
- Tests use temporary directories with unique names to avoid conflicts

## File Structure

- `src/lib.rs`: Main API, Zebo struct, iterators, and core functionality
- `src/page.rs`: Binary page format, document storage, and page operations
- `src/index.rs`: Page indexing and document-to-page mapping
- `src/error.rs`: Error types and Result type alias
- `examples/`: Usage examples demonstrating different features

## Key Implementation Details

### Generic Parameters
The main Zebo struct uses const generics:
- `MAX_DOC_PER_PAGE`: Maximum documents per page (typically 10-1M)  
- `PAGE_SIZE`: Target page size in bytes (can be exceeded for large documents)
- `DocId`: Document identifier type implementing DocumentId trait

### Document Operations
- `add_documents()`: Sequential document insertion
- `add_documents_batch()`: Optimized batch insertion with fewer disk writes
- `get_documents()`: Retrieve documents by ID (returns iterator)
- `remove_documents()`: Mark documents as deleted with optional data cleaning

### Persistence and Reloading
- Zebo instances can be recreated pointing to existing directories
- Index and pages are automatically loaded from filesystem
- Current page state is preserved across instance recreation