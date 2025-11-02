# text-chunking

Token-aware, multi-format text chunking used for ingestion and search.

Capabilities:
- Parse and split text from multiple formats: plain text, markdown, HTML, PDF, DOCX, and code
- Grammar and language-aware chunking (`chunking/grammar_loader.rs`, `languages.rs`)
- Code-aware strategies (`chunking/chunker/code.rs`) and document strategies (`text.rs`, `markdown.rs`, `html.rs`, `pdf.rs`, `docx.rs`)
- Emits stable chunk identifiers and token counts via typed structures in `chunking/types.rs`

Intended usage:
- Produce semantically meaningful chunks for embedding and retrieval
- Control chunk size and boundaries via chunker implementations
- Integrate with ingestion pipeline described in docs/restate_indexing_pipeline.md

Structure:
- `chunking/` module with chunker implementations and grammar/language helpers
- `grammars.rs` - grammar support wiring
- `lib.rs` - crate exports

Status: internal crate.

See: ../../ARCHITECTURE.md and ../../docs/restate_indexing_pipeline.md
