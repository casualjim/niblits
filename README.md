# niblits

A powerful, token-aware text chunking library for processing multiple file formats with language-aware semantic splitting.

## Overview

This library provides streaming, async-first text chunking capabilities designed for ingestion pipelines and search systems. It handles diverse document types while maintaining semantic boundaries and offering configurable tokenization strategies.

## Features

### Multi-Format Support
- **Plain Text**: Basic text splitting with configurable overlap
- **Markdown**: Structure-aware chunking preserving headers and sections
- **HTML**: Tag-aware splitting that respects document structure
- **PDF**: Text extraction with intelligent chunking of document content
- **DOCX**: Word document parsing and content chunking
- **Source Code**: Semantic chunking for 50+ programming languages using tree-sitter grammars

### Language-Aware Code Chunking
- Grammar-aware parsing using tree-sitter
- Semantic boundary detection (functions, classes, etc.)
- Language-specific chunking strategies
- Support for Rust, Python, JavaScript, TypeScript, Go, and many more

### Flexible Tokenization
- **Character-based**: Simple character counting
- **OpenAI tiktoken**: cl100k_base, p50k_base, p50k_edit, r50k_base, o200k_base
- **HuggingFace**: Custom model tokenizers for specialized embeddings

### Streaming Architecture
- Async-first design with Stream API
- Memory-efficient processing of large files
- Progress tracking with file size monitoring
- Graceful error handling and recovery

## Quick Start

Add to your `Cargo.toml`:
```toml
[dependencies]
niblits = "0.3.0"
tokio = { version = "1", features = ["rt", "macros"] }
futures = "0.3"
```

```rust
use niblits::{chunk_stream, ChunkerConfig, Tokenizer};
use futures::StreamExt;
use std::io::Cursor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure chunking
    let config = ChunkerConfig {
        max_chunk_size: 1000,
        overlap_percentage: 0.2,
        tokenizer: Tokenizer::Tiktoken("cl100k_base".to_string()),
    };

    // Process a file
    let content = r#"fn main() {
    println!("Hello, world!");
}

fn helper() {
    println!("This is a helper function");
}"#;

    let reader = Cursor::new(content.as_bytes());
    let mut stream = chunk_stream("main.rs", reader, config).await;

    while let Some(result) = stream.next().await {
        match result? {
            project_chunk => {
                println!("File: {}", project_chunk.file_path);
                match project_chunk.chunk {
                    niblits::Chunk::Semantic(chunk) => {
                        println!("Semantic chunk: {} bytes", chunk.text.len());
                    }
                    niblits::Chunk::Text(chunk) => {
                        println!("Text chunk: {} bytes", chunk.text.len());
                    }
                    niblits::Chunk::EndOfFile { expected_chunks, .. } => {
                        println!("File complete. Expected {} chunks", expected_chunks);
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}
```

## Configuration

### ChunkerConfig

```rust
pub struct ChunkerConfig {
    /// Percentage of tokens to reserve for overlap (0.0 - 1.0)
    pub overlap_percentage: f32,
    /// Maximum size of each chunk (in tokens/characters)
    pub max_chunk_size: usize,
    /// Tokenizer strategy for size calculation
    pub tokenizer: Tokenizer,
}
```

### Tokenizer Options

```rust
pub enum Tokenizer {
    /// Simple character-based tokenization
    Characters,
    /// OpenAI tiktoken with encoding name
    Tiktoken(String),  // "cl100k_base", "p50k_base", etc.
    /// HuggingFace tokenizer with model ID
    HuggingFace(String),  // "bert-base-uncased", etc.
    // Preloaded variants (internal use)
    PreloadedTiktoken(Arc<CoreBPE>),
    PreloadedHuggingFace(Arc<Tokenizer>),
}
```

## Supported Languages

Check supported programming languages:

```rust
use niblits::{supported_languages, is_language_supported};

// Get all supported languages
let languages = supported_languages();
println!("Supported languages: {:?}", languages);

// Check specific language
assert!(is_language_supported("rust"));
assert!(is_language_supported("python"));
```

Commonly supported languages include: Rust, Python, JavaScript, TypeScript, Go, Java, C++, C#, Ruby, PHP, Swift, Kotlin, and many more.

## API Reference

### Core Functions

- `chunk_stream(path, reader, config)` - Process a file stream and yield chunks
- `walk_project(path, options)` - Recursively walk a directory and stream chunks
- `walk_files(files, project_root, options)` - Chunk a stream of file paths with ignore rules
- `walker_includes_path(project_root, path, max_file_size)` - Check if a path would be included
- `supported_languages()` - Get list of supported programming languages  
- `is_language_supported(name)` - Check if a language is supported

### Types

- `Chunk` - Represents different chunk types (Semantic, Text, EndOfFile, Delete)
- `SemanticChunk` - Contains text, tokens, and byte offset information
- `ProjectChunk` - File path, chunk data, and file size
- `ChunkError` - Error types for parsing, IO, and unsupported formats

## Examples

### Processing Different File Types

```rust
// Markdown file
let config = ChunkerConfig::default();
let reader = Cursor::new("# Header\n\nSome content\n\n## Subheader").as_bytes();
let stream = chunk_stream("doc.md", reader, config).await;

// PDF file  
let file = tokio::fs::File::open("document.pdf").await?;
let stream = chunk_stream("document.pdf", file, config).await;

// Code file
let code_stream = chunk_stream("script.py", python_file, config).await;
```

### Walking Projects

```rust
use niblits::{walk_project, WalkOptions};
use futures::StreamExt;

let mut stream = walk_project(
    "./my-project",
    WalkOptions {
        max_chunk_size: 1000,
        overlap_percentage: 0.2,
        ..Default::default()
    },
);

while let Some(result) = stream.next().await {
    let chunk = result?;
    println!("{} -> {:?}", chunk.file_path, chunk.chunk);
}
```

### Custom Tokenizer

```rust
// Using HuggingFace tokenizer
let config = ChunkerConfig {
    tokenizer: Tokenizer::HuggingFace("bert-base-uncased".to_string()),
    ..Default::default()
};

// Using characters for simple cases
let config = ChunkerConfig {
    tokenizer: Tokenizer::Characters,
    max_chunk_size: 500,
    overlap_percentage: 0.1,
};
```

## Architecture

```
src/
├── lib.rs              # Public API and main exports
├── types.rs            # Core data structures and error types
├── chunker/            # Format-specific chunkers
│   ├── code.rs         # Language-aware code chunking
│   ├── text.rs         # Plain text chunking
│   ├── markdown.rs     # Markdown-aware chunking
│   ├── html.rs         # HTML-aware chunking
│   ├── pdf.rs          # PDF processing
│   └── docx.rs         # Word document processing
├── languages.rs        # Language support utilities
├── grammars.rs         # Tree-sitter grammar management
└── grammar_loader.rs   # Dynamic grammar loading
```

## Performance Considerations

- **Streaming**: All processing is streaming-based to handle large files efficiently
- **Memory**: Minimal memory footprint with async I/O
- **Tokenizers**: Preload tokenizers for better performance in batch processing
- **Grammars**: Tree-sitter grammars are loaded on-demand and cached

## Development

### Building

```bash
mise build          # Build the workspace
mise build:rust     # Rust-only build
```

### Testing

```bash
mise test                    # All tests
mise test:rust  # Crate tests only
```

## Dependencies

Key dependencies:
- `text-splitter`: Core splitting logic with tokenization support
- `tree-sitter`: Code parsing for semantic chunking
- `tiktoken-rs`: OpenAI tokenizer implementation
- `tokenizers`: HuggingFace tokenizer support
- `oxidize-pdf`: PDF text extraction
- `docx-parser`: Word document parsing
- `htmd`: HTML processing
- `palate`: Language detection
