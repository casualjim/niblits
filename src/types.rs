use thiserror::Error;
use tree_sitter::Node;

#[derive(Error, Debug)]
pub enum ChunkError {
  #[error("Unsupported language: {0}")]
  UnsupportedLanguage(String),

  #[error("Unsupported file type for chunking: {0}")]
  UnsupportedFileType(String),

  #[error("Failed to parse content: {0}")]
  ParseError(String),

  #[error("IO error: {0}")]
  IoError(#[from] std::io::Error),

  #[error("Query error: {0}")]
  QueryError(String),
}

#[derive(Debug, Clone)]
/// Represents a chunk of code or text with semantic information
pub enum Chunk {
  Semantic(SemanticChunk),
  Text(SemanticChunk),
  EndOfFile {
    file_path: String,
    expected_chunks: usize, // Number of content chunks for this file
  },
  Delete {
    file_path: String,
  },
}

#[derive(Debug, Clone)]
pub struct SemanticChunk {
  pub text: String,
  pub tokens: Option<Vec<u32>>, // Token IDs if pre-tokenized
  pub start_byte: usize,
  pub end_byte: usize,
}

impl SemanticChunk {
  /// Calculate line numbers from byte offsets
  pub fn from_node(node: &Node, source: &str) -> Self {
    let start_byte = node.start_byte();
    let end_byte = node.end_byte();
    let text = source[start_byte..end_byte].to_string();

    Self {
      text,
      tokens: None,
      start_byte,
      end_byte,
    }
  }
}

/// A chunk from a project file with type information
#[derive(Debug, Clone)]
pub struct ProjectChunk {
  pub file_path: String,
  pub chunk: Chunk,
  pub file_size: u64,
}

impl ProjectChunk {
  /// Check if this is a semantic (parsed code) chunk
  pub fn is_semantic(&self) -> bool {
    matches!(self.chunk, Chunk::Semantic(_))
  }

  /// Check if this is a text (plain text) chunk
  pub fn is_text(&self) -> bool {
    matches!(self.chunk, Chunk::Text(_))
  }
}

/// File-level metadata
#[derive(Debug, Clone)]
pub struct FileMetadata {
  pub primary_language: Option<String>, // Primary language (e.g., "Python", "Rust")
  pub size: u64,                        // File size in bytes
  pub modified: std::time::SystemTime,  // Last modification time
  pub content_hash: String,             // SHA-256 hash of content
  pub line_count: usize,                // Total number of lines
  pub is_binary: bool,                  // Whether file was detected as binary
}

#[cfg(test)]
mod tests {
  use crate::languages;

  use super::*;
  use tree_sitter::{Language, Parser};

  #[test]
  fn test_semantic_chunk_from_node() {
    // Create a simple source code
    let source = "fn main() {\n    println!(\"Hello\");\n}";

    // Parse with tree-sitter (using rust parser as example)
    let mut parser = Parser::new();
    let lang: Language = languages::get_language("rust").unwrap().into();
    parser.set_language(&lang).unwrap();

    let tree = parser.parse(source, None).unwrap();
    let root = tree.root_node();

    // Find the function node
    let function_node = root.child(0).unwrap();

    let chunk = SemanticChunk::from_node(&function_node, source);

    assert_eq!(chunk.text, source);
    assert_eq!(chunk.tokens, None); // SemanticChunk::from_node doesn't set tokens
    assert_eq!(chunk.start_byte, 0);
    assert_eq!(chunk.end_byte, source.len());
  }

  #[test]
  fn test_chunk_error_display() {
    let err = ChunkError::UnsupportedLanguage("cobol".to_string());
    assert_eq!(err.to_string(), "Unsupported language: cobol");

    let err = ChunkError::ParseError("syntax error at line 5".to_string());
    assert_eq!(
      err.to_string(),
      "Failed to parse content: syntax error at line 5"
    );

    let err = ChunkError::QueryError("invalid capture name".to_string());
    assert_eq!(err.to_string(), "Query error: invalid capture name");
  }

  #[test]
  fn test_line_number_calculation() {
    let source = "line1\nline2\nline3\nline4\nline5";

    // Test various byte positions
    assert_eq!(source[..0].matches('\n').count() + 1, 1); // Start of file
    assert_eq!(source[..6].matches('\n').count() + 1, 2); // After first newline
    assert_eq!(source[..12].matches('\n').count() + 1, 3); // After second newline
    assert_eq!(source[..source.len()].matches('\n').count() + 1, 5); // End of file
  }
}
