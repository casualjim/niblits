use std::sync::Arc;

use blake3::Hasher;
use thiserror::Error;
use tree_sitter::{Language, Node, Tree};

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
    content: Option<String>,
    content_hash: Option<[u8; 32]>,
    file_metadata: Option<FileMetadata>,
    expected_chunks: usize, // Number of content chunks for this file
  },
  Delete {
    file_path: String,
  },
}

#[derive(Debug, Clone, Default)]
pub struct ChunkMetadata {
  pub node_type: String,
  pub node_name: Option<String>,
  pub language: String,
  pub parent_context: Option<String>,
  pub scope_path: Vec<String>,
  pub definitions: Vec<String>,
  pub references: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SemanticChunk {
  pub text: String,
  pub tokens: Option<Vec<u32>>, // Token IDs if pre-tokenized
  pub chunk_hash: [u8; 32],
  pub start_byte: usize,
  pub end_byte: usize,
  pub start_line: usize,
  pub end_line: usize,
  pub metadata: ChunkMetadata,
}

impl SemanticChunk {
  /// Calculate line numbers from byte offsets
  pub fn from_node(node: &Node, source: &str) -> Self {
    let start_byte = node.start_byte();
    let end_byte = node.end_byte();
    let text = source[start_byte..end_byte].to_string();

    Self::new(text, None, start_byte, end_byte, source)
  }

  pub fn new(text: String, tokens: Option<Vec<u32>>, start_byte: usize, end_byte: usize, source: &str) -> Self {
    let (start_line, end_line) = line_numbers_from_offsets(source, start_byte, end_byte);
    Self::with_line_numbers(text, tokens, start_byte, end_byte, start_line, end_line)
  }

  pub fn with_line_numbers(
    text: String,
    tokens: Option<Vec<u32>>,
    start_byte: usize,
    end_byte: usize,
    start_line: usize,
    end_line: usize,
  ) -> Self {
    let chunk_hash = chunk_hash_for_text(&text);
    Self {
      text,
      tokens,
      chunk_hash,
      start_byte,
      end_byte,
      start_line,
      end_line,
      metadata: ChunkMetadata::default(),
    }
  }
}

#[derive(Debug, Clone)]
pub(crate) struct LineIndex {
  line_starts: Vec<usize>,
  text_len: usize,
}

impl LineIndex {
  pub fn new(text: &str) -> Self {
    let line_starts = collect_line_starts(text);
    Self {
      line_starts,
      text_len: text.len(),
    }
  }

  pub fn line_numbers(&self, start_byte: usize, end_byte: usize) -> (usize, usize) {
    if end_byte == start_byte {
      (self.line_number(start_byte), self.line_number(start_byte))
    } else {
      let end = end_byte.saturating_sub(1);
      (self.line_number(start_byte), self.line_number(end))
    }
  }

  pub fn line_number(&self, byte_offset: usize) -> usize {
    let offset = byte_offset.min(self.text_len);

    self.line_starts.partition_point(|&start| start <= offset).max(1)
  }

  pub fn line_count(&self) -> usize {
    if self.text_len == 0 { 0 } else { self.line_starts.len() }
  }
}

fn chunk_hash_for_text(text: &str) -> [u8; 32] {
  let mut hasher = Hasher::new();
  hasher.update(text.as_bytes());
  let hash = hasher.finalize();
  let mut bytes = [0u8; 32];
  bytes.copy_from_slice(hash.as_bytes());
  bytes
}

pub(crate) fn count_line_breaks(text: &str) -> usize {
  count_line_breaks_in_prefix(text, text.len())
}

fn line_numbers_from_offsets(source: &str, start_byte: usize, end_byte: usize) -> (usize, usize) {
  let start = start_byte.min(source.len());
  let end = end_byte.min(source.len());
  let start_line = count_line_breaks_in_prefix(source, start) + 1;
  let end_line = count_line_breaks_in_prefix(source, end) + 1;
  (start_line, end_line)
}

fn collect_line_starts(text: &str) -> Vec<usize> {
  let bytes = text.as_bytes();
  let mut line_starts = Vec::new();
  line_starts.push(0);
  let mut index = 0;

  while index < bytes.len() {
    match bytes[index] {
      b'\r' => {
        if index + 1 < bytes.len() && bytes[index + 1] == b'\n' {
          index += 2;
        } else {
          index += 1;
        }
        line_starts.push(index);
      }
      b'\n' | b'\x0B' | b'\x0C' => {
        index += 1;
        line_starts.push(index);
      }
      0xC2 => {
        if index + 1 < bytes.len() && bytes[index + 1] == 0x85 {
          index += 2;
          line_starts.push(index);
        } else {
          index += 1;
        }
      }
      0xE2 => {
        if index + 2 < bytes.len() && bytes[index + 1] == 0x80 {
          match bytes[index + 2] {
            0xA8 | 0xA9 => {
              index += 3;
              line_starts.push(index);
            }
            _ => index += 1,
          }
        } else {
          index += 1;
        }
      }
      _ => index += 1,
    }
  }

  line_starts
}

fn count_line_breaks_in_prefix(text: &str, limit: usize) -> usize {
  let bytes = text.as_bytes();
  let mut count = 0;
  let mut index = 0;
  let limit = limit.min(bytes.len());

  while index < limit {
    match bytes[index] {
      b'\r' => {
        count += 1;
        if index + 1 < limit && bytes[index + 1] == b'\n' {
          index += 2;
        } else {
          index += 1;
        }
      }
      b'\n' | b'\x0B' | b'\x0C' => {
        count += 1;
        index += 1;
      }
      0xC2 => {
        if index + 1 < limit && bytes[index + 1] == 0x85 {
          count += 1;
          index += 2;
        } else {
          index += 1;
        }
      }
      0xE2 => {
        if index + 2 < limit && bytes[index + 1] == 0x80 {
          match bytes[index + 2] {
            0xA8 | 0xA9 => {
              count += 1;
              index += 3;
            }
            _ => index += 1,
          }
        } else {
          index += 1;
        }
      }
      _ => index += 1,
    }
  }

  count
}

#[derive(Debug, Clone)]
pub struct CodeParseInfo {
  pub file_path: String,
  pub language_id: String,
  pub language: Language,
  pub tree: Arc<Tree>,
  pub source: Arc<str>,
}

pub trait CodeParseObserver: Send + Sync {
  fn on_parse(&self, info: CodeParseInfo) -> Result<(), ChunkError>;
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
    assert_eq!(chunk.start_line, 1);
    assert_eq!(chunk.end_line, 3);
    assert_eq!(chunk.chunk_hash, *blake3::hash(source.as_bytes()).as_bytes());
  }

  #[test]
  fn test_chunk_error_display() {
    let err = ChunkError::UnsupportedLanguage("cobol".to_string());
    assert_eq!(err.to_string(), "Unsupported language: cobol");

    let err = ChunkError::ParseError("syntax error at line 5".to_string());
    assert_eq!(err.to_string(), "Failed to parse content: syntax error at line 5");

    let err = ChunkError::QueryError("invalid capture name".to_string());
    assert_eq!(err.to_string(), "Query error: invalid capture name");
  }

  #[test]
  fn test_line_number_calculation() {
    let source = "line1\r\nline2\nline3\rline4\u{2028}line5\u{2029}line6\u{000B}line7\u{000C}line8\u{0085}line9";

    assert_eq!(count_line_breaks(source), 8);
    assert_eq!(line_numbers_from_offsets(source, 0, 0), (1, 1));
    assert_eq!(line_numbers_from_offsets(source, 0, source.len()), (1, 9));

    let line_index = LineIndex::new(source);
    assert_eq!(line_index.line_count(), 9);
    assert_eq!(line_index.line_numbers(0, 0), (1, 1));
    assert_eq!(line_index.line_numbers(0, source.len()), (1, 9));
  }

  #[test]
  fn test_line_numbers_end_offset_at_newline() {
    let source = "line1\nline2";
    let line_index = LineIndex::new(source);
    let end_of_first_line = "line1\n".len();

    assert_eq!(
      line_index.line_numbers(0, end_of_first_line),
      (1, 1),
      "end offset at newline should stay on first line"
    );
  }
}
