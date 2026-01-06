mod chunker;
mod grammar_loader;
pub mod languages;

mod types;

mod grammars;
mod walker;

use std::path::Path;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{
  Arc,
  atomic::{AtomicU64, Ordering},
};

use futures::{Stream, StreamExt};
use tokio::io::{AsyncRead, ReadBuf};

use crate::chunker::{get_chunker, get_chunker_with_overrides};
// Re-export main types
pub use crate::types::{
  Chunk, ChunkError, CodeParseInfo, CodeParseObserver, FileMetadata, ProjectChunk, SemanticChunk,
};
pub use crate::walker::{
  WalkOptions, walk_files, walk_files_with_observer, walk_project, walk_project_with_observer, walker_includes_path,
};
pub use chunker::ChunkerOverrides;

/// Tokenizer type for chunk size calculation
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
#[derive(Default)]
pub enum Tokenizer {
  /// Simple character-based tokenization
  #[serde(rename = "characters")]
  #[default]
  Characters,
  /// OpenAI tiktoken tokenizer with encoding name (e.g., "cl100k_base", "p50k_base")
  #[serde(rename = "tiktoken")]
  Tiktoken(#[serde(rename = "encoding")] String),
  /// Pre-loaded tiktoken tokenizer (internal use only, not exposed to bindings)
  #[doc(hidden)]
  #[serde(skip)]
  PreloadedTiktoken(std::sync::Arc<tiktoken_rs::CoreBPE>),
  /// HuggingFace tokenizer with specified model
  #[serde(rename = "huggingface")]
  HuggingFace(#[serde(rename = "model_id")] String),
  /// Pre-loaded HuggingFace tokenizer (internal use only, not exposed to bindings)
  #[doc(hidden)]
  #[serde(skip)]
  PreloadedHuggingFace(std::sync::Arc<tokenizers::Tokenizer>),
}

impl Tokenizer {
  /// Check if this tokenizer is preloaded
  pub fn is_preloaded(&self) -> bool {
    matches!(
      self,
      Tokenizer::PreloadedTiktoken(_) | Tokenizer::PreloadedHuggingFace(_)
    )
  }

  pub fn preload(self) -> Result<Self, ChunkError> {
    match self {
      Tokenizer::Tiktoken(encoding) => {
        let bpe = match encoding.as_str() {
          "cl100k_base" => tiktoken_rs::cl100k_base(),
          "p50k_base" => tiktoken_rs::p50k_base(),
          "p50k_edit" => tiktoken_rs::p50k_edit(),
          "r50k_base" => tiktoken_rs::r50k_base(),
          "o200k_base" => tiktoken_rs::o200k_base(),
          _ => {
            return Err(ChunkError::ParseError(format!(
              "Unknown tiktoken encoding: {}",
              encoding
            )));
          }
        };
        let bpe = bpe.map_err(|e| ChunkError::ParseError(format!("Failed to create tiktoken: {e}")))?;

        Ok(Tokenizer::PreloadedTiktoken(Arc::new(bpe)))
      }
      Tokenizer::HuggingFace(model_id) => {
        let tokenizer = tokenizers::tokenizer::Tokenizer::from_pretrained(&model_id, None)
          .map_err(|e| ChunkError::ParseError(format!("Failed to load HF tokenizer: {}", e)))?;
        Ok(Tokenizer::PreloadedHuggingFace(Arc::new(tokenizer)))
      }
      other => Ok(other),
    }
  }
}

impl std::fmt::Debug for Tokenizer {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Tokenizer::Characters => write!(f, "Characters"),
      Tokenizer::Tiktoken(name) => write!(f, "Tiktoken({})", name),
      Tokenizer::PreloadedTiktoken(_) => write!(f, "PreloadedTiktoken"),
      Tokenizer::HuggingFace(model) => write!(f, "HuggingFace({})", model),
      Tokenizer::PreloadedHuggingFace(_) => write!(f, "PreloadedHuggingFace"),
    }
  }
}

impl FromStr for Tokenizer {
  type Err = String;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    match s.to_lowercase().as_str() {
      "characters" => Ok(Tokenizer::Characters),
      _ if s.starts_with("tiktoken:") => {
        let encoding = s["tiktoken:".len()..].to_string();
        Ok(Tokenizer::Tiktoken(encoding))
      }
      _ if s.starts_with("hf:") => {
        let model_id = s["hf:".len()..].to_string();
        Ok(Tokenizer::HuggingFace(model_id))
      }
      _ => Err(format!("Unknown tokenizer type: {}", s)),
    }
  }
}

/// Configuration for the chunker
#[derive(Clone, Debug)]
pub struct ChunkerConfig {
  /// Percentage of tokens to reserve for overlap between chunks
  pub overlap_percentage: f32,
  /// Maximum size of each chunk (in tokens/characters)
  pub max_chunk_size: usize,
  /// Tokenizer to use for size calculation
  pub tokenizer: Tokenizer,
}

impl Default for ChunkerConfig {
  fn default() -> Self {
    Self {
      overlap_percentage: 0.2,
      max_chunk_size: 1500,
      tokenizer: Tokenizer::default(),
    }
  }
}

/// Get list of supported programming languages
///
/// # Returns
/// A vector of language names that can be used with `chunk_code`
///
/// # Example
/// ```
/// let languages = text_chunking::supported_languages();
/// assert!(languages.contains(&"rust"));
/// ```
pub fn supported_languages() -> Vec<&'static str> {
  languages::supported_languages()
}

/// Check if a language is supported
///
/// # Arguments
/// * `name` - Language name to check (case-insensitive)
///
/// # Returns
/// `true` if the language is supported
///
/// # Example
/// ```
/// assert!(text_chunking::is_language_supported("rust"));
/// assert!(text_chunking::is_language_supported("Python")); // case-insensitive
/// ```
pub fn is_language_supported(name: &str) -> bool {
  languages::is_language_supported(name)
}

struct CountingReader<R> {
  inner: R,
  bytes_read: Arc<AtomicU64>,
}

impl<R> CountingReader<R> {
  fn new(inner: R) -> (Self, CountingReaderHandle) {
    let bytes_read = Arc::new(AtomicU64::new(0));
    let handle = CountingReaderHandle {
      bytes_read: Arc::clone(&bytes_read),
    };
    (Self { inner, bytes_read }, handle)
  }
}

impl<R: AsyncRead + Send + Unpin> AsyncRead for CountingReader<R> {
  fn poll_read(
    mut self: Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
    buf: &mut ReadBuf<'_>,
  ) -> std::task::Poll<std::io::Result<()>> {
    let before = buf.filled().len();
    let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
    if let std::task::Poll::Ready(Ok(())) = &poll {
      let after = buf.filled().len();
      if after > before {
        self.bytes_read.fetch_add((after - before) as u64, Ordering::Relaxed);
      }
    }
    poll
  }
}

#[derive(Clone)]
struct CountingReaderHandle {
  bytes_read: Arc<AtomicU64>,
}

impl CountingReaderHandle {
  fn bytes_read(&self) -> u64 {
    self.bytes_read.load(Ordering::Relaxed)
  }
}

/// Process a single file-like stream and yield chunks as a stream
pub async fn chunk_stream<P, R>(
  path: P,
  reader: R,
  config: ChunkerConfig,
) -> impl Stream<Item = Result<ProjectChunk, ChunkError>> + Send
where
  P: AsRef<Path>,
  R: AsyncRead + Unpin + Send + 'static,
{
  let path = path.as_ref().to_owned();

  async_stream::try_stream! {
      let path_str = path.to_string_lossy().to_string();

      let (selected_chunker, file_reader) = get_chunker(&path, reader, config).await?;
      let (counting_reader, reader_stats) = CountingReader::new(file_reader);

      let mut chunk_stream = selected_chunker.chunk(&path, Box::new(counting_reader)).await;
      let mut chunk_count = 0usize;
      while let Some(chunk_result) = chunk_stream.next().await {
          let chunk = chunk_result?;
          chunk_count += 1;
          let file_size = reader_stats.bytes_read();
          yield ProjectChunk {
              file_path: path_str.clone(),
              chunk,
              file_size,
          };
      }

      if chunk_count == 0 {
          return;
      }

      let file_size = reader_stats.bytes_read();
      yield ProjectChunk {
          file_path: path_str.clone(),
          chunk: Chunk::EndOfFile {
              file_path: path_str,
              content: None,
              content_hash: None,
              file_metadata: None,
              expected_chunks: chunk_count,
          },
          file_size,
      };
  }
}

/// Process a single file-like stream and yield chunks as a stream, with overrides.
pub async fn chunk_stream_with_overrides<P, R>(
  path: P,
  reader: R,
  config: ChunkerConfig,
  overrides: ChunkerOverrides,
) -> impl Stream<Item = Result<ProjectChunk, ChunkError>> + Send
where
  P: AsRef<Path>,
  R: AsyncRead + Unpin + Send + 'static,
{
  let path = path.as_ref().to_owned();

  async_stream::try_stream! {
      let path_str = path.to_string_lossy().to_string();

      let (selected_chunker, file_reader) = get_chunker_with_overrides(&path, reader, config, overrides).await?;
      let (counting_reader, reader_stats) = CountingReader::new(file_reader);

      let mut chunk_stream = selected_chunker.chunk(&path, Box::new(counting_reader)).await;
      let mut chunk_count = 0usize;
      while let Some(chunk_result) = chunk_stream.next().await {
          let chunk = chunk_result?;
          chunk_count += 1;
          let file_size = reader_stats.bytes_read();
          yield ProjectChunk {
              file_path: path_str.clone(),
              chunk,
              file_size,
          };
      }

      if chunk_count == 0 {
          return;
      }

      let file_size = reader_stats.bytes_read();
      yield ProjectChunk {
          file_path: path_str.clone(),
          chunk: Chunk::EndOfFile {
              file_path: path_str,
              content: None,
              content_hash: None,
              file_metadata: None,
              expected_chunks: chunk_count,
          },
          file_size,
      };
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use futures::StreamExt;
  use std::io::Cursor;
  use std::path::Path;

  #[tokio::test]
  async fn test_process_file_semantic_rust() {
    let content = r#"fn main() {
    println!("Hello, world!");
}
"#;
    let path = Path::new("main.rs");
    let reader = Cursor::new(content.as_bytes().to_vec());
    let cfg = ChunkerConfig {
      max_chunk_size: 64,
      tokenizer: Tokenizer::Characters,
      overlap_percentage: 0.0,
    };

    let mut stream = Box::pin(chunk_stream(path, reader, cfg).await);

    let mut chunks = Vec::new();
    while let Some(item) = stream.next().await {
      chunks.push(item.expect("stream should yield Ok(ProjectChunk)"));
    }

    assert!(chunks.len() >= 2, "expect semantic chunks plus EOF");

    let file_size = content.len() as u64;

    // All content chunks should be Semantic
    let mut semantic_count = 0usize;
    for c in &chunks[..chunks.len() - 1] {
      assert_eq!(c.file_path, "main.rs");
      assert_eq!(c.file_size, file_size);
      match &c.chunk {
        Chunk::Semantic(sc) => {
          semantic_count += 1;
          assert!(!sc.text.is_empty());
        }
        other => panic!("expected semantic chunks, got {:?}", other),
      }
    }

    // Last should be EOF with expected_chunks == semantic_count
    match &chunks.last().unwrap().chunk {
      Chunk::EndOfFile {
        file_path,
        expected_chunks,
        ..
      } => {
        assert_eq!(file_path, "main.rs");
        assert_eq!(*expected_chunks, semantic_count);
      }
      other => panic!("expected EOF, got {:?}", other),
    }
  }

  #[tokio::test]
  async fn test_process_file_text_fallback() {
    let content = "lorem ipsum dolor sit amet, consectetur adipiscing elit.\n".repeat(10);
    let path = Path::new("notes.txt");
    let reader = Cursor::new(content.as_bytes().to_vec());
    let cfg = ChunkerConfig {
      max_chunk_size: 80,
      tokenizer: Tokenizer::Characters,
      overlap_percentage: 0.0,
    };

    let mut stream = Box::pin(chunk_stream(path, reader, cfg).await);

    let mut chunks = Vec::new();
    while let Some(item) = stream.next().await {
      chunks.push(item.expect("stream should yield Ok(ProjectChunk)"));
    }

    assert!(chunks.len() >= 2, "expect text chunks plus EOF");

    let file_size = content.len() as u64;

    let mut text_count = 0usize;
    for c in &chunks[..chunks.len() - 1] {
      assert_eq!(c.file_path, "notes.txt");
      assert_eq!(c.file_size, file_size);
      match &c.chunk {
        Chunk::Text(sc) => {
          text_count += 1;
          assert!(!sc.text.is_empty());
        }
        other => panic!("expected text chunks, got {:?}", other),
      }
    }

    match &chunks.last().unwrap().chunk {
      Chunk::EndOfFile {
        file_path,
        expected_chunks,
        ..
      } => {
        assert_eq!(file_path, "notes.txt");
        assert_eq!(*expected_chunks, text_count);
      }
      other => panic!("expected EOF, got {:?}", other),
    }
  }

  #[tokio::test]
  async fn test_process_pdf_file() {
    fn build_pdf(text: &str) -> Vec<u8> {
      let mut pdf = Vec::new();
      pdf.extend_from_slice(b"%PDF-1.4\n");

      let content_stream = format!("BT\n/F1 12 Tf\n36 720 Td\n({text}) Tj\nET\n");
      let objects = vec![
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n".to_string(),
        "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n".to_string(),
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>\nendobj\n".to_string(),
        format!(
          "4 0 obj\n<< /Length {} >>\nstream\n{}endstream\nendobj\n",
          content_stream.len(),
          content_stream
        ),
        "5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n".to_string(),
      ];

      let mut offsets = Vec::with_capacity(objects.len() + 1);
      offsets.push(0);

      for object in &objects {
        offsets.push(pdf.len());
        pdf.extend_from_slice(object.as_bytes());
      }

      let xref_position = pdf.len();
      let mut xref = format!("xref\n0 {}\n0000000000 65535 f \n", objects.len() + 1);
      for offset in offsets.iter().skip(1) {
        xref.push_str(&format!("{:010} 00000 n \n", offset));
      }
      pdf.extend_from_slice(xref.as_bytes());

      let trailer = format!(
        "trailer\n<< /Size {} /Root 1 0 R >>\nstartxref\n{}\n%%EOF\n",
        objects.len() + 1,
        xref_position
      );
      pdf.extend_from_slice(trailer.as_bytes());

      pdf
    }

    let content = "Hello from PDF";
    let data = build_pdf(content);
    let path = Path::new("document.pdf");
    let reader = Cursor::new(data);
    let cfg = ChunkerConfig {
      max_chunk_size: 200,
      tokenizer: Tokenizer::Characters,
      overlap_percentage: 0.0,
    };

    let mut stream = Box::pin(chunk_stream(path, reader, cfg).await);

    let mut saw_text_chunk = false;
    while let Some(item) = stream.next().await {
      let project_chunk = item.expect("stream should yield Ok(ProjectChunk)");
      if let Chunk::Text(sc) = &project_chunk.chunk {
        assert!(sc.text.contains(content));
        saw_text_chunk = true;
        break;
      }
    }

    assert!(saw_text_chunk, "expected to find a PDF text chunk");
  }

  #[tokio::test]
  async fn test_process_file_empty_input_yields_nothing() {
    let content = "";
    let path = Path::new("empty.txt");
    let reader = Cursor::new(content.as_bytes().to_vec());
    let cfg = ChunkerConfig {
      max_chunk_size: 80,
      tokenizer: Tokenizer::Characters,
      overlap_percentage: 0.0,
    };

    let mut stream = Box::pin(chunk_stream(path, reader, cfg).await);

    let mut count = 0usize;
    while let Some(_item) = stream.next().await {
      count += 1;
    }
    assert_eq!(count, 0, "empty input should produce no chunks");
  }
}
