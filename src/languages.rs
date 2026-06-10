use std::path::Path;
use std::pin::Pin;

pub use palate::FileType;
use tokio::io::{AsyncRead, AsyncReadExt};
use tree_sitter_language::LanguageFn;

use crate::grammar_loader;

pub fn get_language(name: &str) -> Option<LanguageFn> {
  grammar_loader::get_language_fn(name)
}

pub fn supported_languages() -> Vec<&'static str> {
  grammar_loader::supported_languages()
}

pub fn is_language_supported(name: &str) -> bool {
  grammar_loader::is_language_supported(name)
}

const MAX_CONTENT_SIZE_BYTES: usize = 51200;

#[derive(Debug)]
pub struct PeekableReader<R> {
  inner: R,
  buffer: Vec<u8>,
  max_buffer: usize,
  cursor: usize,
  inner_exhausted: bool,
}

impl<R: AsyncRead + Send + Unpin + 'static> PeekableReader<R> {
  pub fn new(inner: R, max_buffer: usize) -> Self {
    Self {
      inner,
      buffer: Vec::with_capacity(max_buffer.min(16384)),
      max_buffer,
      cursor: 0,
      inner_exhausted: false,
    }
  }

  fn target_len(&self, requested: usize) -> usize {
    requested.min(self.max_buffer)
  }

  async fn ensure_buffer_len(&mut self, target: usize) -> Result<(), std::io::Error> {
    let target = self.target_len(target);
    while self.buffer.len() < target && !self.inner_exhausted {
      let remaining = target - self.buffer.len();
      if remaining == 0 {
        break;
      }

      let chunk_size = remaining.min(8192);
      let mut temp = vec![0u8; chunk_size];
      let read = self.inner.read(&mut temp).await?;
      if read == 0 {
        self.inner_exhausted = true;
        break;
      }
      self.buffer.extend_from_slice(&temp[..read]);
    }
    Ok(())
  }

  /// Peek ahead in the stream to determine shebang information
  /// This returns the content without consuming the stream
  pub async fn peek_first_line(&mut self) -> Result<Vec<u8>, std::io::Error> {
    let start = self.cursor;
    let target = start + 1024;
    self.ensure_buffer_len(target).await?;

    if self.buffer.len() <= start {
      return Ok(Vec::new());
    }

    let slice = &self.buffer[start..];
    let limit = slice.len().min(1024);
    let limited_slice = &slice[..limit];

    let newline_pos = limited_slice
      .iter()
      .position(|&b| b == b'\n' || b == b'\r')
      .unwrap_or(limited_slice.len());

    Ok(limited_slice[..newline_pos].to_vec())
  }

  /// Read up to max_bytes for full content analysis
  /// This will expand the buffer as needed
  pub async fn peek_content(&mut self, max_bytes: usize) -> Result<Vec<u8>, std::io::Error> {
    let start = self.cursor;
    let target = start + max_bytes;
    self.ensure_buffer_len(target).await?;

    let end = (start + max_bytes).min(self.buffer.len());
    if end <= start {
      return Ok(Vec::new());
    }

    Ok(self.buffer[start..end].to_vec())
  }

  pub fn rewind(&mut self) {
    self.cursor = 0;
  }

  /// Get the actual AsyncRead that can be used for processing after detection
  /// This preserves the buffered content
  pub fn into_async_read(self) -> impl AsyncRead + Send + Unpin {
    CombinedReader::new(self.buffer, self.inner)
  }
}

/// Combine a buffered prefix with the original stream
struct CombinedReader<R> {
  buffer: Vec<u8>,
  position: usize,
  inner: R,
}

impl<R: AsyncRead + Unpin> CombinedReader<R> {
  fn new(buffer: Vec<u8>, inner: R) -> Self {
    Self {
      buffer,
      position: 0,
      inner,
    }
  }
}

impl<R: AsyncRead + Unpin> AsyncRead for CombinedReader<R> {
  fn poll_read(
    mut self: Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
    buf: &mut tokio::io::ReadBuf<'_>,
  ) -> std::task::Poll<std::io::Result<()>> {
    // First read from buffer if available. Once bytes are written to `buf`,
    // return immediately instead of polling the inner reader in the same call:
    // an inner reader may return `Pending`, and `AsyncRead` callers must still
    // observe the buffered progress as `Ready`.
    if self.position < self.buffer.len() {
      let remaining_buffer = &self.buffer[self.position..];
      let to_read = buf.remaining().min(remaining_buffer.len());
      buf.put_slice(&remaining_buffer[..to_read]);
      self.position += to_read;

      return std::task::Poll::Ready(Ok(()));
    }

    if self.position >= self.buffer.len() && !self.buffer.is_empty() {
      self.buffer.clear();
      self.buffer.shrink_to_fit();
      self.position = 0;
    }

    // Then read from inner
    Pin::new(&mut self.inner).poll_read(cx, buf)
  }
}

/// Detects the programming language using a peekable reader
///
/// This function uses palate's file type detection which examines the path and
/// content to determine the file type. The detection includes filename patterns,
/// file extensions, shebangs, and content heuristics.
///
/// Returns the AsyncRead that has been re-useable after detection.
///
/// # Examples
/// ```no_run
/// use std::path::Path;
/// use std::io::Cursor;
/// use tokio::fs::File;
/// use niblits::languages::{detect, PeekableReader};
///
/// # tokio::runtime::Builder::new_current_thread()
/// #   .enable_all()
/// #   .build()
/// #   .unwrap()
/// #   .block_on(async {
/// // From memory buffer
/// let path = Path::new("script.py");
/// let content = "#!/usr/bin/env python\nprint('Hello')";
/// let cursor = Cursor::new(content);
/// let peekable = PeekableReader::new(cursor, 51200);
/// let (_file_type, _peekable) = match detect(path, peekable).await {
///   Ok(result) => result,
///   Err((err, _reader)) => panic!("detect failed: {err}"),
/// };
///
/// // From file for larger files
/// let file = match File::open("large_file.txt").await {
///   Ok(file) => file,
///   Err(err) => panic!("open failed: {err}"),
/// };
/// let peekable = PeekableReader::new(file, 51200);
/// let (_file_type, _file_reader) = match detect(path, peekable).await {
///   Ok(result) => result,
///   Err((err, _reader)) => panic!("detect failed: {err}"),
/// };
/// # });
/// ```
pub async fn detect<R>(
  path: &Path,
  mut content_reader: PeekableReader<R>,
) -> Result<(Option<FileType>, PeekableReader<R>), (std::io::Error, PeekableReader<R>)>
where
  R: AsyncRead + Send + Unpin + 'static,
{
  // Read content for detection
  let content_bytes = match content_reader.peek_content(MAX_CONTENT_SIZE_BYTES).await {
    Ok(content) => content,
    Err(e) => {
      return Err((e, content_reader));
    }
  };

  let content = String::from_utf8_lossy(&content_bytes);

  // Use palate's try_detect which handles all detection logic internally
  let file_type = palate::try_detect(path, &content);

  Ok((file_type, content_reader))
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::io::Cursor;
  use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  };
  use std::task::{Context, Poll};
  use tokio::io::ReadBuf;

  // A simple AsyncRead wrapper that counts how many bytes were actually read from the underlying reader.
  struct CountingReader<R> {
    inner: R,
    bytes_read: Arc<AtomicUsize>,
  }

  impl<R> CountingReader<R> {
    fn new(inner: R, bytes_read: Arc<AtomicUsize>) -> Self {
      Self { inner, bytes_read }
    }
  }

  impl<R: AsyncRead + Unpin> AsyncRead for CountingReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
      let before = buf.filled().len();
      let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
      if let Poll::Ready(Ok(())) = &poll {
        let after = buf.filled().len();
        if after > before {
          self.bytes_read.fetch_add(after - before, Ordering::SeqCst);
        }
      }
      poll
    }
  }

  // Reader-focused tests for PeekableReader and CombinedReader
  #[tokio::test]
  async fn test_peek_content_respects_max_bytes() {
    let content = vec![b'a'; 100_000];
    let bytes_read = Arc::new(AtomicUsize::new(0));
    let reader = CountingReader::new(Cursor::new(content), bytes_read.clone());
    let mut peekable = PeekableReader::new(reader, MAX_CONTENT_SIZE_BYTES);

    let max_bytes = 8192usize; // 8KiB request
    let out = peekable.peek_content(max_bytes).await.unwrap();

    let total = bytes_read.load(Ordering::SeqCst);
    assert!(
      total <= max_bytes,
      "peek_content should not read more than max_bytes; read {} > {}",
      total,
      max_bytes
    );
    assert_eq!(out.len(), max_bytes);
  }

  #[tokio::test]
  async fn test_peek_content_respects_max_buffer() {
    let content = vec![b'b'; 100_000];
    let small_max = 4096usize; // 4KiB internal cap
    let bytes_read = Arc::new(AtomicUsize::new(0));
    let reader = CountingReader::new(Cursor::new(content), bytes_read.clone());
    let mut peekable = PeekableReader::new(reader, small_max);

    // Request more than small_max; we must still cap reads at small_max
    let out = peekable.peek_content(10_000).await.unwrap();

    let total = bytes_read.load(Ordering::SeqCst);
    assert!(
      total <= small_max,
      "peek_content should not read more than max_buffer; read {} > {}",
      total,
      small_max
    );
    assert_eq!(out.len(), small_max);
  }

  #[tokio::test]
  async fn test_peek_first_line_reads_no_more_than_1kb() {
    // Create a large line without a newline to force the 1KB limit behavior.
    let content = vec![b'c'; 10_000];
    let bytes_read = Arc::new(AtomicUsize::new(0));
    let reader = CountingReader::new(Cursor::new(content), bytes_read.clone());
    let mut peekable = PeekableReader::new(reader, MAX_CONTENT_SIZE_BYTES);

    let _ = peekable.peek_first_line().await.unwrap();

    let total = bytes_read.load(Ordering::SeqCst);
    assert!(
      total <= 1024,
      "peek_first_line should not read more than 1KiB; read {} > 1024",
      total,
    );
  }

  #[tokio::test]
  async fn test_combined_reader_reads_buffer_then_inner() {
    // Directly test CombinedReader behavior using private access from child module
    let buffer = b"hello ".to_vec();
    let inner = Cursor::new(b"world".to_vec());
    let mut combined = CombinedReader::new(buffer, inner);

    use tokio::io::AsyncReadExt;
    let mut out = vec![0u8; 11];
    combined.read_exact(&mut out).await.unwrap();

    assert_eq!(std::str::from_utf8(&out).unwrap(), "hello world");
  }

  struct PendingReader;

  impl AsyncRead for PendingReader {
    fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
      Poll::Pending
    }
  }

  #[test]
  fn test_combined_reader_returns_ready_after_buffered_progress() {
    let mut combined = CombinedReader::new(b"hello".to_vec(), PendingReader);
    let mut out = [0u8; 16];
    let mut read_buf = ReadBuf::new(&mut out);
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);

    let poll = Pin::new(&mut combined).poll_read(&mut cx, &mut read_buf);

    assert!(matches!(poll, Poll::Ready(Ok(()))));
    assert_eq!(read_buf.filled(), b"hello");
  }

  #[tokio::test]
  async fn test_detect_with_content_shebang() {
    let python_content = "#!/usr/bin/env python\nprint('Hello, world!')";
    let path = Path::new("test");
    let cursor = Cursor::new(python_content);
    let peekable = PeekableReader::new(cursor, 51200);

    let (file_type, _) = detect(path, peekable).await.unwrap();
    let file_type = file_type.expect("expected file type detection");
    assert_eq!(file_type.canonical(), "python");
  }

  #[tokio::test]
  async fn test_detect_with_content_js() {
    let js_content = r#"function hello() {
console.log("testing");
return "JavaScript";
}"#;
    let path = Path::new("app.js");
    let cursor = Cursor::new(js_content);
    let peekable = PeekableReader::new(cursor, 51200);

    let (file_type, _) = detect(path, peekable).await.unwrap();
    assert!(file_type.is_some());
    let file_type = file_type.unwrap();
    assert_eq!(file_type.canonical(), "javascript");
  }

  #[tokio::test]
  async fn test_detect_extension_only() {
    // Test with empty content, should detect from extension only
    let path = Path::new("test.rs");
    let cursor = Cursor::new("");
    let peekable = PeekableReader::new(cursor, 51200);

    let (file_type, _) = detect(path, peekable).await.unwrap();
    assert!(file_type.is_some());
    let file_type = file_type.unwrap();
    // palate 0.3.8 returns the canonical name
    assert_eq!(file_type.canonical(), "rust");
  }

  #[tokio::test]
  async fn test_detect_with_actual_content() {
    // Test with actual Rust content
    let rust_content = r#"fn main() {
    println!("Hello, world!");
}"#;
    let path = Path::new("main.rs");
    let cursor = Cursor::new(rust_content);
    let peekable = PeekableReader::new(cursor, 51200);

    let (file_type, _) = detect(path, peekable).await.unwrap();

    assert!(file_type.is_some());
    assert_eq!(file_type.unwrap().canonical(), "rust");
  }

  #[tokio::test]
  async fn test_detect_empty_path() {
    // Test with empty path - palate returns None when path has no info to detect from
    let path = Path::new("");
    let cursor = Cursor::new("any content");
    let peekable = PeekableReader::new(cursor, 51200);

    let (file_type, _) = detect(path, peekable).await.unwrap();
    // Empty path returns None because there's no filename or extension to detect from
    assert!(file_type.is_none());
  }

  #[tokio::test]
  async fn test_detect_shebang_overrides_extension_conflict() {
    let content = "#!/usr/bin/env bash\necho hi\n";
    let path = Path::new("script.rs");
    let cursor = Cursor::new(content);
    let peekable = PeekableReader::new(cursor, 51200);

    let (file_type, _) = detect(path, peekable).await.unwrap();
    let file_type = file_type.expect("expected file type detection");
    let canonical = file_type.canonical();
    assert!(
      canonical != "rust" && canonical != "render_script",
      "expected shebang to override extension, got {:?}",
      file_type
    );
  }

  #[tokio::test]
  async fn test_detect_content_overrides_single_candidate_extension() {
    // Note: palate 0.3.8 prioritizes file extension over content in most cases.
    // A .py file will be detected as python even if content looks like JavaScript.
    let js_content = r#"function hello() {
  console.log("not python");
}"#;
    let path = Path::new("script.py");
    let cursor = Cursor::new(js_content);
    let peekable = PeekableReader::new(cursor, 51200);

    let (file_type, _) = detect(path, peekable).await.unwrap();
    let file_type = file_type.expect("expected some file type detection");
    // With palate 0.3.8, .py files are detected as python based on extension
    assert_eq!(
      file_type.canonical(),
      "python",
      "expected extension to take precedence, got {:?}",
      file_type
    );
  }
}
