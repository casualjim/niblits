use std::path::Path;

use super::{ChunkStream, Chunker, ConcreteSizer};
use crate::{Tokenizer, languages::PeekableReader, types::*};
use async_trait::async_trait;
use text_splitter::{ChunkConfig, TextSplitter};
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Clone)]
pub struct TextChunker {
  max_chunk_size: usize,
  chunk_overlap: usize,
  chunk_sizer: ConcreteSizer,
}

impl TextChunker {
  pub fn new(max_chunk_size: usize, tokenizer_type: Tokenizer, chunk_overlap: usize) -> Result<Self, ChunkError> {
    let chunk_sizer = tokenizer_type.try_into()?;
    Ok(Self::new_with_sizer(max_chunk_size, chunk_overlap, chunk_sizer))
  }

  pub fn new_with_sizer(max_chunk_size: usize, chunk_overlap: usize, chunk_sizer: ConcreteSizer) -> Self {
    Self {
      max_chunk_size,
      chunk_overlap,
      chunk_sizer,
    }
  }
}

fn overlap_start_offset(content: &str, offset: usize, overlap_chars: usize) -> usize {
  if overlap_chars == 0 || offset == 0 {
    return offset;
  }

  let mut indices = Vec::new();
  for (index, _) in content[..offset].char_indices() {
    indices.push(index);
  }
  if indices.len() <= overlap_chars {
    0
  } else {
    indices[indices.len() - overlap_chars]
  }
}

#[async_trait]
impl Chunker for TextChunker {
  async fn applies(
    &self,
    _file_path: &Path,
    mut reader: PeekableReader<Box<dyn AsyncRead + Unpin + Send>>,
  ) -> Result<PeekableReader<Box<dyn AsyncRead + Unpin + Send>>, PeekableReader<Box<dyn AsyncRead + Unpin + Send>>> {
    let peeked = reader.peek_content(8192).await;
    match peeked {
      Ok(content) => {
        if let Some(file_type) = infer::get(&content) {
          if file_type.matcher_type() == infer::MatcherType::Text {
            return Ok(reader);
          } else {
            return Err(reader);
          }
        }
        if std::str::from_utf8(&content).is_err() {
          return Err(reader);
        }
        Ok(reader)
      }
      Err(_) => Ok(reader),
    }
  }

  async fn chunk(&self, file_path: &Path, mut reader: Box<dyn AsyncRead + Unpin + Send>) -> ChunkStream {
    let chunker = self.clone();
    let file_path = file_path.to_path_buf();
    let eof_file_path = file_path.to_string_lossy().to_string();
    Box::pin(async_stream::try_stream! {
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await?;
        if data.is_empty() {
            return;
        }

        let content = match String::from_utf8(data) {
          Ok(content) => content,
          Err(_) => {
            Err(ChunkError::UnsupportedFileType(file_path.to_string_lossy().to_string()))?;
            unreachable!();
          }
        };

        let config = ChunkConfig::new(chunker.max_chunk_size)
          .with_sizer(&chunker.chunk_sizer)
          .with_trim(false);
        let splitter = TextSplitter::new(config);

        let line_index = LineIndex::new(&content);
        let mut prev_end_offset = None;
        let mut chunk_count = 0usize;
        for (idx, (offset, chunk_text)) in splitter.chunk_indices(&content).enumerate() {
            if chunk_text.trim().is_empty() {
                continue;
            }
            let mut start_offset = offset;
            if chunker.chunk_overlap > 0 {
              let mut overlap_chars = chunker.chunk_overlap;
              if let Some(prev_end) = prev_end_offset
                && offset < prev_end {
                  let overlap_slice = &content[offset..prev_end];
                  let existing_overlap = overlap_slice.chars().count();
                  overlap_chars = overlap_chars.saturating_sub(existing_overlap);
                }
              if overlap_chars > 0 {
                start_offset = overlap_start_offset(&content, offset, overlap_chars);
              }
            }
            let end_offset = offset + chunk_text.len();
            prev_end_offset = Some(end_offset);

            let overlapped_text = &content[start_offset..end_offset];
            let tokens = match &chunker.chunk_sizer {
                ConcreteSizer::HuggingFace(tokenizer) => {
                    tokenizer.encode(overlapped_text, false)
                        .map(|encoding| encoding.get_ids().to_vec())
                        .ok()
                }
                ConcreteSizer::Tiktoken(tiktoken) => {
                    tiktoken.encode_ordinary(overlapped_text)
                        .into()
                }
                ConcreteSizer::Characters(_) => None,
            };

            let (start_line, end_line) = line_index.line_numbers(start_offset, end_offset);
            let metadata = ChunkMetadata {
              node_type: "text_chunk".to_string(),
              node_name: Some(format!("text_chunk_{}", idx + 1)),
              language: "text".to_string(),
              parent_context: Some(file_path.to_string_lossy().to_string()),
              scope_path: Vec::new(),
              definitions: Vec::new(),
              references: Vec::new(),
            };
            let semantic_chunk = SemanticChunk {
              metadata,
              ..SemanticChunk::with_line_numbers(
                overlapped_text.to_string(),
                tokens,
                start_offset,
                end_offset,
                start_line,
                end_line,
              )
            };

            chunk_count += 1;
            yield Chunk::Text(semantic_chunk);
        }

        if chunk_count > 0 {
          yield Chunk::EndOfFile {
            file_path: eof_file_path,
            content: None,
            content_hash: None,
            file_metadata: None,
            file_symbols: None,
            expected_chunks: chunk_count,
          };
        }
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{Tokenizer, chunker::memory_async_reader, languages::PeekableReader, types::Chunk};
  use futures::StreamExt;

  #[tokio::test]
  async fn test_streaming_time_to_first_chunk_text() {
    let chunker = TextChunker::new(30, Tokenizer::Characters, 0).unwrap();
    let mut content = String::new();
    for _ in 0..500 {
      content.push_str("lorem ipsum dolor sit amet, consectetur adipiscing elit.\n");
    }
    let reader = memory_async_reader(content.clone().into_bytes());
    let mut stream = chunker.chunk(Path::new("notes.txt"), reader).await;

    match stream.next().await {
      Some(Ok(Chunk::Text(sc))) => {
        assert!(!sc.text.is_empty());
        assert_eq!(sc.metadata.language, "text");
      }
      other => panic!("Expected first text chunk, got {:?}", other),
    }
  }

  #[tokio::test]
  async fn test_text_chunker_creation() {
    let chunker = TextChunker::new(1000, Tokenizer::Characters, 0).unwrap();
    assert_eq!(chunker.max_chunk_size, 1000);
  }

  #[tokio::test]
  async fn test_text_overlap_between_chunks() {
    let overlap = 10usize;
    let chunker = TextChunker::new(50, Tokenizer::Characters, overlap).unwrap();

    let content = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789\n".repeat(20);

    let reader = memory_async_reader(content.clone().into_bytes());
    let mut stream = chunker.chunk(Path::new("overlap.txt"), reader).await;

    let mut chunks = Vec::new();
    while let Some(result) = stream.next().await {
      chunks.push(result.expect("text chunking should succeed"));
    }

    assert!(chunks.len() >= 2, "expected multiple chunks to test overlap");

    let mut text_chunks = Vec::new();
    for c in &chunks {
      if let Chunk::Text(sc) = c {
        text_chunks.push((sc.start_byte, sc.end_byte, sc.text.clone()));
      }
    }

    assert!(text_chunks.len() >= 2, "need text chunks to verify overlap");

    for window in text_chunks.windows(2) {
      let (s1, e1, t1) = &window[0];
      let (s2, e2, t2) = &window[1];

      assert!(s2 < e1, "next chunk should overlap previous: s2={}, e1={}", s2, e1);

      let actual_overlap = e1 - s2;
      assert_eq!(
        actual_overlap, overlap,
        "expected exact overlap of {} bytes, got {} (s2={}, e1={})",
        overlap, actual_overlap, s2, e1
      );

      let suffix1 = &t1[t1.len() - overlap..];
      let prefix2 = &t2[..overlap];
      assert_eq!(suffix1, prefix2, "overlap content should match");

      assert!(*e2 > *s2 && *e1 > *s1);
    }
  }

  #[tokio::test]
  async fn test_text_chunker_rejects_unknown_binary() {
    let chunker = TextChunker::new(100, Tokenizer::Characters, 0).unwrap();
    let bytes = vec![0x00, 0xFF, 0x10, 0x80, 0x00, 0x13, 0x7F];
    let reader = memory_async_reader(bytes);
    let peekable = PeekableReader::new(reader, 8192);

    let result = chunker.applies(Path::new("data.bin"), peekable).await;
    assert!(result.is_err(), "expected binary payload to be rejected by TextChunker");
  }

  #[tokio::test]
  async fn test_text_chunker_rejects_late_invalid_utf8() {
    let chunker = TextChunker::new(100, Tokenizer::Characters, 0).unwrap();
    let mut bytes = vec![b'a'; 9000];
    bytes.push(0xFF);
    let reader = memory_async_reader(bytes);
    let mut stream = chunker.chunk(Path::new("late-invalid.txt"), reader).await;

    match stream.next().await {
      Some(Err(ChunkError::UnsupportedFileType(_))) => {}
      other => panic!("expected UnsupportedFileType for invalid utf-8, got {:?}", other),
    }
  }

  #[tokio::test]
  async fn test_text_overlap_multibyte_characters() {
    let overlap = 2usize;
    let chunker = TextChunker::new(3, Tokenizer::Characters, overlap).unwrap();
    let content = "😀😃😄😁😆😅";
    let reader = memory_async_reader(content.as_bytes().to_vec());
    let mut stream = chunker.chunk(Path::new("emoji.txt"), reader).await;

    let mut text_chunks = Vec::new();
    while let Some(item) = stream.next().await {
      let chunk = item.expect("text chunking should succeed");
      if let Chunk::Text(sc) = chunk {
        text_chunks.push((sc.start_byte, sc.end_byte));
      }
    }

    assert!(text_chunks.len() >= 2, "expected multiple chunks");

    let (_s1, e1) = text_chunks[0];
    let (s2, _e2) = text_chunks[1];
    assert!(s2 < e1, "expected overlap between chunks");
    let overlap_slice = &content[s2..e1];
    let actual_overlap = overlap_slice.chars().count();
    assert_eq!(
      actual_overlap, overlap,
      "expected {} character overlap, got {}",
      overlap, actual_overlap
    );
  }
}
