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
  pub fn new(
    max_chunk_size: usize,
    tokenizer_type: Tokenizer,
    chunk_overlap: usize,
  ) -> Result<Self, ChunkError> {
    let chunk_sizer = tokenizer_type.try_into()?;
    Ok(Self::new_with_sizer(
      max_chunk_size,
      chunk_overlap,
      chunk_sizer,
    ))
  }

  pub fn new_with_sizer(
    max_chunk_size: usize,
    chunk_overlap: usize,
    chunk_sizer: ConcreteSizer,
  ) -> Self {
    Self {
      max_chunk_size,
      chunk_overlap,
      chunk_sizer,
    }
  }
}

#[async_trait]
impl Chunker for TextChunker {
  async fn applies(
    &self,
    _file_path: &Path,
    mut reader: PeekableReader<Box<dyn AsyncRead + Unpin + Send>>,
  ) -> Result<
    PeekableReader<Box<dyn AsyncRead + Unpin + Send>>,
    PeekableReader<Box<dyn AsyncRead + Unpin + Send>>,
  > {
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
        Ok(reader)
      }
      Err(_) => Ok(reader),
    }
  }

  async fn chunk(
    &self,
    _file_path: &Path,
    mut reader: Box<dyn AsyncRead + Unpin + Send>,
  ) -> ChunkStream {
    let chunker = self.clone();
    Box::pin(async_stream::try_stream! {
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await?;
        if data.is_empty() {
            return;
        }

        let content = String::from_utf8_lossy(&data).into_owned();

        let config = ChunkConfig::new(chunker.max_chunk_size)
          .with_sizer(&chunker.chunk_sizer)
          .with_trim(false);
        let splitter = TextSplitter::new(config);

        for (offset, chunk_text) in splitter.chunk_indices(&content) {
            if chunk_text.trim().is_empty() {
                continue;
            }
            let mut start_offset = offset.saturating_sub(chunker.chunk_overlap);
            while start_offset > 0 && !content.is_char_boundary(start_offset) {
                start_offset -= 1;
            }
            let end_offset = offset + chunk_text.len();

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

            yield Chunk::Text(SemanticChunk {
                text: overlapped_text.to_string(),
                tokens,
                start_byte: start_offset,
                end_byte: end_offset,
            });
        }
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{Tokenizer, chunker::memory_async_reader, types::Chunk};
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
      Some(Ok(Chunk::Text(sc))) => assert!(!sc.text.is_empty()),
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

    assert!(
      chunks.len() >= 2,
      "expected multiple chunks to test overlap"
    );

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

      assert!(
        s2 < e1,
        "next chunk should overlap previous: s2={}, e1={}",
        s2,
        e1
      );

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
}
