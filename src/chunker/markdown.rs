use std::path::Path;

use super::{ChunkStream, Chunker, ConcreteSizer};
use crate::{
  Tokenizer,
  languages::{self, PeekableReader},
  types::*,
};
use async_trait::async_trait;
use text_splitter::{ChunkCharIndex, ChunkConfig, MarkdownSplitter};
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Clone)]
pub struct MarkdownChunker {
  max_chunk_size: usize,
  chunk_overlap: usize,
  chunk_sizer: ConcreteSizer,
}

impl MarkdownChunker {
  pub fn new(max_chunk_size: usize, tokenizer_type: Tokenizer, chunk_overlap: usize) -> Result<Self, ChunkError> {
    let chunk_sizer = tokenizer_type.try_into()?;
    Ok(Self::new_with_sizer(max_chunk_size, chunk_overlap, chunk_sizer))
  }

  pub fn new_with_sizer(max_chunk_size: usize, chunk_overlap: usize, chunk_sizer: ConcreteSizer) -> Self {
    let normalized_overlap = chunk_overlap.min(max_chunk_size.saturating_sub(1));
    Self {
      max_chunk_size,
      chunk_overlap: normalized_overlap,
      chunk_sizer,
    }
  }

  fn chunk_markdown_content(
    &self,
    content: &str,
    file_path: Option<&Path>,
  ) -> Result<Vec<SemanticChunk>, ChunkError> {
    if content.trim().is_empty() {
      return Ok(Vec::new());
    }

    let config = ChunkConfig::new(self.max_chunk_size)
      .with_sizer(&self.chunk_sizer)
      .with_trim(false);
    let config = config.with_overlap(self.chunk_overlap).map_err(|err| {
      ChunkError::ParseError(format!(
        "Invalid markdown chunk overlap ({}): {err}",
        self.chunk_overlap
      ))
    })?;

    let splitter = MarkdownSplitter::new(config);
    let line_index = LineIndex::new(content);
    let mut chunks = Vec::new();
    for (idx, ChunkCharIndex { chunk, byte_offset, .. }) in splitter.chunk_char_indices(content).enumerate() {
      if chunk.trim().is_empty() {
        continue;
      }

      let start_byte = byte_offset;
      let end_byte = start_byte + chunk.len();
      let tokens = match &self.chunk_sizer {
        ConcreteSizer::HuggingFace(tokenizer) => tokenizer
          .encode(chunk, false)
          .map(|encoding| encoding.get_ids().to_vec())
          .ok(),
        ConcreteSizer::Tiktoken(tiktoken) => Some(tiktoken.encode_ordinary(chunk)),
        ConcreteSizer::Characters(_) => None,
      };

      let (start_line, end_line) = line_index.line_numbers(start_byte, end_byte);
      let metadata = ChunkMetadata {
        node_type: "text_chunk".to_string(),
        node_name: Some(format!("markdown_chunk_{}", idx + 1)),
        language: "markdown".to_string(),
        parent_context: file_path.map(|path| path.to_string_lossy().to_string()),
        scope_path: Vec::new(),
        definitions: Vec::new(),
        references: Vec::new(),
      };
      chunks.push(SemanticChunk {
        metadata,
        ..SemanticChunk::with_line_numbers(
          chunk.to_string(),
          tokens,
          start_byte,
          end_byte,
          start_line,
          end_line,
        )
      });
    }

    Ok(chunks)
  }

  pub fn chunk_markdown_string(
    &self,
    content: String,
    file_path: Option<&Path>,
  ) -> Result<Vec<SemanticChunk>, ChunkError> {
    self.chunk_markdown_content(&content, file_path)
  }
}

#[async_trait]
impl Chunker for MarkdownChunker {
  async fn applies(
    &self,
    file_path: &Path,
    reader: PeekableReader<Box<dyn AsyncRead + Unpin + Send>>,
  ) -> Result<PeekableReader<Box<dyn AsyncRead + Unpin + Send>>, PeekableReader<Box<dyn AsyncRead + Unpin + Send>>> {
    match languages::detect(file_path, reader).await {
      Ok((detection, peekable)) => {
        let applies = detection.is_some_and(|d| is_markdown_language(d.language()));
        if applies { Ok(peekable) } else { Err(peekable) }
      }
      Err((_, peekable)) => Err(peekable),
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

      let content = String::from_utf8_lossy(&data).into_owned();
      let chunks = chunker.chunk_markdown_string(content, Some(file_path.as_path()))?;
      let mut chunk_count = 0usize;
      for semantic_chunk in chunks {
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

fn is_markdown_language(language: &str) -> bool {
  matches!(language, "Markdown" | "GitHub Flavored Markdown" | "RMarkdown" | "MDX") || language.ends_with("Markdown")
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{Tokenizer, chunker::memory_async_reader, types::Chunk};
  use futures::StreamExt;

  #[tokio::test]
  async fn chunk_markdown_document() {
    let chunker = MarkdownChunker::new(32, Tokenizer::Characters, 0).unwrap();
    let content = "# Title\n\nParagraph one\n\nParagraph two";
    let reader = memory_async_reader(content.as_bytes().to_vec());
    let mut stream = chunker.chunk(Path::new("README.md"), reader).await;

    let mut seen = Vec::new();
    while let Some(chunk) = stream.next().await {
      let chunk = chunk.expect("chunking should succeed");
      if let Chunk::Text(text_chunk) = chunk {
        seen.push(text_chunk.text);
      }
    }

    assert!(!seen.is_empty(), "expected at least one markdown chunk");
    assert!(seen.iter().any(|c| c.contains("# Title")));
  }
}
