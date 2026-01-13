use std::io::Write;
use std::panic;
use std::path::Path;

use super::{ChunkStream, Chunker, ConcreteSizer, MarkdownChunker};
use crate::{Tokenizer, languages::PeekableReader, types::*};
use async_trait::async_trait;
use docx_parser::MarkdownDocument;
use tempfile::NamedTempFile;
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Clone)]
pub struct DocxChunker {
  markdown_chunker: MarkdownChunker,
}

impl DocxChunker {
  pub fn new(max_chunk_size: usize, tokenizer_type: Tokenizer, chunk_overlap: usize) -> Result<Self, ChunkError> {
    let chunk_sizer = tokenizer_type.try_into()?;
    Ok(Self::new_with_sizer(max_chunk_size, chunk_overlap, chunk_sizer))
  }

  pub fn new_with_sizer(max_chunk_size: usize, chunk_overlap: usize, chunk_sizer: ConcreteSizer) -> Self {
    Self {
      markdown_chunker: MarkdownChunker::new_with_sizer(max_chunk_size, chunk_overlap, chunk_sizer),
    }
  }
}

#[async_trait]
impl Chunker for DocxChunker {
  async fn applies(
    &self,
    _file_path: &Path,
    mut reader: PeekableReader<Box<dyn AsyncRead + Unpin + Send>>,
  ) -> Result<PeekableReader<Box<dyn AsyncRead + Unpin + Send>>, PeekableReader<Box<dyn AsyncRead + Unpin + Send>>> {
    match reader.peek_content(8192).await {
      Ok(content) if infer::is_document(&content) && infer::doc::is_docx(&content) => Ok(reader),
      _ => Err(reader),
    }
  }

  async fn chunk(&self, file_path: &Path, mut reader: Box<dyn AsyncRead + Unpin + Send>) -> ChunkStream {
    let chunker = self.clone();
    let file_path = file_path.to_path_buf();
    let eof_file_path = file_path.to_string_lossy().to_string();
    Box::pin(async_stream::try_stream! {
      let mut temp_file = NamedTempFile::new()
        .map_err(|err| ChunkError::ParseError(format!("Failed to create temp file for DOCX parsing: {err}")))?;

      let mut buffer = vec![0u8; 32768];
      loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
          break;
        }
        temp_file.write_all(&buffer[..read])?;
      }
      temp_file.flush()?;

      let temp_path = temp_file.into_temp_path();
      let markdown = tokio::task::spawn_blocking(move || -> Result<String, ChunkError> {
        let parse_result = panic::catch_unwind(|| MarkdownDocument::from_file(&temp_path));
        match parse_result {
          Ok(document) => Ok(document.to_markdown(false)),
          Err(_) => Err(ChunkError::ParseError(
            "DOCX parser panicked while reading document".to_string(),
          )),
        }
      })
      .await
      .map_err(|err| ChunkError::ParseError(format!("DOCX parsing task failed: {err}")))??;

      let chunks = chunker
        .markdown_chunker
        .chunk_markdown_string(markdown, Some(file_path.as_path()))?;
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

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{Tokenizer, chunker::memory_async_reader, languages::PeekableReader, types::Chunk};
  use futures::StreamExt;
  use std::path::{Path, PathBuf};

  fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures").join(name)
  }

  #[tokio::test]
  async fn chunks_real_docx_fixture() {
    let chunker = DocxChunker::new(128, Tokenizer::Characters, 0).unwrap();
    let path = fixture_path("word_default.docx");
    let data = tokio::fs::read(&path).await.expect("read docx fixture");

    let reader = memory_async_reader(data);
    let peekable = PeekableReader::new(reader, 65536);

    let detected = match chunker.applies(Path::new("fixture.docx"), peekable).await {
      Ok(peekable) => peekable,
      Err(_) => panic!("docx fixture should be detected by DocxChunker"),
    };

    let mut stream = chunker
      .chunk(Path::new("fixture.docx"), Box::new(detected.into_async_read()))
      .await;

    let mut chunks = Vec::new();
    while let Some(item) = stream.next().await {
      let chunk = item.expect("chunking docx fixture succeeds");
      if let Chunk::Text(sc) = chunk {
        chunks.push(sc.text);
      }
    }

    assert!(
      chunks.iter().any(|text| text.contains("Hello")),
      "expected DOCX fixture chunks to contain greeting; got {:?}",
      chunks
    );
  }
}
