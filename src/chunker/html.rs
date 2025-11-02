use std::path::Path;

use super::{ChunkStream, Chunker, ConcreteSizer, MarkdownChunker};
use crate::{Tokenizer, languages::PeekableReader, types::*};
use async_trait::async_trait;
use htmd::HtmlToMarkdown;
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Clone)]
pub struct HtmlChunker {
  markdown_chunker: MarkdownChunker,
}

impl HtmlChunker {
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
      markdown_chunker: MarkdownChunker::new_with_sizer(max_chunk_size, chunk_overlap, chunk_sizer),
    }
  }
}

#[async_trait]
impl Chunker for HtmlChunker {
  async fn applies(
    &self,
    _file_path: &Path,
    mut reader: PeekableReader<Box<dyn AsyncRead + Unpin + Send>>,
  ) -> Result<
    PeekableReader<Box<dyn AsyncRead + Unpin + Send>>,
    PeekableReader<Box<dyn AsyncRead + Unpin + Send>>,
  > {
    let matches_html = match reader.peek_content(8192).await {
      Ok(content) => infer::get(&content)
        .map(|file_type| file_type.mime_type() == "text/html")
        .unwrap_or(false),
      Err(_) => false,
    };

    if matches_html {
      Ok(reader)
    } else {
      Err(reader)
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

      let html = String::from_utf8_lossy(&data).into_owned();
      let markdown = HtmlToMarkdown::new()
        .convert(&html)
        .map_err(|err| ChunkError::ParseError(format!("Failed to convert HTML to Markdown: {err}")))?;

      let chunks = chunker.markdown_chunker.chunk_markdown_string(markdown)?;
      for semantic_chunk in chunks {
        yield Chunk::Text(semantic_chunk);
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
  async fn chunk_html_converts_to_markdown() {
    let chunker = HtmlChunker::new(64, Tokenizer::Characters, 0).unwrap();
    let html = "<html><body><h1>Heading</h1><p>Paragraph</p></body></html>";
    let reader = memory_async_reader(html.as_bytes().to_vec());
    let mut stream = chunker.chunk(Path::new("page.html"), reader).await;

    let mut chunks = Vec::new();
    while let Some(item) = stream.next().await {
      let chunk = item.expect("chunking HTML should succeed");
      if let Chunk::Text(sc) = chunk {
        chunks.push(sc.text);
      }
    }

    assert!(
      chunks.iter().any(|c| c.contains("Heading")),
      "expected markdown content to include heading"
    );
  }
}
