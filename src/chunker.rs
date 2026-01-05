use std::{path::Path, pin::Pin, sync::Arc};

mod code;
mod docx;
mod html;
mod markdown;
mod pdf;
mod text;

use async_trait::async_trait;
pub use code::CodeChunker;
pub use docx::DocxChunker;
pub use html::HtmlChunker;
pub use markdown::MarkdownChunker;
pub use pdf::PdfChunker;
pub use text::TextChunker;

use text_splitter::ChunkSizer;
use tokio::io::AsyncRead;

use crate::{ChunkerConfig, Tokenizer, languages::*, types::*};

// Concrete chunk sizer enum to avoid trait object issues
#[derive(Clone)]
pub enum ConcreteSizer {
  Characters(text_splitter::Characters),
  Tiktoken(tiktoken_rs::CoreBPE),
  HuggingFace(std::sync::Arc<tokenizers::tokenizer::Tokenizer>),
}

impl ChunkSizer for ConcreteSizer {
  fn size(&self, chunk: &str) -> usize {
    match self {
      ConcreteSizer::Characters(sizer) => sizer.size(chunk),
      ConcreteSizer::Tiktoken(sizer) => sizer.size(chunk),
      ConcreteSizer::HuggingFace(sizer) => sizer.size(chunk),
    }
  }
}

impl TryFrom<Tokenizer> for ConcreteSizer {
  type Error = ChunkError;

  fn try_from(value: Tokenizer) -> Result<Self, Self::Error> {
    match value {
      Tokenizer::Characters => Ok(ConcreteSizer::Characters(text_splitter::Characters)),
      Tokenizer::Tiktoken(encoding) => {
        let tiktoken = match encoding.as_str() {
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
        }
        .map_err(|e| ChunkError::ParseError(format!("Failed to create tiktoken: {}", e)))?;
        Ok(ConcreteSizer::Tiktoken(tiktoken))
      }
      Tokenizer::PreloadedTiktoken(tiktoken) => Ok(ConcreteSizer::Tiktoken(
        std::sync::Arc::try_unwrap(tiktoken).unwrap_or_else(|arc| (*arc).clone()),
      )),
      Tokenizer::HuggingFace(model) => {
        let tokenizer = tokenizers::tokenizer::Tokenizer::from_pretrained(&model, None)
          .map_err(|e| ChunkError::ParseError(format!("Failed to load HF tokenizer: {}", e)))?;
        Ok(ConcreteSizer::HuggingFace(std::sync::Arc::new(tokenizer)))
      }
      Tokenizer::PreloadedHuggingFace(tokenizer) => Ok(ConcreteSizer::HuggingFace(tokenizer)),
    }
  }
}

pub type ChunkStream = Pin<Box<dyn futures::Stream<Item = Result<Chunk, ChunkError>> + Send>>;

#[async_trait]
pub trait Chunker: Send + Sync {
  async fn applies(
    &self,
    path: &Path,
    reader: PeekableReader<Box<dyn AsyncRead + Unpin + Send>>,
  ) -> Result<
    PeekableReader<Box<dyn AsyncRead + Unpin + Send>>,
    PeekableReader<Box<dyn AsyncRead + Unpin + Send>>,
  >;

  async fn chunk(&self, path: &Path, reader: Box<dyn AsyncRead + Unpin + Send>) -> ChunkStream;
}

#[derive(Default)]
pub struct ChunkerOverrides {
  /// Replace the default code chunker in the chain.
  pub code_chunker: Option<Box<dyn Chunker>>,
  /// Observer invoked when the default code chunker parses a file.
  /// Ignored when `code_chunker` is provided.
  pub code_parse_observer: Option<Arc<dyn CodeParseObserver>>,
}

#[cfg(test)]
pub fn memory_async_reader(bytes: Vec<u8>) -> Box<dyn AsyncRead + Unpin + Send> {
  Box::new(std::io::Cursor::new(bytes))
}

struct ChunkerChainNode {
  chunker: Box<dyn Chunker>,
  next: Option<Box<ChunkerChainNode>>,
}

impl ChunkerChainNode {
  fn new(chunker: Box<dyn Chunker>) -> Self {
    Self {
      chunker,
      next: None,
    }
  }

  fn prepend(self: Box<Self>, chunker: Box<dyn Chunker>) -> Box<Self> {
    Box::new(Self {
      chunker,
      next: Some(self),
    })
  }
}

fn compute_overlap(config: &ChunkerConfig) -> usize {
  let overlap = ((config.max_chunk_size as f32) * config.overlap_percentage).round() as usize;
  if config.max_chunk_size > 0 {
    overlap.min(config.max_chunk_size.saturating_sub(1))
  } else {
    overlap
  }
}

fn build_chunker_chain(config: &ChunkerConfig) -> Result<Box<ChunkerChainNode>, ChunkError> {
  let overlap = compute_overlap(config);

  let chain = Box::new(ChunkerChainNode::new(Box::new(TextChunker::new(
    config.max_chunk_size,
    config.tokenizer.clone(),
    overlap,
  )?)));
  let chain = chain.prepend(Box::new(CodeChunker::new(
    config.max_chunk_size,
    config.tokenizer.clone(),
    overlap,
  )?));
  let chain = chain.prepend(Box::new(MarkdownChunker::new(
    config.max_chunk_size,
    config.tokenizer.clone(),
    overlap,
  )?));
  let chain = chain.prepend(Box::new(HtmlChunker::new(
    config.max_chunk_size,
    config.tokenizer.clone(),
    overlap,
  )?));
  let chain = chain.prepend(Box::new(DocxChunker::new(
    config.max_chunk_size,
    config.tokenizer.clone(),
    overlap,
  )?));
  let chain = chain.prepend(Box::new(PdfChunker::new(
    config.max_chunk_size,
    config.tokenizer.clone(),
    overlap,
  )?));

  Ok(chain)
}

fn build_chunker_chain_with_overrides(
  config: &ChunkerConfig,
  overrides: ChunkerOverrides,
) -> Result<Box<ChunkerChainNode>, ChunkError> {
  let overlap = compute_overlap(config);
  let code_chunker = match overrides.code_chunker {
    Some(chunker) => chunker,
    None => {
      let chunker = CodeChunker::new(config.max_chunk_size, config.tokenizer.clone(), overlap)?;
      let chunker = match overrides.code_parse_observer {
        Some(observer) => chunker.with_parse_observer(observer),
        None => chunker,
      };
      Box::new(chunker)
    }
  };

  let chain = Box::new(ChunkerChainNode::new(Box::new(TextChunker::new(
    config.max_chunk_size,
    config.tokenizer.clone(),
    overlap,
  )?)));
  let chain = chain.prepend(code_chunker);
  let chain = chain.prepend(Box::new(MarkdownChunker::new(
    config.max_chunk_size,
    config.tokenizer.clone(),
    overlap,
  )?));
  let chain = chain.prepend(Box::new(HtmlChunker::new(
    config.max_chunk_size,
    config.tokenizer.clone(),
    overlap,
  )?));
  let chain = chain.prepend(Box::new(DocxChunker::new(
    config.max_chunk_size,
    config.tokenizer.clone(),
    overlap,
  )?));
  let chain = chain.prepend(Box::new(PdfChunker::new(
    config.max_chunk_size,
    config.tokenizer.clone(),
    overlap,
  )?));

  Ok(chain)
}

async fn select_chunker<'a>(
  chain: Box<ChunkerChainNode>,
  path: &Path,
  mut peekable: PeekableReader<Box<dyn AsyncRead + Unpin + Send>>,
) -> Result<
  (
    Box<dyn Chunker>,
    PeekableReader<Box<dyn AsyncRead + Unpin + Send>>,
  ),
  ChunkError,
> {
  let mut current = chain;
  loop {
    match current.chunker.applies(path, peekable).await {
      Ok(returned) => return Ok((current.chunker, returned)),
      Err(returned) => {
        peekable = returned;
        if let Some(next) = current.next {
          current = next;
        } else {
          return Err(ChunkError::UnsupportedFileType(
            path.to_string_lossy().to_string(),
          ));
        }
      }
    }
  }
}

pub async fn get_chunker<P, R>(
  path: P,
  reader: R,
  config: ChunkerConfig,
) -> Result<(Box<dyn Chunker>, impl AsyncRead + Unpin + Send + 'static), ChunkError>
where
  P: AsRef<Path>,
  R: AsyncRead + Unpin + Send + 'static,
{
  let path = path.as_ref().to_owned();
  let peekable: PeekableReader<Box<dyn AsyncRead + Unpin + Send>> =
    PeekableReader::new(Box::new(reader), 51200);
  let chain = build_chunker_chain(&config)?;
  let (selected_chunker, peekable) = select_chunker(chain, &path, peekable).await?;
  // Turn the peekable back into an AsyncRead that replays buffered bytes first
  let combined = peekable.into_async_read();
  Ok((selected_chunker, combined))
}

pub async fn get_chunker_with_overrides<P, R>(
  path: P,
  reader: R,
  config: ChunkerConfig,
  overrides: ChunkerOverrides,
) -> Result<(Box<dyn Chunker>, impl AsyncRead + Unpin + Send + 'static), ChunkError>
where
  P: AsRef<Path>,
  R: AsyncRead + Unpin + Send + 'static,
{
  let path = path.as_ref().to_owned();
  let peekable: PeekableReader<Box<dyn AsyncRead + Unpin + Send>> =
    PeekableReader::new(Box::new(reader), 51200);
  let chain = build_chunker_chain_with_overrides(&config, overrides)?;
  let (selected_chunker, peekable) = select_chunker(chain, &path, peekable).await?;
  // Turn the peekable back into an AsyncRead that replays buffered bytes first
  let combined = peekable.into_async_read();
  Ok((selected_chunker, combined))
}
