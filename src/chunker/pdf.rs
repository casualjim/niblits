use std::io::{Seek, Write};
use std::path::Path;

use super::{ChunkStream, Chunker, ConcreteSizer};
use crate::{Tokenizer, languages::PeekableReader, types::*};
use async_trait::async_trait;
use oxidize_pdf::ai::DocumentChunker as PdfDocumentChunker;
use oxidize_pdf::parser::{PdfDocument, PdfReader};
use tempfile::tempfile;
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Clone)]
pub struct PdfChunker {
  max_chunk_size: usize,
  chunk_overlap: usize,
  chunk_sizer: ConcreteSizer,
}

impl PdfChunker {
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
impl Chunker for PdfChunker {
  async fn applies(
    &self,
    _file_path: &Path,
    mut reader: PeekableReader<Box<dyn AsyncRead + Unpin + Send>>,
  ) -> Result<
    PeekableReader<Box<dyn AsyncRead + Unpin + Send>>,
    PeekableReader<Box<dyn AsyncRead + Unpin + Send>>,
  > {
    match reader.peek_content(8192).await {
      Ok(content) if infer::archive::is_pdf(&content) => Ok(reader),
      _ => Err(reader),
    }
  }

  async fn chunk(
    &self,
    _file_path: &Path,
    mut reader: Box<dyn AsyncRead + Unpin + Send>,
  ) -> ChunkStream {
    let chunker = self.clone();
    Box::pin(async_stream::try_stream! {
        let mut file = tempfile()?;

        let mut buffer = vec![0u8; 8192];
        loop {
          let read = reader.read(&mut buffer).await?;
          if read == 0 {
            break;
          }
          file.write_all(&buffer[..read])?;
        }

        file.rewind()?;

        let extraction = tokio::task::spawn_blocking(move || {
          let reader = PdfReader::new(file)
            .map_err(|err| ChunkError::ParseError(format!("Failed to parse PDF: {err}")))?;
          let document = PdfDocument::new(reader);

          let pages = document
            .extract_text()
            .map_err(|err| ChunkError::ParseError(format!("Failed to extract PDF text: {err}")))?;

          if pages.is_empty() {
            return Ok(Vec::new());
          }

          let mut page_texts = Vec::with_capacity(pages.len());
          for (index, page) in pages.into_iter().enumerate() {
            page_texts.push((index + 1, page.text));
          }

          let pdf_chunker = PdfDocumentChunker::new(chunker.max_chunk_size, chunker.chunk_overlap);
          let doc_chunks = pdf_chunker
            .chunk_text_with_pages(&page_texts)
            .map_err(|err| ChunkError::ParseError(format!("Failed to chunk PDF text: {err}")))?;

          Ok::<_, ChunkError>(doc_chunks)
        }).await.map_err(|join_err| ChunkError::ParseError(format!("PDF extraction task failed: {join_err}")))??;

        for doc_chunk in extraction {
          if doc_chunk.content.trim().is_empty() {
            continue;
          }

          let tokens = match &chunker.chunk_sizer {
            ConcreteSizer::HuggingFace(tokenizer) => {
              tokenizer
                .encode(doc_chunk.content.as_str(), false)
                .map(|encoding| encoding.get_ids().to_vec())
                .ok()
            }
            ConcreteSizer::Tiktoken(tiktoken) => {
              tiktoken.encode_ordinary(&doc_chunk.content).into()
            }
            ConcreteSizer::Characters(_) => None,
          };

          let start_byte = doc_chunk.metadata.position.start_char;
          let end_byte = doc_chunk.metadata.position.end_char;

          yield Chunk::Text(SemanticChunk {
            text: doc_chunk.content,
            tokens,
            start_byte,
            end_byte,
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
  use std::path::Path;

  fn build_pdf(text: &str) -> Vec<u8> {
    let mut pdf = Vec::new();
    pdf.extend_from_slice(b"%PDF-1.4\n");

    let content_stream = format!("BT\n/F1 24 Tf\n72 720 Td\n({text}) Tj\nET\n");
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
    offsets.push(0); // object 0 placeholder

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

  #[tokio::test]
  async fn test_pdf_chunker_applies() {
    let data = build_pdf("Hello PDF");
    let reader = memory_async_reader(data.clone());
    let peekable = PeekableReader::new(reader, 51200);
    let chunker = PdfChunker::new(200, Tokenizer::Characters, 0).unwrap();

    let result = chunker.applies(Path::new("doc.pdf"), peekable).await;
    assert!(result.is_ok());
  }

  #[tokio::test]
  async fn test_pdf_chunker_produces_text() {
    let data = build_pdf("Hello PDF");
    let reader = memory_async_reader(data);
    let chunker = PdfChunker::new(200, Tokenizer::Characters, 0).unwrap();
    let mut stream = chunker.chunk(Path::new("doc.pdf"), reader).await;

    match stream.next().await {
      Some(Ok(Chunk::Text(chunk))) => {
        assert!(chunk.text.contains("Hello PDF"));
      }
      other => panic!("expected text chunk, got {:?}", other),
    }
  }

  #[tokio::test]
  async fn chunk_real_pdf_fixture() {
    let chunker = PdfChunker::new(512, Tokenizer::Characters, 0).unwrap();
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
      .join("fixtures")
      .join("unicode_professional_demo.pdf");
    let data = tokio::fs::read(&path).await.expect("read pdf fixture");

    let reader = memory_async_reader(data.clone());
    let peekable = PeekableReader::new(reader, 65536);
    let detected = match chunker.applies(Path::new("fixture.pdf"), peekable).await {
      Ok(peekable) => peekable,
      Err(_) => panic!("expected PDF chunker to accept unicode fixture"),
    };

    let mut stream = chunker
      .chunk(
        Path::new("fixture.pdf"),
        Box::new(detected.into_async_read()),
      )
      .await;

    let mut chunks = Vec::new();
    while let Some(item) = stream.next().await {
      let chunk = item.expect("pdf chunking should succeed");
      if let Chunk::Text(sc) = chunk {
        chunks.push(sc.text);
      }
    }

    let has_keyword = chunks.iter().any(|text| {
      let normalized = text.replace('\0', "");
      normalized.contains("Oxidize-PDF")
    });
    assert!(
      has_keyword,
      "PDF chunks should mention Oxidize-PDF; got {:?}",
      chunks
    );
  }
}
