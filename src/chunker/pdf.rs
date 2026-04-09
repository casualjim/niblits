use std::io::Write;
use std::path::Path;

use super::{ChunkStream, Chunker, ConcreteSizer};
use crate::{Tokenizer, languages::PeekableReader, types::*};
use async_trait::async_trait;
use tempfile::NamedTempFile;
use text_splitter::{ChunkConfig, TextSplitter};
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Clone)]
pub struct PdfChunker {
  max_chunk_size: usize,
  chunk_overlap: usize,
  chunk_sizer: ConcreteSizer,
}

impl PdfChunker {
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

#[async_trait]
impl Chunker for PdfChunker {
  async fn applies(
    &self,
    _file_path: &Path,
    mut reader: PeekableReader<Box<dyn AsyncRead + Unpin + Send>>,
  ) -> Result<PeekableReader<Box<dyn AsyncRead + Unpin + Send>>, PeekableReader<Box<dyn AsyncRead + Unpin + Send>>> {
    match reader.peek_content(8192).await {
      Ok(content) if infer::archive::is_pdf(&content) => Ok(reader),
      _ => Err(reader),
    }
  }

  async fn chunk(&self, file_path: &Path, mut reader: Box<dyn AsyncRead + Unpin + Send>) -> ChunkStream {
    let chunker = self.clone();
    let file_path = file_path.to_path_buf();
    let eof_file_path = file_path.to_string_lossy().to_string();
    Box::pin(async_stream::try_stream! {
        // Create temp file and write PDF data to it
        let mut temp_file = NamedTempFile::new()?;
        let mut buffer = vec![0u8; 8192];
        loop {
          let read = reader.read(&mut buffer).await?;
          if read == 0 {
            break;
          }
          temp_file.write_all(&buffer[..read])?;
        }
        temp_file.flush()?;

        // Get the temp file path for pdftotext
        let temp_path = temp_file.path().to_path_buf();

        // Extract values we need before moving into closure
        let max_chunk_size = chunker.max_chunk_size;
        let chunk_overlap = chunker.chunk_overlap;
        let chunk_sizer = chunker.chunk_sizer.clone();

        let extraction = tokio::task::spawn_blocking(move || {
          // Extract text using poppler (system library) - handles Identity-H encoding correctly
          let doc = poppler::PopplerDocument::new_from_file(&temp_path, None)
            .map_err(|err| ChunkError::ParseError(format!("Failed to open PDF: {err}")))?;

          let num_pages = doc.get_n_pages();
          if num_pages == 0 {
            return Ok(Vec::new());
          }

          // Extract text from each page
          let mut pages = Vec::with_capacity(num_pages);
          for page_idx in 0..num_pages {
            if let Some(page) = doc.get_page(page_idx)
              && let Some(text) = page.get_text() {
                pages.push(text.to_string());
              }
          }

          // Combine pages with separators for chunking
          let mut full_text = String::new();

          for (page_idx, page_text) in pages.iter().enumerate() {
            if page_idx > 0 {
              full_text.push_str("\n\n"); // Page separator
            }
            full_text.push_str(page_text);
          }

          // Use text-splitter for chunking
          let config = ChunkConfig::new(max_chunk_size)
            .with_sizer(&chunk_sizer)
            .with_trim(false);
          let splitter = TextSplitter::new(config);

          let chunks: Vec<_> = splitter.chunks(&full_text).collect();

          // Map chunks back to our format with position tracking
          let mut doc_chunks = Vec::with_capacity(chunks.len());
          let mut current_pos = 0usize;

          for (idx, chunk_text) in chunks.iter().enumerate() {
            let chunk_len = chunk_text.len();
            let start_char = current_pos;
            let end_char = current_pos + chunk_len;

            doc_chunks.push(PdfChunk {
              content: chunk_text.to_string(),
              start_char,
              end_char,
              chunk_idx: idx,
            });

            // Apply overlap for next chunk position
            if chunk_overlap > 0 && idx < chunks.len() - 1 {
              // Move back by overlap amount (in characters)
              let overlap_chars = chunk_overlap.min(chunk_len);
              current_pos = end_char - overlap_chars;
            } else {
              current_pos = end_char;
            }
          }

          Ok::<_, ChunkError>(doc_chunks)
        }).await.map_err(|join_err| ChunkError::ParseError(format!("PDF extraction task failed: {join_err}")))??;

        let mut line_cursor = 1usize;
        let mut chunk_count = 0usize;
        for doc_chunk in extraction.into_iter() {
          if doc_chunk.content.trim().is_empty() {
            continue;
          }

          let content = doc_chunk.content;
          let tokens = match &chunker.chunk_sizer {
            ConcreteSizer::HuggingFace(tokenizer) => {
              tokenizer
                .encode(content.as_str(), false)
                .map(|encoding| encoding.get_ids().to_vec())
                .ok()
            }
            ConcreteSizer::Tiktoken(tiktoken) => {
              tiktoken.encode_ordinary(&content).into()
            }
            ConcreteSizer::Characters(_) => None,
          };

          let start_byte = doc_chunk.start_char;
          let end_byte = doc_chunk.end_char;
          let start_line = line_cursor;
          let end_line = start_line + count_line_breaks(&content);
          line_cursor = end_line;

          let metadata = ChunkMetadata {
            node_type: "text_chunk".to_string(),
            node_name: Some(format!("pdf_chunk_{}", doc_chunk.chunk_idx + 1)),
            language: "pdf".to_string(),
            parent_context: Some(file_path.to_string_lossy().to_string()),
            scope_path: Vec::new(),
            definitions: Vec::new(),
            references: Vec::new(),
          };
          let semantic_chunk = SemanticChunk {
            metadata,
            ..SemanticChunk::with_line_numbers(
              content,
              tokens,
              start_byte,
              end_byte,
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

/// Internal struct to hold PDF chunk information
struct PdfChunk {
  content: String,
  start_char: usize,
  end_char: usize,
  chunk_idx: usize,
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
  async fn chunk_embedded_font_pdf_fixture() {
    // Test with a PDF that has embedded fonts (Helvetica)
    // This should always work regardless of system fonts
    let chunker = PdfChunker::new(512, Tokenizer::Characters, 0).unwrap();
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
      .join("fixtures")
      .join("embedded_font_test.pdf");
    let data = tokio::fs::read(&path).await.expect("read embedded font pdf fixture");

    let reader = memory_async_reader(data.clone());
    let peekable = PeekableReader::new(reader, 65536);
    let detected = match chunker.applies(Path::new("fixture.pdf"), peekable).await {
      Ok(peekable) => peekable,
      Err(_) => panic!("expected PDF chunker to accept embedded font fixture"),
    };

    let mut stream = chunker
      .chunk(Path::new("fixture.pdf"), Box::new(detected.into_async_read()))
      .await;

    let mut chunks = Vec::new();
    while let Some(item) = stream.next().await {
      let chunk = item.expect("pdf chunking should succeed");
      if let Chunk::Text(sc) = chunk {
        chunks.push(sc.text);
      }
    }

    let has_keyword = chunks.iter().any(|text| text.contains("Oxidize-PDF"));
    assert!(has_keyword, "PDF chunks should mention Oxidize-PDF; got {:?}", chunks);
  }

  #[tokio::test]
  async fn chunk_real_pdf_fixture() {
    // This test uses a PDF with Identity-H font encoding (Arial Unicode MS)
    // which triggers a known bug in oxidize-pdf that produces spaced characters.
    // This is a real-world issue documented at:
    // https://github.com/bzsanti/oxidizePdf/issues/116
    // The test intentionally fails until the upstream bug is fixed.
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
      .chunk(Path::new("fixture.pdf"), Box::new(detected.into_async_read()))
      .await;

    let mut chunks = Vec::new();
    while let Some(item) = stream.next().await {
      let chunk = item.expect("pdf chunking should succeed");
      if let Chunk::Text(sc) = chunk {
        chunks.push(sc.text);
      }
    }

    let has_keyword = chunks.iter().any(|text| text.contains("Oxidize-PDF"));
    assert!(has_keyword, "PDF chunks should mention Oxidize-PDF; got {:?}", chunks);
  }
}
