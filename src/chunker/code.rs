use std::{path::Path, sync::Arc};

use super::{ChunkStream, Chunker, ConcreteSizer};
use crate::{
  Tokenizer,
  languages::{self, *},
  metadata_extractor::{extract_file_symbols, extract_metadata_from_tree},
  types::*,
};
use async_trait::async_trait;
use text_splitter::{ChunkConfig, CodeSplitter};
use tokio::io::{AsyncRead, AsyncReadExt};
use tree_sitter::Parser;

#[derive(Clone)]
pub struct CodeChunker {
  max_chunk_size: usize,
  chunk_overlap: usize,
  chunk_sizer: ConcreteSizer,
}

impl CodeChunker {
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

fn overlap_start_offset(content: &str, offset: usize, overlap: usize) -> usize {
  if overlap == 0 || offset == 0 {
    return offset;
  }

  let mut indices = Vec::new();
  for (index, _) in content[..offset].char_indices() {
    indices.push(index);
  }
  if indices.len() <= overlap {
    0
  } else {
    indices[indices.len() - overlap]
  }
}

#[async_trait]
impl Chunker for CodeChunker {
  async fn applies(
    &self,
    file_path: &Path,
    reader: PeekableReader<Box<dyn AsyncRead + Unpin + Send>>,
  ) -> Result<PeekableReader<Box<dyn AsyncRead + Unpin + Send>>, PeekableReader<Box<dyn AsyncRead + Unpin + Send>>> {
    match languages::detect(file_path, reader).await {
      Ok((detection, peekable)) => {
        // Determine language from detection if supported
        let applies = detection.is_some_and(|d| {
          let language = d.canonical();
          languages::get_language(language).is_some()
        });
        if applies { Ok(peekable) } else { Err(peekable) }
      }
      Err((_, peekable)) => Err(peekable),
    }
  }

  async fn chunk(&self, path: &Path, reader: Box<dyn AsyncRead + Unpin + Send>) -> ChunkStream {
    let chunker = self.clone();
    let path = path.to_path_buf();
    let eof_file_path = path.to_string_lossy().to_string();

    Box::pin(async_stream::try_stream! {
        let peekable = PeekableReader::new(reader, 51200);
        let (detected, peekable) = languages::detect(&path, peekable)
          .await
          .map_err(|(err, _peekable)| err)?;
        let detection = detected
          .ok_or_else(|| ChunkError::UnsupportedLanguage("Unknown".to_string()))?;
        let language_name = detection.canonical().to_string();

        let language_fn = get_language(&language_name)
          .ok_or_else(|| ChunkError::UnsupportedLanguage(language_name.clone()))?;
        let ts_language: tree_sitter::Language = language_fn.into();

        let mut reader: Box<dyn AsyncRead + Unpin + Send> =
          Box::new(peekable.into_async_read());

        let mut data = Vec::new();
        reader.read_to_end(&mut data).await?;
        if data.is_empty() {
            return;
        }

        let content: Arc<str> = Arc::from(String::from_utf8_lossy(&data).into_owned());

        let mut parser = Parser::new();
        parser
            .set_language(&ts_language)
            .map_err(|err| ChunkError::ParseError(format!("Failed to set parser language: {err}")))?;
        let tree = parser
            .parse(content.as_ref(), None)
            .ok_or_else(|| ChunkError::ParseError("Failed to parse content".to_string()))?;
        let tree = Arc::new(tree);
        let file_symbols = extract_file_symbols(tree.as_ref(), content.as_ref());

        let config = ChunkConfig::new(chunker.max_chunk_size)
          .with_sizer(&chunker.chunk_sizer)
          .with_trim(false);
        let splitter = CodeSplitter::new(ts_language, config)
          .map_err(|e| ChunkError::ParseError(format!("Failed to create splitter: {}", e)))?;

        let line_index = LineIndex::new(content.as_ref());
        let mut chunk_count = 0usize;
        for (idx, (offset, chunk_text)) in splitter.chunk_indices(content.as_ref()).enumerate() {
            if chunk_text.trim().is_empty() {
                continue;
            }
            let start_offset = overlap_start_offset(content.as_ref(), offset, chunker.chunk_overlap);
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

            let (start_line, end_line) = line_index.line_numbers(start_offset, end_offset);
            let metadata = match extract_metadata_from_tree(
              tree.as_ref(),
              content.as_ref(),
              start_offset,
              end_offset,
              &language_name,
            ) {
              Ok(mut meta) => {
                if meta.node_name.is_none() {
                  meta.node_name = Some(format!("chunk_{}", idx + 1));
                }
                if meta.parent_context.is_none() {
                  meta.parent_context = Some(path.to_string_lossy().to_string());
                }
                meta
              }
              Err(err) => {
                tracing::warn!(
                  "Failed to extract metadata for {}: {}",
                  path.display(),
                  err
                );
                ChunkMetadata {
                  node_type: "code_chunk".to_string(),
                  node_name: Some(format!("chunk_{}", idx + 1)),
                  language: language_name.clone(),
                  parent_context: Some(path.to_string_lossy().to_string()),
                  scope_path: Vec::new(),
                  definitions: Vec::new(),
                  references: Vec::new(),
                }
              }
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
            yield Chunk::Semantic(semantic_chunk);
        }

        if chunk_count > 0 {
          yield Chunk::EndOfFile {
            file_path: eof_file_path,
            content: None,
            content_hash: None,
            file_metadata: None,
            file_symbols: Some(file_symbols),
            expected_chunks: chunk_count,
          };
        }
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{
    Tokenizer,
    chunker::memory_async_reader,
    types::{Chunk, ChunkError},
  };
  use futures::StreamExt;
  #[tokio::test]
  async fn test_streaming_time_to_first_chunk_code() {
    let chunker = CodeChunker::new(20, Tokenizer::Characters, 0).unwrap();
    let mut content = String::new();
    for i in 0..200 {
      content.push_str(&format!("fn f{}() {{ println!(\"hi\"); }}\n", i));
    }
    let reader = memory_async_reader(content.clone().into_bytes());
    let mut stream = chunker.chunk(Path::new("virtual.rs"), reader).await;

    match stream.next().await {
      Some(Ok(Chunk::Semantic(sc))) => assert!(!sc.text.is_empty()),
      other => panic!("Expected first semantic chunk, got {:?}", other),
    }
  }

  #[tokio::test]
  async fn test_code_chunker_creation() {
    let chunker = CodeChunker::new(1000, Tokenizer::Characters, 0).unwrap();
    assert_eq!(chunker.max_chunk_size, 1000);
  }

  #[tokio::test]
  async fn test_python_class_chunking() {
    let chunker = CodeChunker::new(300, Tokenizer::Characters, 0).unwrap();

    let code = r#"
class Calculator:
    """A simple calculator class"""

    def __init__(self):
        self.memory = 0

    def add(self, a, b):
        """Add two numbers"""
        result = a + b
        self.memory = result
        return result

    def subtract(self, a, b):
        """Subtract b from a"""
        return a - b

    def clear_memory(self):
        """Clear the memory"""
        self.memory = 0
"#;

    let reader = memory_async_reader(code.to_string().into_bytes());
    let mut stream = chunker.chunk(Path::new("calculator.py"), reader).await;

    let mut chunks = Vec::new();
    while let Some(result) = stream.next().await {
      chunks.push(result.expect("Should chunk Python code"));
    }

    assert!(chunks.len() >= 2);

    let first_chunk = &chunks[0];
    match first_chunk {
      Chunk::Semantic(sc) => {
        assert!(sc.text.contains("class Calculator"));
      }
      _ => panic!("Expected semantic chunk"),
    }

    let mut definitions = Vec::new();
    for chunk in &chunks {
      if let Chunk::Semantic(sc) = chunk {
        definitions.extend(sc.metadata.definitions.iter().cloned());
      }
    }

    assert!(
      definitions.iter().any(|name| name == "Calculator"),
      "expected class definition in metadata"
    );
    assert!(
      definitions.iter().any(|name| name == "add"),
      "expected method definition in metadata"
    );
    assert!(
      definitions.iter().any(|name| name == "subtract"),
      "expected method definition in metadata"
    );
  }

  #[tokio::test]
  async fn test_javascript_async_chunking() {
    let chunker = CodeChunker::new(200, Tokenizer::Characters, 0).unwrap();

    let code = r#"
async function fetchUserData(userId) {
    const response = await fetch(`/api/users/${userId}`);
    const data = await response.json();
    return data;
}

class UserManager {
    constructor(apiClient) {
        this.client = apiClient;
        this.cache = new Map();
    }

    async getUser(id) {
        if (this.cache.has(id)) {
            return this.cache.get(id);
        }

        const user = await fetchUserData(id);
        this.cache.set(id, user);
        return user;
    }
}

const manager = new UserManager(apiClient);
const user = await manager.getUser(123);
"#;

    let reader = memory_async_reader(code.to_string().into_bytes());
    let mut stream = chunker.chunk(Path::new("user_manager.js"), reader).await;

    let mut chunks = Vec::new();
    while let Some(result) = stream.next().await {
      chunks.push(result.expect("Should chunk JavaScript code"));
    }

    assert!(chunks.len() >= 3, "Should have multiple chunks");

    let chunk_texts: Vec<&str> = chunks
      .iter()
      .filter_map(|c| match c {
        Chunk::Semantic(sc) => Some(sc.text.as_str()),
        _ => None,
      })
      .collect();

    assert!(chunk_texts.iter().any(|t| t.contains("async function fetchUserData")));
    assert!(chunk_texts.iter().any(|t| t.contains("class UserManager")));
    assert!(chunk_texts.iter().any(|t| t.contains("this.cache.set")));
  }

  #[tokio::test]
  async fn test_rust_impl_chunking() {
    let chunker = CodeChunker::new(250, Tokenizer::Characters, 0).unwrap();

    let code = r#"
use std::collections::HashMap;

pub struct Cache<K, V> {
    storage: HashMap<K, V>,
    capacity: usize,
}

impl<K, V> Cache<K, V>
where
    K: Eq + std::hash::Hash,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            storage: HashMap::with_capacity(capacity),
            capacity,
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.storage.get(key)
    }

    pub fn insert(&mut self, key: K, value: V) {
        if self.storage.len() >= self.capacity {
            if let Some(first_key) = self.storage.keys().next().cloned() {
                self.storage.remove(&first_key);
            }
        }
        self.storage.insert(key, value);
    }
}
"#;

    let reader = memory_async_reader(code.to_string().into_bytes());
    let mut stream = chunker.chunk(Path::new("cache.rs"), reader).await;

    let mut chunks = Vec::new();
    while let Some(result) = stream.next().await {
      chunks.push(result.expect("Should chunk Rust code"));
    }

    let chunk_texts: Vec<&str> = chunks
      .iter()
      .filter_map(|c| match c {
        Chunk::Semantic(sc) => Some(sc.text.as_str()),
        _ => None,
      })
      .collect();

    assert!(chunk_texts.iter().any(|t| t.contains("struct Cache")));
    assert!(chunk_texts.iter().any(|t| t.contains("impl<K, V> Cache<K, V>")));
    assert!(chunk_texts.iter().any(|t| t.contains("pub fn insert")));
  }

  #[tokio::test]
  async fn test_nested_scope_extraction() {
    let chunker = CodeChunker::new(500, Tokenizer::Characters, 0).unwrap();

    let code = r#"
module OuterModule {
    export namespace InnerNamespace {
        export class NestedClass {
            private data: string[];

            constructor() {
                this.data = [];
            }

            public addItem(item: string): void {
                this.data.push(item);
            }

            public getItems(): string[] {
                return [...this.data];
            }
        }

        export function helperFunction(): NestedClass {
            return new NestedClass();
        }
    }
}
"#;

    let reader = memory_async_reader(code.to_string().into_bytes());
    let mut stream = chunker.chunk(Path::new("nested.ts"), reader).await;

    let mut chunks = Vec::new();
    while let Some(result) = stream.next().await {
      chunks.push(result.expect("Should chunk TypeScript code"));
    }

    let chunk_texts: Vec<&str> = chunks
      .iter()
      .filter_map(|c| match c {
        Chunk::Semantic(sc) => Some(sc.text.as_str()),
        _ => None,
      })
      .collect();

    assert!(
      chunk_texts.iter().any(|t| t.contains("class NestedClass")),
      "Should find NestedClass definition"
    );
  }

  #[tokio::test]
  async fn test_chunk_boundaries_preserve_semantics() {
    let chunker = CodeChunker::new(150, Tokenizer::Characters, 0).unwrap();

    let code = r#"
def process_data(items):
    \"\"\"Process a list of items\"\"\"
    results = []

    for item in items:
        # This is a long comment that explains what we're doing
        # It might cause the chunk to split at an interesting boundary
        processed = transform(item)
        validated = validate(processed)

        if validated:
            results.append(validated)
        else:
            log_error(f\"Invalid item: {item}\")

    return results

def transform(item):
    \"\"\"Transform an item\"\"\"
    return item.upper()

def validate(item):
    \"\"\"Validate an item\"\"\"
    return len(item) > 0
"#;

    let reader = memory_async_reader(code.to_string().into_bytes());
    let mut stream = chunker.chunk(Path::new("process.py"), reader).await;

    let mut chunks = Vec::new();
    while let Some(result) = stream.next().await {
      chunks.push(result.expect("Should chunk Python code"));
    }

    let semantic_chunks: Vec<_> = chunks
      .iter()
      .filter(|chunk| matches!(chunk, Chunk::Semantic(_)))
      .collect();

    assert!(semantic_chunks.len() > 1);
  }

  #[tokio::test]
  async fn test_chunk_simple_rust_code() {
    let chunker = CodeChunker::new(100, Tokenizer::Characters, 0).unwrap();

    let code = r#"
fn main() {
    println!(\"Hello, world!\");
}

fn helper() {
    let x = 42;
}
"#;

    let reader = memory_async_reader(code.to_string().into_bytes());
    let mut stream = chunker.chunk(Path::new("main.rs"), reader).await;

    let mut chunks = Vec::new();
    while let Some(result) = stream.next().await {
      chunks.push(result.expect("Chunking should succeed"));
    }

    assert!(!chunks.is_empty());
    assert!(chunks.iter().any(|chunk| match chunk {
      Chunk::Semantic(sc) => sc.text.contains("fn main"),
      _ => false,
    }));
  }

  #[tokio::test]
  async fn test_line_numbers_are_1_based() {
    let chunker = CodeChunker::new(1000, Tokenizer::Characters, 0).unwrap();

    let code = "def hello():\n    print('Hello')\n\ndef world():\n    print('World')";

    let reader = memory_async_reader(code.to_string().into_bytes());
    let mut stream = chunker.chunk(Path::new("line_numbers.py"), reader).await;

    let mut chunks = Vec::new();
    while let Some(result) = stream.next().await {
      chunks.push(result.expect("Should chunk Python code"));
    }

    let semantic_chunks: Vec<_> = chunks
      .into_iter()
      .filter(|chunk| matches!(chunk, Chunk::Semantic(_)))
      .collect();

    assert!(!semantic_chunks.is_empty());
    for chunk in semantic_chunks {
      if let Chunk::Semantic(sc) = chunk {
        assert!(sc.start_byte <= sc.end_byte);
        assert_eq!(sc.text.len(), sc.end_byte - sc.start_byte);
        assert!(sc.start_line >= 1);
        assert!(sc.end_line >= sc.start_line);
        assert_eq!(sc.chunk_hash, *blake3::hash(sc.text.as_bytes()).as_bytes());
      }
    }
  }

  #[tokio::test]
  async fn test_unsupported_language() {
    let chunker = CodeChunker::new(1000, Tokenizer::Characters, 0).unwrap();

    let reader = memory_async_reader("code".to_string().into_bytes());
    let mut stream = chunker.chunk(Path::new("cobol.cbl"), reader).await;

    let result = stream.next().await.unwrap();
    assert!(result.is_err());

    match result {
      Err(ChunkError::UnsupportedLanguage(lang)) => assert_eq!(lang, "cobol"),
      _ => panic!("Expected UnsupportedLanguage error"),
    }
  }

  #[tokio::test]
  async fn test_rust_enum_extraction() {
    let chunker = CodeChunker::new(15, Tokenizer::Characters, 0).unwrap();

    let code = r#"enum Color {
    Red,
    Green,
    Blue,
}

enum Result<T, E> {
    Ok(T),
    Err(E),
}"#;

    let reader = memory_async_reader(code.to_string().into_bytes());
    let mut stream = chunker.chunk(Path::new("enums.rs"), reader).await;

    let mut chunks = Vec::new();
    while let Some(result) = stream.next().await {
      chunks.push(result.expect("Should chunk Rust code"));
    }

    let enum_chunks: Vec<_> = chunks
      .iter()
      .filter_map(|c| match c {
        Chunk::Semantic(sc) if sc.text.contains("enum") => Some(sc),
        _ => None,
      })
      .collect();
    assert!(!enum_chunks.is_empty());

    assert!(chunks.len() > 1);
  }

  #[tokio::test]
  async fn test_eof_contains_file_symbols_outline() {
    let chunker = CodeChunker::new(1000, Tokenizer::Characters, 0).unwrap();
    let code = r#"
struct Foo {
    value: i32,
}

impl Foo {
    fn new() -> Self {
        Self { value: 0 }
    }
}

fn helper() -> i32 {
    42
}
"#;

    let reader = memory_async_reader(code.as_bytes().to_vec());
    let mut stream = chunker.chunk(Path::new("symbols.rs"), reader).await;

    let mut eof_symbols = None;
    while let Some(result) = stream.next().await {
      let chunk = result.expect("chunking should succeed");
      if let Chunk::EndOfFile { file_symbols, .. } = chunk {
        eof_symbols = file_symbols;
      }
    }

    let file_symbols = eof_symbols.expect("EOF should include file symbols");
    assert!(!file_symbols.outline.is_empty(), "expected non-empty outline");

    let mut sorted = file_symbols.outline.clone();
    sorted.sort_by(|a, b| {
      a.start_byte
        .cmp(&b.start_byte)
        .then_with(|| a.end_byte.cmp(&b.end_byte))
        .then_with(|| a.kind.cmp(&b.kind))
        .then_with(|| a.name.as_deref().unwrap_or("").cmp(b.name.as_deref().unwrap_or("")))
    });
    assert_eq!(file_symbols.outline, sorted, "outline should be deterministic");

    assert!(
      file_symbols
        .outline
        .iter()
        .any(|unit| unit.kind == "struct_item" && unit.name.as_deref() == Some("Foo")),
      "expected struct Foo in outline"
    );
    assert!(
      file_symbols
        .outline
        .iter()
        .any(|unit| unit.kind == "impl_item" && unit.name.as_deref() == Some("Foo")),
      "expected impl Foo in outline"
    );
    assert!(
      file_symbols
        .outline
        .iter()
        .any(|unit| unit.kind == "function_item" && unit.name.as_deref() == Some("helper")),
      "expected helper fn in outline"
    );
  }
}
