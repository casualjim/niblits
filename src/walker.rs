use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use blake3::Hasher;
use futures::{Stream, StreamExt};
use ignore::{DirEntry, WalkBuilder, overrides::OverrideBuilder};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::types::LineIndex;
use crate::{Chunk, ChunkError, ChunkerConfig, FileMetadata, ProjectChunk, Tokenizer, chunk_stream};

pub type EntryFilter = Arc<dyn Fn(&DirEntry) -> bool + Send + Sync + 'static>;

/// Default ignore patterns embedded from extra-ignores file
const DEFAULT_IGNORE_PATTERNS: &str = include_str!("../extra-ignores");

/// Get or create the default ignore file path
fn get_default_ignore_file() -> &'static PathBuf {
  static DEFAULT_IGNORE_FILE: OnceLock<PathBuf> = OnceLock::new();

  DEFAULT_IGNORE_FILE.get_or_init(|| {
    // Create a temporary file with our default patterns
    let temp_dir = std::env::temp_dir();
    let ignore_path = temp_dir.join("niblits-default-ignore");

    // Write the default patterns to the file
    if let Err(err) = std::fs::write(&ignore_path, DEFAULT_IGNORE_PATTERNS) {
      error!("Failed to create default ignore file: {}", err);
    }

    ignore_path
  })
}

/// Options for walking a project directory
#[derive(Clone)]
pub struct WalkOptions {
  pub max_chunk_size: usize,
  pub tokenizer: Tokenizer,
  pub overlap_percentage: f32,
  pub max_parallel: usize,
  pub max_file_size: Option<u64>,
  pub large_file_threads: usize,
  pub existing_hashes: std::collections::BTreeMap<PathBuf, [u8; 32]>,
  pub cancel_token: Option<CancellationToken>,
  /// Application-specific ignore filename. Set to None to disable.
  pub custom_ignore_filename: Option<String>,
  /// Custom predicate for entry filtering. Overrides the default decision when provided.
  pub entry_filter: Option<EntryFilter>,
}

impl std::fmt::Debug for WalkOptions {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("WalkOptions")
      .field("max_chunk_size", &self.max_chunk_size)
      .field("tokenizer", &self.tokenizer)
      .field("overlap_percentage", &self.overlap_percentage)
      .field("max_parallel", &self.max_parallel)
      .field("max_file_size", &self.max_file_size)
      .field("large_file_threads", &self.large_file_threads)
      .field("existing_hashes", &self.existing_hashes)
      .field("cancel_token", &self.cancel_token)
      .field("custom_ignore_filename", &self.custom_ignore_filename)
      .field("entry_filter", &self.entry_filter.as_ref().map(|_| "custom"))
      .finish()
  }
}

impl Default for WalkOptions {
  fn default() -> Self {
    let config = ChunkerConfig::default();
    Self {
      max_chunk_size: config.max_chunk_size,
      tokenizer: config.tokenizer,
      overlap_percentage: config.overlap_percentage,
      max_parallel: 4,
      max_file_size: Some(5 * 1024 * 1024), // 5MB default
      large_file_threads: 4,
      existing_hashes: std::collections::BTreeMap::new(),
      cancel_token: None,
      custom_ignore_filename: None,
      entry_filter: None,
    }
  }
}

/// Build an ignore walker with the same project ignore rules used by `walk_project`.
pub fn project_walk_builder(path: impl AsRef<Path>, options: &WalkOptions) -> WalkBuilder {
  let mut builder = WalkBuilder::new(path);

  configure_ignore_rules(
    &mut builder,
    options.max_file_size,
    options.custom_ignore_filename.as_deref(),
  );

  let entry_filter = options.entry_filter.clone();
  builder.filter_entry(move |entry| should_process_entry(entry, entry_filter.as_ref()));

  builder
}

/// Walk a project directory with options
pub fn walk_project(
  path: impl AsRef<Path>,
  options: WalkOptions,
) -> impl Stream<Item = Result<ProjectChunk, ChunkError>> {
  walk_project_inner(path, options)
}

fn walk_project_inner(
  path: impl AsRef<Path>,
  options: WalkOptions,
) -> impl Stream<Item = Result<ProjectChunk, ChunkError>> {
  let path = path.as_ref().to_owned();
  let max_file_size = options.max_file_size;
  let cancel_token = options.cancel_token.clone();

  // Create a channel for streaming results
  let (tx, rx) = mpsc::channel::<Result<ProjectChunk, ChunkError>>(options.max_parallel * 2);

  tokio::spawn(async move {
    // Phase 1: Collect all files with their sizes
    let file_entries = match collect_files_with_sizes(
      &path,
      max_file_size,
      options.custom_ignore_filename.clone(),
      options.entry_filter.clone(),
    )
    .await
    {
      Ok(entries) => entries,
      Err(err) => {
        let _ = tx.send(Err(ChunkError::IoError(err))).await;
        return;
      }
    };

    if let Some(cancel) = &cancel_token
      && cancel.is_cancelled()
    {
      debug!("walk_project cancelled before processing files");
      return;
    }

    if file_entries.is_empty() {
      debug!("No files found to process");
      return;
    }

    info!("Collected {} files for processing", file_entries.len());

    // Find deleted files by comparing existing_hashes with collected files
    if !options.existing_hashes.is_empty() {
      let collected_paths: BTreeSet<PathBuf> = file_entries.iter().map(|(path, _)| path.clone()).collect();
      let tx_clone = tx.clone();
      let existing_hashes = options.existing_hashes.clone();
      let cancel_clone = cancel_token.clone();

      // Emit delete chunks in a separate task to avoid blocking
      tokio::spawn(async move {
        let mut deleted_count = 0usize;

        for (existing_path, _) in existing_hashes {
          if let Some(cancel) = &cancel_clone
            && cancel.is_cancelled()
          {
            debug!("walk_project deletion emission cancelled");
            break;
          }
          if !collected_paths.contains(&existing_path) {
            // This file was in the index but no longer exists
            deleted_count += 1;
            let delete_chunk = ProjectChunk {
              file_path: existing_path.to_string_lossy().to_string(),
              chunk: Chunk::Delete {
                file_path: existing_path.to_string_lossy().to_string(),
              },
              file_size: 0, // File no longer exists
            };
            if tx_clone.send(Ok(delete_chunk)).await.is_err() {
              break;
            }
          }
        }

        if deleted_count > 0 {
          info!("Detected {} deleted files to remove from index", deleted_count);
        }
      });
    }

    // Sort by size (largest first)
    let mut file_entries = file_entries;
    file_entries.sort_by_key(|(_, size)| std::cmp::Reverse(*size));

    // Log size distribution
    let total_size: u64 = file_entries.iter().map(|(_, size)| size).sum();
    let largest_size = file_entries.first().map(|(_, size)| *size).unwrap_or(0);
    let smallest_size = file_entries.last().map(|(_, size)| *size).unwrap_or(0);
    info!(
      "File size distribution: {} files, total: {} MB, largest: {} KB, smallest: {} bytes",
      file_entries.len(),
      total_size / (1024 * 1024),
      largest_size / 1024,
      smallest_size
    );

    // Phase 2: Process files with dual-pool work-stealing
    process_with_dual_pools(
      file_entries,
      ChunkerConfig {
        max_chunk_size: options.max_chunk_size,
        tokenizer: options.tokenizer,
        overlap_percentage: options.overlap_percentage,
      },
      tx,
      options.large_file_threads,
      options.max_parallel,
      Arc::new(options.existing_hashes),
      cancel_token,
    )
    .await;
  });

  ReceiverStream::new(rx)
}

/// Check if an entry should be traversed (for directories) or processed (for files)
fn should_process_entry(entry: &DirEntry, entry_filter: Option<&EntryFilter>) -> bool {
  if let Some(entry_filter) = entry_filter {
    return entry_filter(entry);
  }
  process_supported_files(entry)
}

pub fn process_supported_files(entry: &DirEntry) -> bool {
  // Always traverse directories
  if entry.file_type().is_some_and(|ft| ft.is_dir()) {
    return true;
  }

  // For files, apply our filtering logic
  if !entry.file_type().is_some_and(|ft| ft.is_file()) {
    return false;
  }

  let path = entry.path();

  // Skip empty files
  if let Ok(metadata) = entry.metadata()
    && metadata.len() == 0
  {
    debug!("Skipping empty file: {}", path.display());
    return false;
  }

  // Allow text files and the binary formats we can process (PDF/DOCX).
  if let Ok(Some(file_type)) = infer::get_from_path(path) {
    if file_type.matcher_type() == infer::MatcherType::Text {
      return true;
    }

    let mime = file_type.mime_type();
    if mime == "application/pdf" || mime == "application/vnd.openxmlformats-officedocument.wordprocessingml.document" {
      return true;
    }
    debug!("Skipping binary file: {}", path.display());
    return false;
  }

  // If infer can't determine, check with palate
  // palate::try_detect returns Some if a file type is detected, None otherwise
  if palate::try_detect(path, "").is_none() {
    debug!("Skipping binary file: {}", path.display());
    return false;
  }

  true
}

pub fn process_text_files_only(entry: &DirEntry) -> bool {
  // Always traverse directories
  if entry.file_type().is_some_and(|ft| ft.is_dir()) {
    return true;
  }

  // For files, apply our filtering logic
  if !entry.file_type().is_some_and(|ft| ft.is_file()) {
    return false;
  }

  let path = entry.path();

  // Skip empty files
  if let Ok(metadata) = entry.metadata()
    && metadata.len() == 0
  {
    debug!("Skipping empty file: {}", path.display());
    return false;
  }

  if !is_text_file(path) {
    debug!("Skipping binary file: {}", path.display());
    return false;
  }

  true
}

fn is_text_file(path: &Path) -> bool {
  if let Ok(Some(file_type)) = infer::get_from_path(path) {
    return file_type.matcher_type() == infer::MatcherType::Text;
  }

  // If infer can't determine, check with palate
  // palate::try_detect returns Some(FileType) if it can detect, None otherwise
  // Empty content is used for path-only detection
  palate::try_detect(path, "").is_some()
}

fn configure_ignore_rules(builder: &mut WalkBuilder, max_file_size: Option<u64>, custom_ignore_filename: Option<&str>) {
  // Add our default ignore patterns
  let default_ignore = get_default_ignore_file();
  if default_ignore.exists() {
    builder.add_ignore(default_ignore);
  }

  builder.max_filesize(max_file_size);

  if let Some(custom_ignore_filename) = custom_ignore_filename {
    builder.add_custom_ignore_filename(custom_ignore_filename);
  }
}

/// Decide if a single path would be included by the walker, using the same
/// ignore/VCS pruning semantics. This is intended for file watcher events.
pub fn is_included_path(project_root: impl AsRef<Path>, path: impl AsRef<Path>, max_file_size: Option<u64>) -> bool {
  is_included_path_with_ignore_filename(project_root, path, max_file_size, None, None)
}

/// Decide if a single path would be included by the walker, using the same
/// ignore/VCS pruning semantics and the provided walk options.
fn is_included_path_with_options(
  project_root: impl AsRef<Path>,
  path: impl AsRef<Path>,
  options: &WalkOptions,
) -> bool {
  is_included_path_with_ignore_filename(
    project_root,
    path,
    options.max_file_size,
    options.custom_ignore_filename.as_deref(),
    options.entry_filter.as_ref(),
  )
}

/// Decide if a single path would be ignored by the walker, using the same
/// ignore/VCS pruning semantics and the provided walk options.
pub fn is_ignored_path(project_root: impl AsRef<Path>, path: impl AsRef<Path>, options: &WalkOptions) -> bool {
  !is_included_path_with_ignore_filename(
    project_root,
    path,
    options.max_file_size,
    options.custom_ignore_filename.as_deref(),
    options.entry_filter.as_ref(),
  )
}

fn is_included_path_with_ignore_filename(
  project_root: impl AsRef<Path>,
  path: impl AsRef<Path>,
  max_file_size: Option<u64>,
  custom_ignore_filename: Option<&str>,
  entry_filter: Option<&EntryFilter>,
) -> bool {
  let project_root = project_root.as_ref();
  let path = path.as_ref();

  // If the path is outside the project, exclude it
  if let Ok(abs) = std::fs::canonicalize(path)
    && let Ok(root) = std::fs::canonicalize(project_root)
    && !abs.starts_with(&root)
  {
    return false;
  }

  // Build an override that only includes the specific path, so the walker
  // applies ignore and VCS pruning but we don't traverse the entire tree.
  let mut ob = OverrideBuilder::new(project_root);
  // Use a relative pattern if possible for portability
  let candidate = match path.strip_prefix(project_root) {
    Ok(rel) => rel,
    Err(_) => path,
  };
  // Override expects Unix-style separators in patterns; Path display is fine
  ob.add(&candidate.to_string_lossy()).ok();
  let overrides = match ob.build() {
    Ok(o) => o,
    Err(_) => return false,
  };

  let mut builder = WalkBuilder::new(project_root);

  // Default and custom ignores, matching walk_project
  configure_ignore_rules(&mut builder, max_file_size, custom_ignore_filename);
  builder.overrides(overrides);

  let traversal_entry_filter = entry_filter.cloned();
  builder.filter_entry(move |entry| should_process_entry(entry, traversal_entry_filter.as_ref()));

  // Build the iterator and check whether our file appears
  for ent in builder.build().flatten() {
    let p = ent.path();
    if p == path {
      // Only include files; directory entries are traversal-only.
      if let Some(ft) = ent.file_type() {
        if !ft.is_file() {
          return false;
        }
        return should_process_entry(&ent, entry_filter);
      }
      return false;
    }
  }
  false
}

/// Process a stream of file paths with options
pub fn walk_files<S>(
  files: S,
  project_root: impl AsRef<Path>,
  options: WalkOptions,
) -> impl Stream<Item = Result<ProjectChunk, ChunkError>>
where
  S: Stream<Item = PathBuf> + Send + 'static,
{
  walk_files_inner(files, project_root, options)
}

fn walk_files_inner<S>(
  files: S,
  project_root: impl AsRef<Path>,
  options: WalkOptions,
) -> impl Stream<Item = Result<ProjectChunk, ChunkError>>
where
  S: Stream<Item = PathBuf> + Send + 'static,
{
  let project_root = project_root.as_ref().to_owned();
  let cancel_token = options.cancel_token.clone();
  let existing_hashes = options.existing_hashes.clone();

  // Create a channel for streaming results
  let (tx, rx) = mpsc::channel::<Result<ProjectChunk, ChunkError>>(options.max_parallel * 2);

  tokio::spawn(async move {
    // Phase 1: Collect files from stream with their sizes
    let mut file_entries = Vec::new();
    let mut files = Box::pin(files);

    while let Some(path) = files.next().await {
      if let Some(cancel) = &cancel_token
        && cancel.is_cancelled()
      {
        debug!("walk_files cancelled before file collection complete");
        break;
      }
      // First check if file exists
      match tokio::fs::metadata(&path).await {
        Ok(meta) => {
          // File exists, check if walker would include this file (inherit ignores/VCS pruning)
          if is_included_path_with_options(&project_root, &path, &options) {
            let size = meta.len();
            file_entries.push((path, size));
          } else if existing_hashes.contains_key(&path) {
            let path_str = path.to_string_lossy().to_string();
            let delete_chunk = ProjectChunk {
              file_path: path_str.clone(),
              chunk: Chunk::Delete { file_path: path_str },
              file_size: 0,
            };
            if let Err(send_err) = tx.send(Ok(delete_chunk)).await {
              debug!("Failed to send delete chunk: {}", send_err);
              return;
            }
          }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
          // File doesn't exist - emit a Delete chunk immediately
          let path_str = path.to_string_lossy().to_string();
          let delete_chunk = ProjectChunk {
            file_path: path_str.clone(),
            chunk: Chunk::Delete { file_path: path_str },
            file_size: 0, // File doesn't exist
          };
          if let Err(send_err) = tx.send(Ok(delete_chunk)).await {
            debug!("Failed to send delete chunk: {}", send_err);
            return;
          }
        }
        Err(err) => {
          debug!("Failed to get metadata for {}: {}", path.display(), err);
        }
      }
    }

    if file_entries.is_empty() {
      debug!("No valid files found to process");
      return;
    }

    info!("Collected {} files for processing", file_entries.len());

    // Sort by size (largest first) - same as walk_project
    file_entries.sort_by_key(|(_, size)| std::cmp::Reverse(*size));

    // Log size distribution
    let total_size: u64 = file_entries.iter().map(|(_, size)| size).sum();
    let largest_size = file_entries.first().map(|(_, size)| *size).unwrap_or(0);
    let smallest_size = file_entries.last().map(|(_, size)| *size).unwrap_or(0);
    info!(
      "File size distribution: {} files, total: {} MB, largest: {} KB, smallest: {} bytes",
      file_entries.len(),
      total_size / (1024 * 1024),
      largest_size / 1024,
      smallest_size
    );

    // Phase 2: Process files with dual-pool work-stealing (share implementation)
    process_with_dual_pools(
      file_entries,
      ChunkerConfig {
        max_chunk_size: options.max_chunk_size,
        tokenizer: options.tokenizer,
        overlap_percentage: options.overlap_percentage,
      },
      tx,
      options.large_file_threads,
      options.max_parallel,
      Arc::new(options.existing_hashes),
      cancel_token,
    )
    .await;
  });

  ReceiverStream::new(rx)
}

/// Collect all files with their sizes (without reading content)
async fn collect_files_with_sizes(
  path: &Path,
  max_file_size: Option<u64>,
  custom_ignore_filename: Option<String>,
  entry_filter: Option<EntryFilter>,
) -> Result<Vec<(PathBuf, u64)>, std::io::Error> {
  // Use a dedicated thread for the synchronous walker.
  let path = path.to_owned();
  let (tx, rx) = oneshot::channel();

  std::thread::spawn(move || {
    let mut entries = Vec::new();
    let mut builder = WalkBuilder::new(&path);

    configure_ignore_rules(&mut builder, max_file_size, custom_ignore_filename.as_deref());

    // Filter entries for traversal, then apply the same predicate again before
    // collecting files because filter_entry is primarily a traversal/pruning hook.
    let traversal_entry_filter = entry_filter.clone();
    builder.filter_entry(move |entry| should_process_entry(entry, traversal_entry_filter.as_ref()));

    for entry in builder.build().flatten() {
      if let Some(file_type) = entry.file_type()
        && file_type.is_file()
        && should_process_entry(&entry, entry_filter.as_ref())
        && let Ok(metadata) = entry.metadata()
      {
        let size = metadata.len();
        if size > 0 {
          entries.push((entry.path().to_owned(), size));
        }
      }
    }
    let _ = tx.send(Ok(entries));
  });

  rx.await
    .unwrap_or_else(|_| Err(std::io::Error::other("collector thread failed")))
}

/// Process files using dual-pool work-stealing approach
async fn process_with_dual_pools(
  file_entries: Vec<(PathBuf, u64)>,
  config: ChunkerConfig,
  tx: mpsc::Sender<Result<ProjectChunk, ChunkError>>,
  large_file_threads: usize,
  small_file_threads: usize,
  existing_hashes: Arc<std::collections::BTreeMap<PathBuf, [u8; 32]>>,
  cancel_token: Option<CancellationToken>,
) {
  use std::collections::VecDeque;
  use std::sync::Mutex;

  // Create a deque that allows taking from both ends
  let work_queue = Arc::new(Mutex::new(VecDeque::from(file_entries.clone())));

  // Track total work and completed work
  let total_files = file_entries.len();
  let remaining_work = Arc::new(AtomicUsize::new(total_files));
  let skipped_files = Arc::new(AtomicUsize::new(0));
  let skipped_size = Arc::new(AtomicUsize::new(0));

  info!(
    "Starting dual-pool processing: {} total files, {} large file threads, {} small file threads",
    total_files, large_file_threads, small_file_threads
  );

  // Spawn large file workers (take from front)
  let mut handles = Vec::new();
  for i in 0..large_file_threads {
    let work_queue = work_queue.clone();
    let config = config.clone();
    let tx = tx.clone();
    let remaining = remaining_work.clone();
    let existing_hashes = existing_hashes.clone();
    let skipped_files = skipped_files.clone();
    let skipped_size = skipped_size.clone();
    let cancel_token = cancel_token.clone();

    let handle = tokio::spawn(async move {
      debug!("Large file worker {} started", i);
      let mut processed = 0usize;
      let mut total_size_processed = 0u64;

      loop {
        if let Some(cancel) = &cancel_token
          && cancel.is_cancelled()
        {
          debug!("Large file worker {} cancelled", i);
          break;
        }
        // Take from the front (largest files)
        let work_item = {
          let mut queue = work_queue.lock().unwrap();
          queue.pop_front()
        };

        match work_item {
          Some((path, size)) => {
            debug!(
              "Large file worker {} processing: {} ({} KB)",
              i,
              path.display(),
              size / 1024
            );
            total_size_processed += size;

            let existing_hash = existing_hashes.get(&path).cloned();

            // Check if file will be skipped
            let mut chunk_count = 0usize;
            let mut stream = Box::pin(process_file(&path, size, config.clone(), existing_hash));
            while let Some(result) = stream.next().await {
              chunk_count += 1;
              if tx.send(result).await.is_err() {
                debug!("Large file worker {} exiting: receiver dropped", i);
                return;
              }
            }

            // If no chunks were produced, the file was skipped
            if chunk_count == 0 && existing_hash.is_some() {
              skipped_files.fetch_add(1, Ordering::Relaxed);
              skipped_size.fetch_add(size as usize, Ordering::Relaxed);
            }

            processed += 1;
            let remaining_count = remaining.fetch_sub(1, Ordering::SeqCst) - 1;
            if remaining_count.is_multiple_of(100) && remaining_count > 0 {
              debug!("Progress: {} files remaining", remaining_count);
            }
          }
          None => {
            debug!("Large file worker {} found empty queue, exiting", i);
            break;
          }
        }
      }

      info!(
        "Large file worker {} completed: processed {} files, {} MB total",
        i,
        processed,
        total_size_processed / (1024 * 1024)
      );
    });
    handles.push(handle);
  }

  // Spawn small file workers (take from back)
  for i in 0..small_file_threads {
    let work_queue = work_queue.clone();
    let config = config.clone();
    let tx = tx.clone();
    let remaining = remaining_work.clone();
    let existing_hashes = existing_hashes.clone();
    let skipped_files = skipped_files.clone();
    let skipped_size = skipped_size.clone();
    let cancel_token = cancel_token.clone();

    let handle = tokio::spawn(async move {
      debug!("Small file worker {} started", i);
      let mut processed = 0usize;
      let mut total_size_processed = 0u64;

      loop {
        if let Some(cancel) = &cancel_token
          && cancel.is_cancelled()
        {
          debug!("Small file worker {} cancelled", i);
          break;
        }
        // Take from the back (smallest files)
        let work_item = {
          let mut queue = work_queue.lock().unwrap();
          queue.pop_back()
        };

        match work_item {
          Some((path, size)) => {
            total_size_processed += size;

            let existing_hash = existing_hashes.get(&path).cloned();

            // Check if file will be skipped
            let mut chunk_count = 0usize;
            let mut stream = Box::pin(process_file(&path, size, config.clone(), existing_hash));
            while let Some(result) = stream.next().await {
              chunk_count += 1;
              if tx.send(result).await.is_err() {
                debug!("Small file worker {} exiting: receiver dropped", i);
                return;
              }
            }

            // If no chunks were produced, the file was skipped
            if chunk_count == 0 && existing_hash.is_some() {
              skipped_files.fetch_add(1, Ordering::Relaxed);
              skipped_size.fetch_add(size as usize, Ordering::Relaxed);
            }

            processed += 1;
            let remaining_count = remaining.fetch_sub(1, Ordering::SeqCst) - 1;
            if remaining_count > 0 && (remaining_count.is_multiple_of(100) || remaining_count < 10) {
              debug!("Progress: {} files remaining", remaining_count);
            }
          }
          None => {
            debug!("Small file worker {} found empty queue, exiting", i);
            break;
          }
        }
      }

      info!(
        "Small file worker {} completed: processed {} files, {} KB total",
        i,
        processed,
        total_size_processed / 1024
      );
    });
    handles.push(handle);
  }

  // Wait for all workers to complete
  for handle in handles {
    let _ = handle.await;
  }

  let skipped = skipped_files.load(Ordering::Relaxed);
  let skipped_mb = skipped_size.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0);
  let processed = total_files - skipped;

  info!(
    "File processing complete: {} total files, {} processed, {} skipped ({:.2} MB saved)",
    total_files, processed, skipped, skipped_mb
  );
}

/// Process a single file and yield chunks as a stream
fn process_file<P: AsRef<Path>>(
  path: P,
  file_size: u64,
  config: ChunkerConfig,
  existing_hash: Option<[u8; 32]>,
) -> impl Stream<Item = Result<ProjectChunk, ChunkError>> + Send {
  let path = path.as_ref().to_owned();

  async_stream::try_stream! {
      let path_str = path.to_string_lossy().to_string();

      // Read file bytes once so we can support binary chunkers too.
      let bytes = match tokio::fs::read(&path).await {
          Ok(bytes) => bytes,
          Err(err) => {
              if err.kind() == std::io::ErrorKind::NotFound {
                  // File was deleted between collection and reading
                  debug!("File deleted during processing: {}", path.display());
                  yield ProjectChunk {
                      file_path: path_str.clone(),
                      chunk: Chunk::Delete {
                          file_path: path_str.clone(),
                      },
                      file_size,
                  };
                  return;
              } else {
                  // Propagate other IO errors
                  Err(ChunkError::IoError(err))?;
                  unreachable!();
              }
          }
      };

      if bytes.is_empty() {
          return;
      }

      let sniff_len = bytes.len().min(8192);
      let sniff = &bytes[..sniff_len];
      let inferred_mime = infer::get(sniff).map(|file_type| file_type.mime_type());

      let is_known_binary =
          infer::archive::is_pdf(sniff) || (infer::is_document(sniff) && infer::doc::is_docx(sniff));
      let content = if is_known_binary {
          None
      } else {
          std::str::from_utf8(&bytes).ok().map(str::to_string)
      };

      // Compute hash of the content
      let mut hasher = Hasher::new();
      hasher.update(&bytes);
      let hash = hasher.finalize();
      let mut content_hash = [0u8; 32];
      content_hash.copy_from_slice(hash.as_bytes());
      let content_hash_hex = hash.to_hex().to_string();

      let modified = tokio::fs::metadata(&path).await?.modified()?;
      let line_count = content
          .as_ref()
          .map(|text| LineIndex::new(text).line_count())
          .unwrap_or(0);
      let primary_language = if infer::archive::is_pdf(sniff) {
          "pdf".to_string()
      } else if infer::is_document(sniff) && infer::doc::is_docx(sniff) {
          "docx".to_string()
      } else if inferred_mime.is_some_and(|mime| mime == "text/html") {
          "html".to_string()
      } else {
          let sample_len = bytes.len().min(51200);
          let cursor = std::io::Cursor::new(bytes[..sample_len].to_vec());
          let peekable = crate::languages::PeekableReader::new(cursor, 51200);
          let detection = match crate::languages::detect(&path, peekable).await {
              Ok((detection, _reader)) => detection,
              Err((_err, _reader)) => None,
          };

          match detection {
              Some(file_type) => {
                  let language = file_type.canonical();
                  if language == "markdown" {
                      "markdown".to_string()
                  } else if crate::languages::get_language(language).is_some() {
                      language.to_string()
                  } else {
                      match inferred_mime {
                          Some(mime) if mime != "text/plain" => mime
                              .strip_prefix("text/")
                              .filter(|subtype| !subtype.is_empty())
                              .map(|subtype| subtype.to_string())
                              .unwrap_or_else(|| "text".to_string()),
                          _ => "text".to_string(),
                      }
                  }
              }
              None => match inferred_mime {
                  Some(mime) if mime != "text/plain" => mime
                      .strip_prefix("text/")
                      .filter(|subtype| !subtype.is_empty())
                      .map(|subtype| subtype.to_string())
                      .unwrap_or_else(|| "text".to_string()),
                  _ => "text".to_string(),
              },
          }
      };
      let is_binary = is_known_binary || content.is_none();
      let file_metadata = FileMetadata {
          primary_language,
          size: file_size,
          modified,
          content_hash: content_hash_hex,
          line_count,
          is_binary,
      };

      // Check if file has changed
      if let Some(existing) = existing_hash
          && existing == content_hash
      {
          // File unchanged, skip it entirely
          debug!("Skipping unchanged file: {}", path.display());
          return;
      }

      let content_for_eof = content.clone();
      let reader = std::io::Cursor::new(bytes);
      let mut stream: std::pin::Pin<
        Box<dyn Stream<Item = Result<ProjectChunk, ChunkError>> + Send>,
      > = Box::pin(chunk_stream(&path, reader, config).await);

          while let Some(chunk_result) = stream.next().await {
              let mut project_chunk = match chunk_result {
            Ok(chunk) => chunk,
            Err(ChunkError::UnsupportedFileType(_)) => {
              debug!("Skipping unsupported file type: {}", path.display());
              if existing_hash.is_some() {
                yield ProjectChunk {
                  file_path: path_str.clone(),
                  chunk: Chunk::Delete {
                    file_path: path_str.clone(),
                  },
                  file_size,
                };
              }
              return;
            }
            Err(err) => {
              Err(err)?;
              unreachable!();
            }
          };
          if let Chunk::EndOfFile {
            file_path,
            expected_chunks,
            file_symbols,
            ..
          } = project_chunk.chunk {
              project_chunk.chunk = Chunk::EndOfFile {
                  file_path,
                  content: content_for_eof.clone(),
                  content_hash: Some(content_hash),
                  file_metadata: Some(file_metadata.clone()),
                  file_symbols,
                  expected_chunks,
              };
          }
          project_chunk.file_size = file_size;
          yield project_chunk;
      }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::types::Chunk;
  use std::fs;
  use std::path::PathBuf;
  use tempfile::TempDir;
  use tokio_stream::StreamExt;

  async fn create_test_project() -> TempDir {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    // Create a simple project structure
    fs::create_dir_all(base_path.join("src")).unwrap();
    fs::create_dir_all(base_path.join("tests")).unwrap();
    fs::create_dir_all(base_path.join("docs")).unwrap();
    fs::create_dir_all(base_path.join("scripts")).unwrap();
    fs::create_dir_all(base_path.join(".git")).unwrap();

    // Create a Python module structure
    fs::write(
      base_path.join("src/__init__.py"),
      r#"""Main package for the test project."""

__version__ = "0.1.0"
__author__ = "Test Author"

from .calculator import Calculator
from .utils import factorial

__all__ = ["Calculator", "factorial"]
"#,
    )
    .unwrap();

    fs::write(
      base_path.join("src/calculator.py"),
      r#"""Calculator module with basic arithmetic operations."""

class Calculator:
    """A simple calculator class."""

    def __init__(self):
        self.memory = 0

    def add(self, a, b):
        """Add two numbers."""
        result = a + b
        self.memory = result
        return result

    def subtract(self, a, b):
        """Subtract b from a."""
        result = a - b
        self.memory = result
        return result

    def multiply(self, a, b):
        """Multiply two numbers."""
        result = a * b
        self.memory = result
        return result

    def clear_memory(self):
        """Clear the calculator memory."""
        self.memory = 0
"#,
    )
    .unwrap();

    // Create Python file
    fs::write(
      base_path.join("scripts/test.py"),
      r#"#!/usr/bin/env python3
def factorial(n):
    if n <= 1:
        return 1
    return n * factorial(n - 1)

class Calculator:
    def __init__(self):
        self.value = 0

    def add(self, x):
        self.value += x
        return self.value

if __name__ == "__main__":
    print(f"5! = {factorial(5)}")
"#,
    )
    .unwrap();

    // Create another Python file
    fs::write(
      base_path.join("scripts/data_processor.py"),
      r#"#!/usr/bin/env python3
"""Data processing utilities."""

import json
import csv
from typing import List, Dict, Any

class DataProcessor:
    """Process various data formats."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.data = []

    def load_json(self, filepath: str) -> List[Dict]:
        """Load data from JSON file."""
        with open(filepath, 'r') as f:
            self.data = json.load(f)
        return self.data

    def process_records(self) -> List[Dict]:
        """Process loaded records."""
        processed = []
        for record in self.data:
            if self._validate_record(record):
                processed.append(self._transform_record(record))
        return processed

    def _validate_record(self, record: Dict) -> bool:
        """Validate a single record."""
        required_fields = self.config.get('required_fields', [])
        return all(field in record for field in required_fields)

    def _transform_record(self, record: Dict) -> Dict:
        """Transform a single record."""
        # Apply transformations based on config
        return record
"#,
    )
    .unwrap();

    // Create a markdown file
    fs::write(
      base_path.join("README.md"),
      r#"# Test Project

This is a test project for the walker functionality.

## Features
- Python modules and packages
- Data processing utilities
- Calculator implementations
- Documentation

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```python
from src import Calculator

calc = Calculator()
result = calc.add(5, 3)
print(f"Result: {result}")
```

## Testing

Run tests with pytest:

```bash
python -m pytest tests/
```
"#,
    )
    .unwrap();

    // Create a binary file (should be skipped)
    fs::write(base_path.join("test.bin"), [0u8, 1, 2, 3, 255, 254, 253, 252]).unwrap();

    // Create .gitignore
    fs::write(base_path.join(".gitignore"), "target/\n*.log\n").unwrap();

    // Create custom ignore file for predicate tests
    fs::write(base_path.join(".appignore"), "scripts/\n").unwrap();

    // Create a file in .git (should be ignored)
    fs::write(base_path.join(".git/config"), "[core]\nrepositoryformatversion = 0\n").unwrap();

    // Create a requirements file
    fs::write(
      base_path.join("requirements.txt"),
      "pytest>=7.0.0\nnumpy>=1.20.0\npandas>=1.3.0\n",
    )
    .unwrap();

    temp_dir
  }

  #[tokio::test]
  async fn test_walk_project_basic() {
    let temp_dir = create_test_project().await;
    let path = temp_dir.path();

    let mut chunks = Vec::new();
    let mut stream = walk_project(
      path,
      WalkOptions {
        max_chunk_size: 500,
        tokenizer: Tokenizer::Characters,
        overlap_percentage: 0.0,
        max_parallel: 4,
        max_file_size: None,
        large_file_threads: 2,
        existing_hashes: std::collections::BTreeMap::new(),
        cancel_token: None,
        custom_ignore_filename: None,
        entry_filter: None,
      },
    );

    while let Some(result) = stream.next().await {
      match result {
        Ok(chunk) => chunks.push(chunk),
        Err(err) => panic!("Unexpected error: {}", err),
      }
    }

    // Should have found and chunked multiple files
    assert!(!chunks.is_empty(), "Should have found some chunks");

    // Check we got chunks from different files
    let unique_files: std::collections::HashSet<_> = chunks.iter().map(|c| &c.file_path).collect();
    assert!(unique_files.len() > 1, "Should have chunks from multiple files");

    // Check we have both semantic and text chunks
    let has_semantic = chunks.iter().any(|c| c.is_semantic());
    let has_text = chunks.iter().any(|c| c.is_text());
    assert!(has_semantic, "Should have semantic chunks");
    assert!(has_text, "Should have text chunks");

    // Debug: print all file paths
    let file_paths: Vec<_> = chunks.iter().map(|c| &c.file_path).collect();
    println!("Found files: {:?}", file_paths);

    // Verify .git files were ignored
    assert!(
      !chunks.iter().any(|c| c.file_path.contains(".git")),
      ".git files should be ignored"
    );

    // Verify binary files were skipped
    assert!(
      !chunks.iter().any(|c| c.file_path.contains("test.bin")),
      "Binary files should be skipped"
    );
  }

  #[tokio::test]
  async fn test_is_ignored_path_default_rules() {
    let temp_dir = create_test_project().await;
    let path = temp_dir.path();
    let options = WalkOptions {
      custom_ignore_filename: None,
      ..WalkOptions::default()
    };

    let git_config = path.join(".git/config");
    assert!(is_ignored_path(path, &git_config, &options));

    let src_file = path.join("src/calculator.py");
    assert!(!is_ignored_path(path, &src_file, &options));
  }

  #[tokio::test]
  async fn test_walk_project_entry_filter_excludes_files() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();
    fs::create_dir_all(path.join("src")).unwrap();

    let readme = path.join("README.md");
    let rust_file = path.join("src/lib.rs");
    fs::write(&readme, "# Test Project\n\nOnly markdown should be chunked.\n").unwrap();
    fs::write(&rust_file, "pub fn should_not_be_chunked() {}\n").unwrap();

    let entry_filter: EntryFilter = Arc::new(|entry| {
      entry.file_type().is_some_and(|ft| ft.is_dir())
        || entry
          .path()
          .extension()
          .and_then(|extension| extension.to_str())
          .is_some_and(|extension| extension == "md")
    });
    let options = WalkOptions {
      entry_filter: Some(entry_filter),
      ..WalkOptions::default()
    };

    assert!(!is_ignored_path(path, &readme, &options));
    assert!(is_ignored_path(path, &rust_file, &options));

    let mut emitted_paths = BTreeSet::new();
    let mut stream = walk_project(path, options);
    while let Some(result) = stream.next().await {
      emitted_paths.insert(result.unwrap().file_path);
    }

    assert!(
      emitted_paths.iter().any(|file_path| file_path.ends_with("README.md")),
      "expected README.md to be chunked; emitted paths: {emitted_paths:?}"
    );
    assert!(
      !emitted_paths.iter().any(|file_path| file_path.ends_with("src/lib.rs")),
      "expected src/lib.rs to be excluded; emitted paths: {emitted_paths:?}"
    );
  }

  #[tokio::test]
  async fn test_project_walk_builder_uses_project_ignore_rules() {
    let temp_dir = create_test_project().await;
    let path = temp_dir.path();
    let options = WalkOptions {
      custom_ignore_filename: Some(".appignore".to_string()),
      ..WalkOptions::default()
    };

    let files: Vec<_> = project_walk_builder(path, &options)
      .build()
      .flatten()
      .filter(|entry| entry.file_type().is_some_and(|ft| ft.is_file()))
      .map(|entry| entry.path().to_owned())
      .collect();

    assert!(files.iter().any(|file| file.ends_with("src/calculator.py")));
    assert!(!files.iter().any(|file| file.ends_with(".git/config")));
    assert!(!files.iter().any(|file| file.ends_with("scripts/test.py")));
  }

  #[tokio::test]
  async fn test_is_ignored_path_custom_ignore_file() {
    let temp_dir = create_test_project().await;
    let path = temp_dir.path();
    let options = WalkOptions {
      custom_ignore_filename: Some(".appignore".to_string()),
      ..WalkOptions::default()
    };

    let ignored_script = path.join("scripts/test.py");
    assert!(is_ignored_path(path, &ignored_script, &options));

    let src_file = path.join("src/calculator.py");
    assert!(!is_ignored_path(path, &src_file, &options));
  }

  #[tokio::test]
  async fn test_is_ignored_path_custom_entry_filter() {
    let temp_dir = create_test_project().await;
    let path = temp_dir.path();
    let entry_filter: EntryFilter =
      Arc::new(|entry| entry.file_type().is_some_and(|ft| ft.is_dir()) || entry.path().ends_with("test.bin"));
    let options = WalkOptions {
      entry_filter: Some(entry_filter),
      ..WalkOptions::default()
    };

    let test_bin = path.join("test.bin");
    assert!(!is_ignored_path(path, &test_bin, &options));
  }

  #[tokio::test]
  async fn test_is_ignored_path_text_only_filter_excludes_known_binary() {
    let fixtures_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures");
    let options = WalkOptions {
      entry_filter: Some(Arc::new(process_text_files_only)),
      ..WalkOptions::default()
    };

    let pdf_path = fixtures_dir.join("unicode_professional_demo.pdf");
    assert!(is_ignored_path(&fixtures_dir, &pdf_path, &options));

    let docx_path = fixtures_dir.join("word_default.docx");
    assert!(is_ignored_path(&fixtures_dir, &docx_path, &options));
  }

  #[tokio::test]
  async fn test_is_ignored_path_allows_known_binary_formats() {
    let fixtures_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures");
    let options = WalkOptions::default();

    let pdf_path = fixtures_dir.join("unicode_professional_demo.pdf");
    assert!(!is_ignored_path(&fixtures_dir, &pdf_path, &options));

    let docx_path = fixtures_dir.join("word_default.docx");
    assert!(!is_ignored_path(&fixtures_dir, &docx_path, &options));
  }

  #[tokio::test]
  async fn test_walk_project_languages() {
    let temp_dir = create_test_project().await;
    let path = temp_dir.path();

    let mut chunks = Vec::new();
    let mut stream = walk_project(
      path,
      WalkOptions {
        max_chunk_size: 1000,
        tokenizer: Tokenizer::Characters,
        overlap_percentage: 0.0,
        max_parallel: 2,
        max_file_size: None,
        large_file_threads: 2,
        existing_hashes: std::collections::BTreeMap::new(),
        cancel_token: None,
        custom_ignore_filename: None,
        entry_filter: None,
      },
    );

    while let Some(result) = stream.next().await {
      chunks.push(result.unwrap());
    }

    // Check Python files (the only supported language currently)
    let python_chunks: Vec<_> = chunks
      .iter()
      .filter(|c| c.file_path.ends_with(".py") && c.is_semantic())
      .collect();
    assert!(!python_chunks.is_empty(), "Should have Python semantic chunks");

    // Check Markdown files (should be text chunks)
    let md_chunks: Vec<_> = chunks
      .iter()
      .filter(|c| c.file_path.ends_with(".md") && !matches!(c.chunk, Chunk::EndOfFile { .. }))
      .collect();
    assert!(!md_chunks.is_empty(), "Should have Markdown chunks");
    assert!(md_chunks.iter().all(|c| c.is_text()));
  }

  #[tokio::test]
  async fn test_walk_project_empty_dir() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path();

    let mut chunks = Vec::new();
    let mut stream = walk_project(
      path,
      WalkOptions {
        max_chunk_size: 500,
        tokenizer: Tokenizer::Characters,
        overlap_percentage: 0.0,
        max_parallel: 4,
        max_file_size: None,
        large_file_threads: 2,
        existing_hashes: std::collections::BTreeMap::new(),
        cancel_token: None,
        custom_ignore_filename: None,
        entry_filter: None,
      },
    );

    while let Some(result) = stream.next().await {
      chunks.push(result.unwrap());
    }

    assert!(chunks.is_empty(), "Empty directory should yield no chunks");
  }

  #[tokio::test]
  async fn test_walk_project_concurrency() {
    let temp_dir = create_test_project().await;
    let path = temp_dir.path();

    // Test with different concurrency levels
    for max_parallel in [1, 2, 8] {
      let mut chunks = Vec::new();
      let mut stream = walk_project(
        path,
        WalkOptions {
          max_chunk_size: 500,
          tokenizer: Tokenizer::Characters,
          overlap_percentage: 0.0,
          max_parallel,
          max_file_size: None,
          large_file_threads: 2,
          existing_hashes: std::collections::BTreeMap::new(),
          cancel_token: None,
          custom_ignore_filename: None,
          entry_filter: None,
        },
      );

      while let Some(result) = stream.next().await {
        chunks.push(result.unwrap());
      }

      assert!(
        !chunks.is_empty(),
        "Should get chunks with concurrency level {}",
        max_parallel
      );
    }
  }

  #[tokio::test]
  async fn test_process_file_stream() {
    let temp_dir = TempDir::new().unwrap();
    let rust_file = temp_dir.path().join("test.rs");

    let content = r#"
fn main() {
    println!("Test");
}

fn helper() {
    let x = 42;
}
"#;
    fs::write(&rust_file, content).unwrap();

    let file_size = fs::metadata(&rust_file).unwrap().len();
    let mut stream = Box::pin(process_file(&rust_file, file_size, ChunkerConfig::default(), None));

    let mut chunks = Vec::new();
    while let Some(result) = stream.next().await {
      chunks.push(result.unwrap());
    }

    assert!(!chunks.is_empty(), "Should get chunks from Rust file");

    // Filter out EOF chunks for semantic checks
    let semantic_chunks: Vec<_> = chunks
      .iter()
      .filter(|c| !matches!(c.chunk, Chunk::EndOfFile { .. }))
      .collect();

    assert!(!semantic_chunks.is_empty(), "Should have semantic chunks");
    assert!(semantic_chunks.iter().all(|c| c.is_semantic()));

    // Should have exactly one EOF chunk at the end
    assert!(matches!(chunks.last().unwrap().chunk, Chunk::EndOfFile { .. }));

    let eof_chunk = chunks
      .iter()
      .find(|c| matches!(c.chunk, Chunk::EndOfFile { .. }))
      .expect("EOF chunk should be present");
    if let Chunk::EndOfFile {
      content_hash,
      file_metadata,
      ..
    } = &eof_chunk.chunk
    {
      let content_hash = content_hash.expect("EOF content_hash should be set");
      let metadata = file_metadata.as_ref().expect("file metadata should be set");
      assert_eq!(
        metadata.content_hash,
        blake3::hash(content.as_bytes()).to_hex().to_string()
      );
      assert_eq!(metadata.size, file_size);
      assert!(!metadata.is_binary);
      assert!(metadata.line_count >= 1);
      assert_eq!(content_hash, *blake3::hash(content.as_bytes()).as_bytes());
    }
  }

  #[tokio::test]
  async fn test_work_avoidance_hash_comparison() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("test.rs");
    let content = r#"
fn main() {
    println!("Hello, world!");
}
"#;

    fs::write(&test_file, content).unwrap();

    // First, process the file without any existing hashes
    let file_size = fs::metadata(&test_file).unwrap().len();
    let mut stream = Box::pin(process_file(&test_file, file_size, ChunkerConfig::default(), None));

    let mut chunks = Vec::new();
    while let Some(result) = stream.next().await {
      chunks.push(result.unwrap());
    }

    assert!(!chunks.is_empty(), "Should get chunks on first run");

    // Extract the hash from the EOF chunk
    let eof_chunk = chunks
      .iter()
      .find(|c| matches!(c.chunk, Chunk::EndOfFile { .. }))
      .unwrap();
    let content_hash = match &eof_chunk.chunk {
      Chunk::EndOfFile { content_hash, .. } => content_hash.expect("hash should be set"),
      _ => panic!("Expected EOF chunk"),
    };

    // Now process the same file again with the hash
    let file_size = fs::metadata(&test_file).unwrap().len();
    let mut stream = Box::pin(process_file(
      &test_file,
      file_size,
      ChunkerConfig::default(),
      Some(content_hash),
    ));

    let mut chunks = Vec::new();
    while let Some(result) = stream.next().await {
      chunks.push(result.unwrap());
    }

    assert!(chunks.is_empty(), "Should get no chunks when file is unchanged");
  }

  fn fixture_path(name: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
      .join("fixtures")
      .join(name)
  }

  #[tokio::test]
  async fn test_walk_project_pdf_fixture() {
    let temp_dir = TempDir::new().unwrap();
    let source = fixture_path("unicode_professional_demo.pdf");
    let dest = temp_dir.path().join("document.pdf");
    fs::write(&dest, fs::read(source).unwrap()).unwrap();

    let dest_str = dest.to_string_lossy().to_string();
    let mut stream = walk_project(temp_dir.path(), WalkOptions::default());
    let mut saw_chunk = false;
    let mut saw_eof = false;

    while let Some(chunk_result) = stream.next().await {
      let project_chunk = chunk_result.unwrap();
      if project_chunk.file_path != dest_str {
        continue;
      }

      match project_chunk.chunk {
        Chunk::EndOfFile {
          content, file_metadata, ..
        } => {
          saw_eof = true;
          assert!(content.is_none(), "expected no utf-8 content for pdf");
          let metadata = file_metadata.expect("expected file metadata for pdf");
          assert!(metadata.is_binary, "expected pdf metadata to be binary");
        }
        _ => {
          saw_chunk = true;
        }
      }
    }

    assert!(saw_chunk, "expected pdf to produce at least one chunk");
    assert!(saw_eof, "expected pdf to produce EOF chunk");
  }

  #[tokio::test]
  async fn test_walk_project_docx_fixture() {
    let temp_dir = TempDir::new().unwrap();
    let source = fixture_path("word_default.docx");
    let dest = temp_dir.path().join("document.docx");
    fs::write(&dest, fs::read(source).unwrap()).unwrap();

    let dest_str = dest.to_string_lossy().to_string();
    let mut stream = walk_project(temp_dir.path(), WalkOptions::default());
    let mut saw_chunk = false;
    let mut saw_eof = false;

    while let Some(chunk_result) = stream.next().await {
      let project_chunk = chunk_result.unwrap();
      if project_chunk.file_path != dest_str {
        continue;
      }

      match project_chunk.chunk {
        Chunk::EndOfFile {
          content, file_metadata, ..
        } => {
          saw_eof = true;
          assert!(content.is_none(), "expected no utf-8 content for docx");
          let metadata = file_metadata.expect("expected file metadata for docx");
          assert!(metadata.is_binary, "expected docx metadata to be binary");
        }
        _ => {
          saw_chunk = true;
        }
      }
    }

    assert!(saw_chunk, "expected docx to produce at least one chunk");
    assert!(saw_eof, "expected docx to produce EOF chunk");
  }

  #[tokio::test]
  async fn test_walk_project_invalid_utf8_metadata_consistency() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("bad.txt");
    fs::write(&file_path, b"hello\xFFworld").unwrap();

    let file_path_str = file_path.to_string_lossy().to_string();
    let mut stream = walk_project(temp_dir.path(), WalkOptions::default());
    let mut saw_chunk = false;

    while let Some(result) = stream.next().await {
      let project_chunk = result.unwrap();
      if project_chunk.file_path != file_path_str {
        continue;
      }

      match project_chunk.chunk {
        Chunk::Text(_) | Chunk::Semantic(_) | Chunk::EndOfFile { .. } => {
          saw_chunk = true;
        }
        Chunk::Delete { .. } => {}
      }
    }

    assert!(
      !saw_chunk,
      "expected invalid utf-8 file to be treated as binary and skipped"
    );
  }

  #[tokio::test]
  async fn test_walk_files_unsupported_file_emits_delete() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("bad.bin");
    fs::write(&file_path, b"\xFF\x00\x01").unwrap();

    let mut existing_hashes = std::collections::BTreeMap::new();
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"old content");
    let hash = hasher.finalize();
    let mut hash_bytes = [0u8; 32];
    hash_bytes.copy_from_slice(hash.as_bytes());
    existing_hashes.insert(file_path.clone(), hash_bytes);

    let files = tokio_stream::iter(vec![file_path.clone()]);
    let options = WalkOptions {
      existing_hashes,
      ..WalkOptions::default()
    };

    let mut stream = walk_files(files, temp_dir.path(), options);
    let mut saw_delete = false;

    while let Some(result) = stream.next().await {
      let project_chunk = result.unwrap();
      if project_chunk.file_path != file_path.to_string_lossy() {
        continue;
      }
      if matches!(project_chunk.chunk, Chunk::Delete { .. }) {
        saw_delete = true;
        break;
      }
    }

    assert!(saw_delete, "expected delete chunk for unsupported file");
  }
}
