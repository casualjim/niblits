// Vendored from docx-parser v0.1.1 by Erik Vullings
// https://github.com/erikvullings/docx-parser
// License: MIT OR Apache-2.0

use std::collections::HashMap;

use base64::prelude::*;
use serde::Serializer;
use serde::ser::SerializeMap;
use std::{
  fs::{File, create_dir_all},
  io::{self, Write},
  path::PathBuf,
};

pub fn max_lengths_per_column(table_with_simple_cells: &Vec<(bool, Vec<String>)>, min_width: usize) -> Vec<usize> {
  if table_with_simple_cells.is_empty() {
    return vec![];
  }
  let num_columns = table_with_simple_cells[0].1.len();
  let mut max_lengths = vec![min_width; num_columns];
  for (_, row) in table_with_simple_cells {
    for (i, cell) in row.iter().enumerate() {
      if i == max_lengths.len() {
        max_lengths.push(0);
      }
      if cell.len() > max_lengths[i] {
        max_lengths[i] = cell.len();
      }
    }
  }
  max_lengths
}

pub fn pad_left(s: &str, width: &usize) -> String {
  if *width <= s.len() {
    return s.to_string();
  }
  let padding = width - s.len();
  let mut padded = s.to_string();
  padded.push_str(&" ".repeat(padding));
  padded
}

pub fn table_row_to_markdown(column_lengths: &[usize], row: &[String]) -> String {
  let mut table_row_in_markdown = String::new();
  column_lengths.iter().enumerate().for_each(|(j, width)| {
    let cell = if j < row.len() { &row[j] } else { "" };
    table_row_in_markdown.push_str(&format!("| {} ", pad_left(cell, width)));
  });
  table_row_in_markdown.push_str("|\n");
  table_row_in_markdown
}

pub fn save_image_to_file(path: &str, image_data: &[u8]) -> io::Result<()> {
  let current_dir = std::env::current_dir()?;
  let full_path = current_dir.join(path);
  if let Some(parent) = full_path.parent() {
    create_dir_all(parent)?;
  }
  let mut file_path = PathBuf::new();
  file_path.push(full_path);
  let mut file = File::create(&file_path)?;
  file.write_all(image_data)?;
  Ok(())
}

fn get_mime_type(filename: &str) -> Option<&'static str> {
  let extension = filename.split('.').next_back()?;
  match extension.to_lowercase().as_str() {
    "png" => Some("image/png"),
    "jpg" | "jpeg" => Some("image/jpeg"),
    "gif" => Some("image/gif"),
    "bmp" => Some("image/bmp"),
    "tiff" => Some("image/tiff"),
    _ => None,
  }
}

pub fn serialize_images<S>(images: &HashMap<String, Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
where
  S: Serializer,
{
  let mut map = serializer.serialize_map(Some(images.len()))?;
  for (key, value) in images {
    let encoded = BASE64_STANDARD.encode(value);
    let prefix = match get_mime_type(key) {
      Some(mime_type) => format!("data:{};base64,", mime_type),
      None => "data:application/octet-stream;base64,".to_string(),
    };
    let base64_string = format!("{}{}", prefix, encoded);
    map.serialize_entry(key, &base64_string)?;
  }
  map.end()
}
