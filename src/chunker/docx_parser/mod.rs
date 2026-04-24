// Vendored from docx-parser v0.1.1 by Erik Vullings
// https://github.com/erikvullings/docx-parser
// License: MIT OR Apache-2.0

mod utils;

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;

use docx_rust::DocxFile;
use docx_rust::app::App;
use docx_rust::core::Core;
use docx_rust::document::BodyContent::{Paragraph, Run, Sdt, SectionProperty, Table, TableCell};
use docx_rust::document::{ParagraphContent, RunContent, TableCellContent, TableRowContent};
use docx_rust::formatting::{NumberFormat, OnOffOnlyType, ParagraphProperty};
use docx_rust::media::MediaType;
use docx_rust::styles::StyleType;
use serde::Serialize;

use utils::{max_lengths_per_column, save_image_to_file, serialize_images, table_row_to_markdown};

fn app_company<'a>(app: &'a App<'a>) -> Option<&'a str> {
  match app {
    App::AppNoApNamespace(app) => app.company.as_deref(),
    App::AppWithApNamespace(app) => app.company.as_deref(),
  }
}

fn core_title<'a>(core: &'a Core<'a>) -> Option<&'a str> {
  match core {
    Core::CoreNamespace(core) => core.title.as_deref(),
    Core::CoreNoNamespace(core) => core.title.as_deref(),
  }
}

fn core_subject<'a>(core: &'a Core<'a>) -> Option<&'a str> {
  match core {
    Core::CoreNamespace(core) => core.subject.as_deref(),
    Core::CoreNoNamespace(core) => core.subject.as_deref(),
  }
}

fn core_keywords<'a>(core: &'a Core<'a>) -> Option<&'a str> {
  match core {
    Core::CoreNamespace(core) => core.keywords.as_deref(),
    Core::CoreNoNamespace(core) => core.keywords.as_deref(),
  }
}

fn core_description<'a>(core: &'a Core<'a>) -> Option<&'a str> {
  match core {
    Core::CoreNamespace(core) => core.description.as_deref(),
    Core::CoreNoNamespace(core) => core.description.as_deref(),
  }
}

fn core_creator<'a>(core: &'a Core<'a>) -> Option<&'a str> {
  match core {
    Core::CoreNamespace(core) => core.creator.as_deref(),
    Core::CoreNoNamespace(core) => core.creator.as_deref(),
  }
}

fn core_last_modified_by<'a>(core: &'a Core<'a>) -> Option<&'a str> {
  match core {
    Core::CoreNamespace(core) => core.last_modified_by.as_deref(),
    Core::CoreNoNamespace(core) => core.last_modified_by.as_deref(),
  }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockStyle {
  pub bold: bool,
  pub italics: bool,
  pub underline: bool,
  pub strike: bool,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub size: Option<isize>,
}

impl BlockStyle {
  pub fn new() -> Self {
    BlockStyle {
      bold: false,
      italics: false,
      underline: false,
      strike: false,
      size: None,
    }
  }

  pub fn combine_with(&mut self, other: &BlockStyle) {
    self.bold = other.bold;
    self.italics = other.italics;
    self.underline = other.underline;
    self.strike = other.strike;
    if let Some(size) = other.size {
      self.size = Some(size);
    }
  }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarkdownNumbering {
  #[serde(skip_serializing_if = "Option::is_none")]
  pub id: Option<isize>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub indent_level: Option<isize>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub format: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub level_text: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ParagraphStyle {
  #[serde(skip_serializing_if = "Option::is_none")]
  pub style_id: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub outline_lvl: Option<isize>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub numbering: Option<MarkdownNumbering>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub page_break_before: Option<bool>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub style: Option<BlockStyle>,
}

impl ParagraphStyle {
  pub fn new() -> Self {
    ParagraphStyle {
      style_id: None,
      outline_lvl: None,
      numbering: None,
      page_break_before: None,
      style: None,
    }
  }

  pub fn combine_with(&mut self, other: &ParagraphStyle) {
    self.style_id = self.style_id.clone().or_else(|| other.style_id.clone());
    self.outline_lvl = self.outline_lvl.or(other.outline_lvl);
    self.page_break_before = self.page_break_before.or(other.page_break_before);
    if self.numbering.is_none() {
      self.numbering = other.numbering.clone()
    }
    if let Some(ref mut style) = self.style {
      if let Some(ref other_style) = other.style {
        style.combine_with(other_style);
      }
    } else {
      self.style = other.style.clone();
    }
  }
}

impl<'a> From<&'a ParagraphProperty<'a>> for ParagraphStyle {
  fn from(paragraph_property: &'a ParagraphProperty) -> Self {
    let mut paragraph_style = ParagraphStyle::new();
    if let Some(style_id) = &paragraph_property.style_id {
      paragraph_style.style_id = Some(style_id.value.to_string());
    }
    if let Some(outline_lvl) = &paragraph_property.outline_lvl {
      paragraph_style.outline_lvl = Some(outline_lvl.value);
    }
    if let Some(page_break_before) = &paragraph_property.page_break_before {
      paragraph_style.page_break_before = page_break_before.value;
    }
    if let Some(numbering) = &paragraph_property.numbering {
      paragraph_style.numbering = Some(MarkdownNumbering {
        id: numbering.id.as_ref().map(|ni| ni.value),
        indent_level: numbering.level.as_ref().map(|level| level.value),
        format: None,
        level_text: None,
      });
    }
    if !paragraph_property.r_pr.is_empty() {
      let mut block_style = BlockStyle::new();
      paragraph_property.r_pr.iter().for_each(|character_property| {
        if let Some(size) = &character_property.size {
          block_style.size = Some(size.value);
        }
        if character_property.bold.is_some() {
          block_style.bold = true;
        }
        if character_property.underline.is_some() {
          block_style.underline = true;
        }
        if character_property.italics.is_some() || character_property.emphasis.is_some() {
          block_style.italics = true;
        }
        if character_property.strike.is_some() || character_property.dstrike.is_some() {
          block_style.strike = true;
        }
      });
      paragraph_style.style = Some(block_style);
    }
    paragraph_style
  }
}

#[derive(Debug, PartialEq, Eq, Serialize)]
#[expect(dead_code)]
pub enum TextType {
  Text,
  Image,
  Link,
  Code,
  Quote,
  List,
  Table,
  Header,
  HorizontalRule,
  BlockQuote,
  CodeBlock,
  HeaderBlock,
  BookmarkLink,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TextBlock {
  pub text_type: TextType,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub style: Option<BlockStyle>,
  pub text: String,
}

impl TextBlock {
  pub fn new(text: String, style: Option<BlockStyle>, text_type: TextType) -> Self {
    TextBlock { style, text, text_type }
  }

  pub fn to_markdown(&self, paragraph_style: &ParagraphStyle) -> String {
    let mut markdown = self.text.clone();

    let mut style = if self.style.is_some() {
      self.style.as_ref().unwrap().clone()
    } else {
      BlockStyle::new()
    };

    if let Some(block_style) = &paragraph_style.style {
      style.combine_with(block_style);
    };

    if style.bold {
      markdown = format!("**{markdown}**");
    }
    if style.italics {
      markdown = format!("*{markdown}*");
    }
    if style.underline {
      markdown = format!("__{markdown}__");
    }
    if style.strike {
      markdown = format!("~~{markdown}~~");
    }
    markdown
  }
}

#[derive(Debug, Serialize)]
pub struct MarkdownParagraph {
  #[serde(skip_serializing_if = "Option::is_none")]
  pub style: Option<ParagraphStyle>,
  pub blocks: Vec<TextBlock>,
}

impl MarkdownParagraph {
  pub fn new() -> Self {
    MarkdownParagraph {
      style: None,
      blocks: vec![],
    }
  }

  pub fn to_markdown(
    &self,
    styles: &HashMap<String, ParagraphStyle>,
    numberings: &mut HashMap<isize, usize>,
    doc: &MarkdownDocument,
  ) -> String {
    let mut markdown = String::new();

    let mut style = if self.style.is_some() {
      self.style.as_ref().unwrap().clone()
    } else {
      ParagraphStyle::default()
    };

    if let Some(style_id) = &style.style_id
      && let Some(doc_style) = styles.get(style_id)
    {
      style.combine_with(doc_style);
    };

    if let Some(outline_lvl) = style.outline_lvl {
      let heading_level = match outline_lvl {
        0 => "# ",
        1 => "## ",
        2 => "### ",
        3 => "#### ",
        4 => "##### ",
        _ => "###### ",
      };
      markdown += heading_level;
    }

    if let Some(numbering) = &style.numbering {
      if let Some(level) = numbering.indent_level
        && level > 0
      {
        markdown += &"    ".repeat(level as usize);
      }
      if let Some(id) = numbering.id {
        let format = match &doc.numberings[&id].format {
          Some(entry) => NumberFormat::from_str(entry).unwrap_or(NumberFormat::Decimal),
          None => NumberFormat::Decimal,
        };
        let count = numberings.entry(id).or_insert(0);
        let numbering_symbol = match format {
          NumberFormat::UpperRoman => format!("{}.", ((*count) as u8 + b'I') as char),
          NumberFormat::LowerRoman => format!("{}.", ((*count) as u8 + b'i') as char),
          NumberFormat::UpperLetter => format!("{}.", ((*count) as u8 + b'A') as char),
          NumberFormat::LowerLetter => format!("{}.", ((*count) as u8 + b'a') as char),
          NumberFormat::Bullet => match &doc.numberings[&id].level_text {
            Some(level_text) if level_text.trim().is_empty() => " ".to_string(),
            _ => "-".to_string(),
          },
          _ => format!("{}.", *count + 1),
        };
        *count += 1;
        markdown += &format!("{numbering_symbol} ");
      }
    }

    for block in &self.blocks {
      markdown += &block.to_markdown(&style);
    }
    markdown
  }

  fn from_paragraph(paragraph: &docx_rust::document::Paragraph, docx: &docx_rust::Docx) -> MarkdownParagraph {
    let mut markdown_paragraph = MarkdownParagraph::new();
    if let Some(paragraph_property) = &paragraph.property {
      let paragraph_style: ParagraphStyle = paragraph_property.into();
      markdown_paragraph.style = Some(paragraph_style);
    }
    for paragraph_content in &paragraph.content {
      match paragraph_content {
        ParagraphContent::Run(run) => {
          let block_style = match &run.property {
            Some(character_property) => {
              let mut block_style = BlockStyle::new();
              if let Some(size) = &character_property.size {
                block_style.size = Some(size.value);
              }
              if character_property.bold.is_some() {
                block_style.bold = true;
              }
              if character_property.underline.is_some() {
                block_style.underline = true;
              }
              if character_property.italics.is_some() || character_property.emphasis.is_some() {
                block_style.italics = true;
              }
              if character_property.strike.is_some() || character_property.dstrike.is_some() {
                block_style.strike = true;
              }
              Some(block_style)
            }
            None => None,
          };

          let is_same_style = |style: &Option<BlockStyle>| style == &block_style;

          for run_content in &run.content {
            match run_content {
              RunContent::Text(text) => {
                let text = text.text.to_string();
                let mut could_extend_text = false;
                if let Some(prev_block) = markdown_paragraph.blocks.last_mut()
                  && is_same_style(&prev_block.style)
                  && prev_block.text_type == TextType::Text
                {
                  prev_block.text.push_str(&text);
                  could_extend_text = true
                };
                if !could_extend_text {
                  let text_block = TextBlock::new(text, block_style.clone(), TextType::Text);
                  markdown_paragraph.blocks.push(text_block);
                }
              }
              RunContent::Drawing(drawing) => {
                if let Some(inline) = &drawing.inline
                  && let Some(graphic) = &inline.graphic
                  && let Some(pic) = graphic.data.children.first()
                {
                  let id = pic.fill.blip.embed.to_string();
                  if let Some(relationships) = &docx.document_rels
                    && let Some(target) = relationships.get_target(&id)
                  {
                    let descr = match &inline.doc_property.descr {
                      Some(descr) => descr.to_string(),
                      None => "".to_string(),
                    };
                    let img_text = format!("![{}](./{})", descr, target);
                    let text_block = TextBlock::new(img_text, None, TextType::Image);
                    markdown_paragraph.blocks.push(text_block);
                  }
                }
              }
              _ => (),
            }
          }
        }
        ParagraphContent::Link(link) => {
          let descr = link.content.as_ref().and_then(|run| run.content.first());
          let target = match &link.anchor {
            Some(anchor) => Some(format!("#{}", anchor)),
            None => match &link.id {
              Some(id) => match &docx.document_rels {
                Some(doc_relationships) => doc_relationships
                  .relationships
                  .iter()
                  .find_map(|r| if r.id == *id { Some(r.target.to_string()) } else { None }),
                None => None,
              },
              None => None,
            },
          };
          if let (Some(RunContent::Text(descr)), Some(target)) = (descr, target) {
            let link = format!("[{}]({})", descr.text, target);
            let text_block = TextBlock::new(link, None, TextType::Link);
            markdown_paragraph.blocks.push(text_block);
          }
        }
        ParagraphContent::BookmarkStart(bookmark_start) => {
          if let Some(name) = &bookmark_start.name {
            let bookmark = format!(r#"<a name="{}"></a>"#, name);
            let text_block = TextBlock::new(bookmark, None, TextType::BookmarkLink);
            markdown_paragraph.blocks.push(text_block);
          }
        }
        _ => (),
      }
    }
    markdown_paragraph
  }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarkdownDocument {
  #[serde(skip_serializing_if = "Option::is_none")]
  pub creator: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub last_editor: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub company: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub title: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub description: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub subject: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub keywords: Option<String>,
  pub content: Vec<MarkdownContent>,
  pub styles: HashMap<String, ParagraphStyle>,
  pub numberings: HashMap<isize, MarkdownNumbering>,
  #[serde(serialize_with = "serialize_images")]
  pub images: HashMap<String, Vec<u8>>,
}

impl MarkdownDocument {
  pub fn new() -> Self {
    MarkdownDocument {
      creator: None,
      last_editor: None,
      company: None,
      title: None,
      description: None,
      subject: None,
      keywords: None,
      content: vec![],
      styles: HashMap::new(),
      numberings: HashMap::new(),
      images: HashMap::new(),
    }
  }

  pub fn from_file<P: AsRef<Path>>(path: P) -> Self {
    let mut markdown_doc = MarkdownDocument::new();

    let docx = match DocxFile::from_file(path) {
      Ok(docx_file) => docx_file,
      Err(err) => {
        panic!("Error processing file: {:?}", err)
      }
    };
    let docx = match docx.parse() {
      Ok(docx) => docx,
      Err(err) => {
        panic!("Exiting: {:?}", err);
      }
    };

    if let Some(app) = &docx.app
      && let Some(company) = app_company(app)
      && !company.is_empty()
    {
      markdown_doc.company = Some(company.to_string());
    }

    if let Some(core) = &docx.core {
      if let Some(title) = core_title(core)
        && !title.is_empty()
      {
        markdown_doc.title = Some(title.to_string());
      }
      if let Some(subject) = core_subject(core)
        && !subject.is_empty()
      {
        markdown_doc.subject = Some(subject.to_string());
      }
      if let Some(keywords) = core_keywords(core)
        && !keywords.is_empty()
      {
        markdown_doc.keywords = Some(keywords.to_string());
      }
      if let Some(description) = core_description(core)
        && !description.is_empty()
      {
        markdown_doc.description = Some(description.to_string());
      }
      if let Some(creator) = core_creator(core)
        && !creator.is_empty()
      {
        markdown_doc.creator = Some(creator.to_string());
      }
      if let Some(last_modified_by) = core_last_modified_by(core)
        && !last_modified_by.is_empty()
      {
        markdown_doc.last_editor = Some(last_modified_by.to_string());
      }
    }

    if let Some(numbering) = &docx.numbering {
      numbering.numberings.iter().for_each(|n| {
        if let Some(id) = n.num_id
          && let Some(details) = numbering.numbering_details(id)
        {
          markdown_doc.numberings.insert(
            id,
            MarkdownNumbering {
              id: Some(id),
              indent_level: None,
              format: details.levels[0].number_format.as_ref().map(|i| i.value.to_string()),
              level_text: details.levels[0]
                .level_text
                .as_ref()
                .and_then(|i| i.value.as_ref().map(|value| value.to_string())),
            },
          );
        }
      })
    }

    for (id, (MediaType::Image, media_data)) in &docx.media {
      markdown_doc.images.insert(id.clone(), media_data.to_vec());
    }

    for style in &docx.styles.styles {
      if let Some(StyleType::Paragraph) = style.ty
        && let Some(paragraph_property) = &style.paragraph
      {
        let paragraph_style: ParagraphStyle = paragraph_property.into();
        markdown_doc.styles.insert(style.style_id.to_string(), paragraph_style);
      }
    }

    for content in &docx.document.body.content {
      match content {
        Paragraph(paragraph) => {
          let markdown_paragraph = MarkdownParagraph::from_paragraph(paragraph, &docx);
          if !markdown_paragraph.blocks.is_empty() {
            markdown_doc
              .content
              .push(MarkdownContent::Paragraph(markdown_paragraph));
          }
        }
        Table(table) => {
          let rows_columns: MarkdownTable = table
            .rows
            .iter()
            .map(|row| {
              let is_header = match &row.property.table_header {
                Some(table_header) => match table_header.value {
                  Some(OnOffOnlyType::On) => true,
                  _ => false,
                },
                None => false,
              };
              let cells: Vec<Vec<MarkdownParagraph>> = row
                .cells
                .iter()
                .filter_map(|row_content| match row_content {
                  TableRowContent::TableCell(cell) => {
                    let cells: Vec<MarkdownParagraph> = cell
                      .content
                      .iter()
                      .filter_map(|content| match content {
                        TableCellContent::Paragraph(paragraph) => {
                          Some(MarkdownParagraph::from_paragraph(paragraph, &docx))
                        }
                      })
                      .collect();
                    if !cells.is_empty() { Some(cells) } else { None }
                  }
                  _ => None,
                })
                .collect();
              MarkdownTableRow { is_header, cells }
            })
            .collect();

          markdown_doc.content.push(MarkdownContent::Table(rows_columns));
        }
        Sdt(_) => {}
        SectionProperty(_sp) => {}
        Run(_) => {}
        TableCell(tc) => {
          tracing::debug!("TableCell: {:?}", tc);
        }
      }
    }

    markdown_doc
  }

  pub fn to_markdown(&self, export_images: bool) -> String {
    let mut markdown = String::new();

    if let Some(title) = &self.title {
      markdown += &format!("# {}\n\n", title);
    }

    let mut numberings: HashMap<isize, usize> = HashMap::new();

    for (index, content) in self.content.iter().enumerate() {
      match content {
        MarkdownContent::Paragraph(paragraph) => {
          markdown += &paragraph.to_markdown(&self.styles, &mut numberings, self);
          markdown += "\n";
        }
        MarkdownContent::Table(table) => {
          let table_with_simple_cells: Vec<(bool, Vec<String>)> = table
            .iter()
            .map(|MarkdownTableRow { is_header, cells }| {
              let row_content: Vec<String> = cells
                .iter()
                .map(|cell| {
                  cell
                    .iter()
                    .enumerate()
                    .fold(String::new(), |mut content, (i, paragraph)| {
                      let paragraph_as_markdown = &paragraph.to_markdown(&self.styles, &mut numberings, self);
                      if i + 1 < cell.len() {
                        content += &format!("{}<br/>", paragraph_as_markdown);
                      } else {
                        content += paragraph_as_markdown;
                      }
                      content
                    })
                })
                .collect();
              (*is_header, row_content)
            })
            .collect();
          let column_lengths = max_lengths_per_column(&table_with_simple_cells, 3);
          let divider = &table_row_to_markdown(
            &column_lengths,
            &column_lengths.iter().map(|i| "-".repeat(*i)).collect(),
          );
          let table =
            &table_with_simple_cells
              .iter()
              .enumerate()
              .fold(String::new(), |mut acc, (i, (is_header, row))| {
                let markdown_row = &table_row_to_markdown(&column_lengths, row);
                if i == 0 {
                  if *is_header {
                    acc.push_str(markdown_row);
                    acc.push_str(divider);
                  } else {
                    acc.push_str(&table_row_to_markdown(
                      &column_lengths,
                      &column_lengths.iter().map(|_| String::new()).collect(),
                    ));
                    acc.push_str(divider);
                    acc.push_str(markdown_row);
                  }
                } else {
                  acc.push_str(markdown_row);
                }
                if i == table_with_simple_cells.len() {
                  acc.push('\n');
                }
                acc
              });
          markdown += table;
        }
      };
      if index != self.content.len() - 1 {
        markdown += "\n";
      }
    }

    if export_images {
      for (image, data) in &self.images {
        match save_image_to_file(image, data) {
          Ok(_) => (),
          Err(err) => tracing::error!("{err}"),
        };
      }
    }

    markdown
  }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum MarkdownContent {
  Paragraph(MarkdownParagraph),
  Table(MarkdownTable),
}

pub type MarkdownTable = Vec<MarkdownTableRow>;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarkdownTableRow {
  is_header: bool,
  cells: Vec<MarkdownTableCell>,
}

pub type MarkdownTableCell = Vec<MarkdownParagraph>;
