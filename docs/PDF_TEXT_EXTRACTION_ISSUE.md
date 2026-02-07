# PDF Text Extraction Issue Analysis

## Summary

During the palate 0.2.0 → 0.3.8 upgrade, we discovered a pre-existing issue with PDF text extraction that affects Identity-H font encoding.

## The Problem

**Test:** `chunker::pdf::tests::chunk_real_pdf_fixture`
**PDF:** `fixtures/unicode_professional_demo.pdf`
**Expected:** Text extracts as "Oxidize-PDF Professional Unicode..."
**Actual:** Text extracts as "O x i d i z e - P D F P r o f e s s i o n a l..." (spaces between characters)

## Root Cause

The issue is in **oxidize-pdf** v1.6.11's text extraction algorithm for PDFs using Identity-H font encoding.

### Font Encoding Analysis

The problematic PDF uses:
- `/FontName /Arial` (Arial Unicode MS)
- `/Encoding /Identity-H` (Horizontal identity mapping)
- Type0 font with CIDFontType2

Identity-H encoding maps Unicode code points directly to character IDs, which oxidize-pdf's text extraction interprets as requiring spaces between characters.

### Verification

1. **pdftotext (poppler)**: Extracts correctly ✓
   ```bash
   pdftotext fixtures/unicode_professional_demo.pdf -
   # Output: "Oxidize-PDF Professional Unicode..."
   ```

2. **oxidize-pdf direct extraction**: Produces spaces ✗
   ```rust
   let pages = document.extract_text()?;
   // pages[0].text contains: "O x i d i z e - P D F..."
   ```

3. **Configuration attempt**: The `space_threshold` setting (tried 0.1-1.0) does not fix this issue

## Upstream Issue

**GitHub:** https://github.com/bzsanti/oxidizePdf/issues/116

The oxidize-pdf maintainers acknowledged this as a text positioning threshold issue, but their fix only addressed NUL byte sanitization (\\0\\u{3}), not the core spacing problem with Identity-H fonts.

## Workarounds Attempted

### 1. space_threshold Configuration
```rust
let options = ExtractionOptions {
    space_threshold: 0.8, // Tried 0.1 to 1.0
    ..Default::default()
};
let pages = document.extract_text_with_options(options)?;
```
**Result:** Does not fix Identity-H encoding issue

### 2. Embedded Font PDF Test
Created `fixtures/embedded_font_test.pdf` using Helvetica (WinAnsiEncoding):
**Result:** Works correctly ✓

## Solution

Migrate to **poppler-rs** which uses the industry-standard Poppler library (same backend as `pdftotext`).

## Test Status

- `chunk_embedded_font_pdf_fixture`: **PASSING** (uses standard font encoding)
- `chunk_real_pdf_fixture`: **FAILING** (Identity-H encoding)

The failing test is kept active (not ignored) to track when this issue is resolved.

## Impact

This issue affects:
- PDFs with CJK fonts (Chinese, Japanese, Korean)
- PDFs with Arial Unicode MS
- PDFs using Identity-H or Identity-V encoding
- Any PDF with non-standard font encodings

## Recommendation

Replace oxidize-pdf with poppler-rs for text extraction to handle the full range of PDF font encodings correctly.

## References

- oxidize-pdf issue #116: https://github.com/bzsanti/oxidizePdf/issues/116
- PDF Association corpora: https://github.com/pdf-association/pdf-corpora
- Poppler library: https://poppler.freedesktop.org/
