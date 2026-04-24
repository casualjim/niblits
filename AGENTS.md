# Agent Coding Guidelines

> **Important:** Prefer the `mise` tasks for installs, builds, tests, and formatting. Only use raw toolchain commands when no `mise` wrapper exists, and call that out explicitly.


## Build, Test, and Development Commands
Always default to the `mise` tasks below; only run direct toolchain commands if no `mise` wrapper exists and note the deviation.

- `mise install`: Install pinned Rust, Bun, Wrangler, etc.
- `mise build:debug`: Build Rust
- `mise test`: All tests (Rust nextest + Workers via bun test).

## Code Style & Formatting
- Rust:
  - Use `eyre::Result` for error handling, `thiserror` for domain errors
  - No `unwrap()` or `expect()` in public APIs
  - Async streaming first - avoid `collect()` patterns
  - Imports: Group std/core, external crates, and internal modules separately
  - Formatting: run `mise format`; never invoke `cargo fmt` directly
  - Strict error handling - fail spectacularly, don't swallow errors
- TypeScript:
  - Strict mode with no `any` or `unknown`
  - Bun package manager
  - Double quotes for strings
- General:
  - 2-space indentation (except Python which uses 4)
  - LF line endings with final newline
  - Trim trailing whitespace
  - UTF-8 encoding

## Naming Conventions
- Rust: snake_case for variables/functions, PascalCase for types
- TypeScript: camelCase for variables/functions, PascalCase for types
- Files: snake_case for Rust, camelCase for TypeScript

## Error Handling
- Rust: Use `eyre::Result` for function returns, `thiserror` for domain-specific errors
- TypeScript: Proper error catching and handling without swallowing
- Never ignore errors - propagate or handle explicitly

## Commit Messages
- Write clear, descriptive commit messages in plain English
- Do NOT use conventional commits, semantic commits, or any commit prefixes (no "feat:", "fix:", "refactor:", etc.)
- Focus on WHAT changed and WHY, not the type of change
- First line should be a clear summary (50-72 chars recommended)
- Use the body for detailed explanation if needed
- Reference issue IDs when relevant (e.g., "Closes: slipstream-24")

Good examples:
- "Split search into dedicated Searcher service"
- "Add reranking provider for DeepInfra Qwen3-Reranker"
- "Fix flaky test by increasing tolerance for timing variance"

Bad examples:
- "refactor(embedding): Split search into dedicated Searcher service"
- "feat: add reranking provider"
- "fix: flaky test"
