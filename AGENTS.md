# Agent Coding Guidelines

> **Important:** Prefer the `mise` tasks for installs, builds, tests, and formatting. Only use raw toolchain commands when no `mise` wrapper exists, and call that out explicitly.
>
> **CRITICAL: Use the Rust LSP for all code navigation!** Do NOT use grep/find/rg or manual file browsing - the Rust LSP provides accurate, fast, type-aware navigation. See the "Code Navigation (Use Rust LSP!)" section for detailed commands.

## Issue Tracking

We use bd (beads) for issue tracking instead of Markdown TODOs or external tools.

### CLI Quick Reference

If you're not using the MCP server, here are the CLI commands:

```bash
# Find ready work (no blockers)
bd ready

# Create new issue
bd create "Issue title" -t bug|feature|task -p 0-4 -d "Description"

# Create with explicit ID (for parallel workers)
bd create "Issue title" --id worker1-100 -p 1

# Create with labels
bd create "Issue title" -t bug -p 1 -l bug,critical

# Create multiple issues from markdown file
bd create -f feature-plan.md

# Update issue status
bd update <id> --status in_progress

# Link discovered work (old way)
bd dep add <discovered-id> <parent-id> --type discovered-from

# Create and link in one command (new way)
bd create "Issue title" -t bug -p 1 --deps discovered-from:<parent-id>

# Label management
bd label add <id> <label>
bd label remove <id> <label>
bd label list <id>
bd label list-all

# Filter issues by label
bd list --label bug,critical

# Complete work
bd close <id> --reason "Done"

# Show dependency tree
bd dep tree <id>

# Get issue details
bd show <id>

# Rename issue prefix (e.g., from 'knowledge-work-' to 'kw-')
bd rename-prefix kw- --dry-run  # Preview changes
bd rename-prefix kw-     # Apply rename

# Restore compacted issue from git history
bd restore <id>  # View full history at time of compaction

# Import with collision detection
bd import -i .beads/issues.jsonl --dry-run             # Preview only
bd import -i .beads/issues.jsonl --resolve-collisions  # Auto-resolve

# Multi-repo management (requires global daemon)
bd repos list                    # List all cached repositories
bd repos ready                   # View ready work across all repos
bd repos ready --group           # Group by repository
bd repos stats                   # Combined statistics
bd repos clear-cache             # Clear repository cache
```

### Workflow

1. **Check for ready work**: Run `bd ready` to see what's unblocked
2. **Claim your task**: `bd update <id> --status in_progress`
3. **Work on it**: Implement, test, document
4. **Discover new work**: If you find bugs or TODOs, create issues:
   - Old way (two commands): `bd create "Found bug in auth" -t bug -p 1 --json` then `bd dep add <new-id> <current-id> --type discovered-from`
   - New way (one command): `bd create "Found bug in auth" -t bug -p 1 --deps discovered-from:<current-id> --json`
5. **Complete**: `bd close <id> --reason "Implemented"`
6. **Export**: Changes auto-sync to `.beads/issues.jsonl` (5-second debounce)

### Issue Types

- `bug` - Something broken that needs fixing
- `feature` - New functionality
- `task` - Work item (tests, docs, refactoring)
- `epic` - Large feature composed of multiple issues
- `chore` - Maintenance work (dependencies, tooling)

### Priorities

- `0` - Critical (security, data loss, broken builds)
- `1` - High (major features, important bugs)
- `2` - Medium (nice-to-have features, minor bugs)
- `3` - Low (polish, optimization)
- `4` - Backlog (future ideas)

### Dependency Types

- `blocks` - Hard dependency (issue X blocks issue Y)
- `related` - Soft relationship (issues are connected)
- `parent-child` - Epic/subtask relationship
- `discovered-from` - Track issues discovered during work

Only `blocks` dependencies affect the ready work queue.

## Build, Test, and Development Commands
Always default to the `mise` tasks below; only run direct toolchain commands if no `mise` wrapper exists and note the deviation.

**For code navigation and understanding, use the Rust LSP!** See the "Code Navigation (Use Rust LSP!)" section above for detailed commands.

- `mise install`: Install pinned Rust, Bun, Wrangler, etc.
- `bun install`: Install JS deps across workspaces.
- `mise build`: Build Rust workspace + type-check Workers.
- `mise build:rust`: Build Rust only.
- `mise test`: All tests (Rust nextest + Workers via bun test).
- `mise test:rust [--package <crate>]`: Rust tests only (use `-- --nocapture` to see output).
- `mise test:rust --package common --lib` for a specific crate test
- `mise test:workers llm-agents [path/to/file.test.ts]`: Worker tests.
- Dev servers:
  - Worker: `bun run --cwd workers/llm-agents dev`
  - Rust server: `cargo run --package slipstreamd`

## Code Navigation (Use Rust LSP!)

**IMPORTANT: Always use the Rust LSP for code navigation!** The Rust LSP should be your primary tool for:
- Finding symbols and definitions
- Navigating to references
- Getting function signatures and documentation
- Understanding code structure
- Finding implementations and usages

**Prefer Rust LSP over:** grep/find/rg, manual file browsing, or any other navigation method!

### Rust LSP Commands Available

Use these `mcp__rust-lsp__*` tools for navigation:

```bash
# Get file structure and symbols
mcp__rust-lsp__outline <file_path>

# Search for symbols across the codebase
mcp__rust-lsp__search <query>

# Find all references to a symbol
mcp__rust-lsp__references <file_path> <line> <character>

# Get detailed info about a symbol at cursor position
mcp__rust-lsp__inspect <file_path> <line> <character>

# Get code completions at a position
mcp__rust-lsp__completion <file_path> <line> <character>

# Rename a symbol across the codebase
mcp__rust-lsp__rename <file_path> <line> <character> <new_name>

# Get diagnostics (errors/warnings) for a file
mcp__rust-lsp__diagnostics <file_path>
```

### Navigation Examples

```bash
# Find all search-related services
mcp__rust-lsp__search "SearchService"

# Explore the main application structure
mcp__rust-lsp__outline "crates/slipstreamd/src/lib.rs"

# Find all references to AppState
mcp__rust-lsp__references "crates/slipstreamd/src/app.rs" 16 1

# Inspect a function to get its documentation
mcp__rust-lsp__inspect "crates/embedding/src/lib.rs" 127 1

# Get completions for method calls
mcp__rust-lsp__completion "crates/slipstreamd/src/routes.rs" 42 20
```

### Why Use Rust LSP?

- **Accurate**: Understands Rust's type system and module resolution
- **Fast**: Instant navigation without scanning files
- **Context-aware**: Knows about imports, traits, generics
- **Complete**: Shows parameters, return types, documentation
- **IDE-quality**: Same experience as modern IDEs

**Remember: When you need to understand or navigate code, reach for the Rust LSP first!**

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

## References
This file combines repository guidelines with specific agent instructions for working with the codebase effectively.

**Final Reminder: When working with this Rust codebase, always reach for the Rust LSP first for navigation, symbol finding, and understanding code structure!** The `mcp__rust-lsp__*` tools provide IDE-quality code analysis that understands Rust's type system, traits, and module resolution.
