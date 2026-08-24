# docs/ — Documentation Workspace

This folder hosts the bilingual user documentation sites plus repository-level engineering references.

```
docs/
├── en/                    # English site source (mkdocs.yml → docs_dir)
│   ├── architecture/      # Component deep-dives (one page per src/ module group)
│   ├── getting-started/   # Install, quickstart, concepts
│   ├── workflows/         # Task-oriented guides
│   ├── how-to/            # Integration & troubleshooting recipes
│   ├── reference/         # API / specs (python-api, rule-syntax, lake-map-spec, audit-codes)
│   ├── glossary.md        # Term → source-file quick map
│   └── other/             # Benchmarks, changelog
├── vi/                    # Vietnamese mirror — identical nav & structure
├── README.md              # This file
├── ARCHITECTURE.md        # System design summary
├── CODEBASE_REFERENCE.md  # File-by-file map of src/
└── DEV_GUIDE.md           # Build, test, lint, benchmark, release workflow
```

## Build the Sites

```bash
uv run zensical build                  # EN → site/
uv run zensical build -f mkdocs.vi.yml # VI → site/vi/
```

Both builds must pass with **no warnings** (`--strict`-clean). Anchors are slugified with diacritics stripped and `đ` dropped — verify VI anchors against rendered HTML when linking to headings.

## Editing Rules

1. `en/` is the source of truth; every change is mirrored to `vi/` in the same commit.
2. All code examples must be executable against the real API (`tests/python/test_engine.py` is ground truth).
3. Changelog updates follow `.agents/rules/01-changelog-rules.md` (sources: `src/` + `Cargo.toml` only).
4. Diagrams use UML notation: `sequenceDiagram` for call/data flows, `stateDiagram-v2` for lifecycles.

Related top-level docs: [../README.md](../README.md) · [ARCHITECTURE.md](ARCHITECTURE.md) · [CODEBASE_REFERENCE.md](CODEBASE_REFERENCE.md) · [DEV_GUIDE.md](DEV_GUIDE.md)
