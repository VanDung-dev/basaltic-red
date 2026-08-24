# Changelog Maintenance Rules

When updating or maintaining project changelogs ([docs/en/changelog.md](/docs/en/changelog.md) & [docs/vi/changelog.md](/docs/vi/changelog.md)):

## Allowed sources (exclusive)

- `src/**` — the only source of feature/change facts:
  - `src/lib.rs` → registered modules & exported classes
  - `src/pyapi/*.rs` → public Python commands, signatures, return types
  - `src/engine/*.rs`, `src/engine/formats/**` → engine capabilities
  - `src/error.rs` → error taxonomy changes
- `Cargo.toml` — the only source of version numbers, dependency additions/removals/upgrades, crate-type, and bench targets.

Never pull changelog content from README, docs prose, notebooks (`demo.ipynb`), benchmark output, or commit messages. If a fact is not visible in `src/` or `Cargo.toml`, it does not go into the changelog.

## Procedure

The changelog carries a single standing `## Unreleased` section; no per-version headings.

1. **Structure**: exactly two collapsed admonitions — `??? note "Improvements (N)"` and `??? warning "Fix (M)"` — where N/M are the total bullet counts inside each block.
2. **Date nesting**: inside each admonition, group entries as `* YYYY-MM-DD` sub-lists (newest date first).
3. **Component labels**: every bullet opens with a bold component tag — `**Engine**:`, **PyAPI**:, `**Formats**:`, `**SQL**:`, `**Ingest**:`, **Tools**: ... Tightly-related same-day commits may merge into one labeled bullet.
4. **Sources**: commit history (`commit.txt`) supplies the timeline; every fact must remain verifiable in the current `src/` or `Cargo.toml`. Never pull content from README, docs prose, notebooks, benchmark output, or commit messages alone.
5. **Classification**: capability additions and performance wins → Improvements; robustness/error-handling/cache/GIL corrections → Fix. Skip chore/refactor/docs/ci/test-only commits unless user-visible.
6. **Scope filter**: keep only changes still reflected in the current `src/` tree (plus `Cargo.toml`). Drop entries whose code no longer exists (e.g. removed subsystems) and anything outside `src/` — demo notebooks, `tools/`, `tests/`, `benches/`, CI, license/docs files.
7. **Keep both languages in sync**: EN file first, then mirror to VI with identical structure, ordering, counts, and dates.
8. **Roll-up**: on release day, replace `Unreleased` with the released version heading and start a fresh empty `## Unreleased`.

## Format reference

```markdown
## Unreleased

??? note "Improvements (2)"

    * 2026-08-24

        * **Engine**: capability description...
        * **PyAPI**: capability description...

    * 2026-08-23

        * **Formats**: capability description...

??? warning "Fix (1)"

    * 2026-08-24

        * **Engine**: correction description...
```
