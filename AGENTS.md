# Repository guidelines

## Crate independence

- Keep the crates under `crates/` self-contained.
- Do not centralize their dependencies in the root `Cargo.toml`.
- Declare and version each crate's dependencies in that crate's own
  `Cargo.toml`.

## Changelogs and documentation

- When changing a crate at `crates/X`, add or update an entry under
  `## [Unreleased]` in `crates/X/CHANGELOG.md`.
- If a crate change affects its setup, configuration, behavior, usage, or
  other user-facing documentation, update `crates/X/README.md` in the same
  change.

## Template crate

- `crates/template` is a generic starting point for people creating a new CDK
  payment processor.
- Keep it self-contained, reusable, and backend-agnostic.
