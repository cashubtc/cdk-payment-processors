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

## Security fixes

When a task involves a vulnerability or an uncoordinated security fix, do not
describe the exploit in depth in anything public: PR titles or bodies, commit
messages, review comments, or code comments. Keep the public summary high-level
(state that a security issue was fixed) and leave out reproduction steps, proofs
of concept, root-cause specifics, and attack paths.

Until a fix has been released and disclosure has been coordinated, send the
detailed write-up to the security contact listed under "Reporting a Vulnerability"
in `SECURITY.md`.
