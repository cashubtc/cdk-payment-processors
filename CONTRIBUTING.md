# Contributing to CDK Payment Processors

Thank you for contributing to CDK Payment Processors. This repository contains
several independent Rust crates that implement CDK's `MintPayment` interface
over gRPC.

## Code of conduct

Be respectful and constructive in all interactions. We aim to create a
welcoming environment for all contributors.

## Reporting issues

Before opening an issue, check whether one already exists. New issues should
include a clear description, reproduction steps and expected behavior where
applicable, and relevant environment details such as the operating system and
Rust version. Identify the affected processor or processors.

## Repository structure

```text
crates/
├── bark/        # Bark-backed payment processor
├── ldk-server/  # LDK Server-backed payment processor
├── spark/       # Spark-backed payment processor
└── template/    # Backend-agnostic starting point for a new processor
```

The crates are intentionally not members of a shared Cargo workspace. Each
crate owns its `Cargo.toml`, `Cargo.lock`, dependencies, configuration,
documentation, development commands, and release history. Do not move crate
dependencies into a root manifest or introduce dependencies between processors
solely to share implementation details.

Keep `crates/template` generic, reusable, and independent of any particular
payment backend.

## Development setup

Install Git, the stable Rust toolchain (including `rustfmt` and Clippy), and
`protoc`. Individual processors may require additional backend services or
system packages; see that crate's README.

Clone the repository and create a branch from `main`:

```bash
git clone https://github.com/YOUR_USERNAME/cdk-payment-processors.git
cd cdk-payment-processors
git switch -c feature/my-improvement
```

The Bark, Spark, and template crates provide Nix development shells. Enter one
from the repository root with, for example:

```bash
nix develop ./crates/bark
```

Each crate also provides a `justfile` for its supported development tasks:

```bash
cd crates/bark
just --list
just ci
```

## Making changes

- Follow standard Rust conventions and the existing code style.
- Prefer explicit error handling over panics.
- Document public items and explain the reason for non-obvious logic.
- Add or update tests for behavioral changes.
- Keep each changed crate's dependencies declared and versioned in its own
  `Cargo.toml`.
- Add an entry under `## [Unreleased]` in each changed crate's `CHANGELOG.md`.
- Update the changed crate's `README.md` when setup, configuration, behavior,
  or usage changes.

Unit tests may live in `#[cfg(test)]` modules; integration tests belong in the
crate's `tests/` directory. Mock external services where practical. The Bark,
LDK Server, and Spark crates also have opt-in regtest suites documented in
their READMEs and `justfile`s.

## Checking changes

Run formatting, linting, and tests for every crate you changed. From the
repository root, check all crates with:

```bash
for manifest in crates/*/Cargo.toml; do
  cargo fmt --manifest-path "$manifest" -- --check
  cargo clippy --locked --manifest-path "$manifest" -- -D warnings
  cargo test --locked --manifest-path "$manifest"
done
```

To check only one processor:

```bash
cargo fmt --manifest-path crates/bark/Cargo.toml -- --check
cargo clippy --locked --manifest-path crates/bark/Cargo.toml -- -D warnings
cargo test --locked --manifest-path crates/bark/Cargo.toml
```

Backend integration tests require extra services and are not included in the
commands above. Run the relevant crate's regtest command when your change
affects its backend interaction or process lifecycle.

## Pull requests

In the pull request description, summarize the change, identify the affected
crates, link related issues, list the checks you ran, and call out breaking
changes. Keep documentation and changelogs in the same pull request as the
code they describe.

Use clear commit messages. Conventional Commit prefixes are encouraged, for
example:

```text
feat(bark): add a payment method
fix(spark): preserve transfer status
docs: clarify monorepo development
chore(template): update dependencies
```

Pull requests must pass CI and be reviewed before merging. Discuss breaking or
cross-crate architectural changes before investing in a large implementation.

## License

By contributing, you agree that your contributions will be dual-licensed under
the MIT License or Apache License 2.0, at the user's option.
