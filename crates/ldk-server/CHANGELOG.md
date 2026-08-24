# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- A crate-local `Cargo.lock` for reproducible standalone builds.
- An opt-in regtest integration suite (`--features regtest-tests`,
  `just test-regtest`) that runs the backend and the processor binary against
  two real `ldk-server` daemons on a regtest `bitcoind`, covering BOLT11 and
  BOLT12 receive/send, held-HTLC failure semantics, invoice expiry,
  processor restarts, and a full Cashu mint/melt round trip. The daemon
  binary is taken from `LDK_SERVER_EXE` when set, otherwise it is cloned and
  built from the same upstream revision pinned in `Cargo.toml`.
- A `lib.rs` target exposing the `backend`, `error`, and `settings` modules
  so integration tests can exercise the backend directly. The binary now
  consumes the library instead of declaring the modules itself.

### Changed

- Replaced the `CDK_LDK_*` environment namespace with shared `SERVER_*`,
  `TLS_*`, and `ALLOW_INSECURE` variables plus backend-specific `LDK_*`
  variables. Environment values are now applied through Figment providers,
  and booleans accept only `true` or `false`.

### Fixed

- The payment processor now fails closed without TLS unless plaintext is
  explicitly enabled with `allow_insecure`, warning more strongly when the
  effective bind address is not loopback.
