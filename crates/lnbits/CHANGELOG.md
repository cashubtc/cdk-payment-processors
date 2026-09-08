# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Initial standalone LNbits payment processor for CDK 0.18, adapted from the
  former `cdk-lnbits` in-process backend.
- LNbits v1 BOLT11 invoice creation, payment, status lookup, and websocket
  settlement notifications with bounded reconnect backoff.
- TOML and environment configuration, configurable melt fee reserves, an
  mTLS-by-default gRPC server, unit tests, and crate-local development commands.
- An opt-in Docker-backed regtest suite (`--features regtest-tests`,
  `just test-regtest`) using Bitcoin Core and two LND nodes to cover live LNbits
  REST and websocket integration, BOLT11 receive/send flows, status polling,
  and processor restarts.

[Unreleased]: https://github.com/cashubtc/cdk-payment-processors/compare/v0.1.0...HEAD
