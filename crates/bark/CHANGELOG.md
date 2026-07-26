# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- A Bark-backed implementation of CDK's `MintPayment` interface, served through
  the CDK payment processor gRPC server.
- Fixed-amount BOLT11 invoice creation, outgoing payment quotes and payments,
  payment status checks, and payment event streaming.
- On-chain receive support that waits for one confirmation and boards deposits
  into Ark.
- On-chain payment quotes and sends that offboard Bark funds, including fee
  estimation and transaction confirmation tracking.
- Durable receive and send state backed by SQLite and redb, including quote
  mappings, restart reconciliation, retry/review states, and event
  deduplication.
- Zero-fee arkoor routing for valid Ark destinations, with Lightning fallback.
- Configuration through an optional `config.toml` file and `BARK_*` and
  `SERVER_*` environment variables.
- Graceful shutdown on `SIGINT` and `SIGTERM`.
- Bark wallet balance logging during startup.
- Development tooling through a Nix flake, GitHub Actions, and a `justfile`.

### Changed

- Updated the CDK integration to `cdk-common` and `cdk-payment-processor`
  `0.17.3`.
- Updated the Bark dependency stack to `0.3.0`.
- Renamed the backend and configuration from Ark to Bark and consolidated the
  implementation in `src/backend.rs`.
- Changed the default network and public Bark/Esplora endpoints from signet to
  mainnet.
- Changed the default gRPC bind address from all interfaces to
  `127.0.0.1`.
- Replaced the committed runtime configuration with
  `config.toml.example`; local `config.toml` files are ignored.
- Rewrote the README around the implemented Bark processor and removed the
  generic payment processor template guide.

### Fixed

- The gRPC server now honors the configured bind address and port.
- Backend environment variables use the `BARK_*` namespace consistently.
- Payment attempts are persisted before external Bark operations and
  reconciled after interruptions, reducing the risk of duplicate outgoing
  payments.
- Completed incoming and outgoing payments are recorded so they are not
  emitted repeatedly after polling or restart.

### Removed

- The placeholder template backend and its unimplemented payment methods.
- Unused HTTP/2 keep-alive and connection-age configuration.
- Runtime wallet and payment database files from the tracked source tree.
