# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- A crate-local `Cargo.lock` and locked development commands for reproducible
  standalone builds.
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
- A durable custom `arkoor` payment method for Ark-native destinations, with
  zero-fee quotes, idempotent sends, status reconciliation, and terminal event
  delivery.
- Configuration through an optional `config.toml` file and `BARK_*` and
  `SERVER_*` environment variables.
- Graceful shutdown on `SIGINT` and `SIGTERM`.
- Bark wallet balance logging during startup.
- Development tooling through a Nix flake, GitHub Actions, and a `justfile`.
- Deterministic configuration and payment-state contract tests, plus an
  opt-in, black-box Regtest suite covering wallet lifecycle, Lightning,
  on-chain, arkoor, real gRPC process restarts, and full CDK mint/melt flows.
- Held-HTLC Regtest coverage for concurrent first-attempt deduplication,
  pending Lightning and Cashu melt recovery across restarts, actual movement
  counts, expired receives, process-level wallet locking, insufficient-funds
  balance safety, and unused fee-reserve return.
- A library target so integration tests and downstream tooling can construct
  the Bark backend without embedding the service binary.

### Changed

- Configuration environment variables are now applied through Figment
  providers, and boolean environment values accept only `true` or `false`.
- Updated the CDK integration to `cdk-common` and `cdk-payment-processor`
  `0.17.3`.
- Updated the Bark dependency stack from `0.3.0` to `0.6.1` and adapted the
  backend to Bark's shared on-chain wallet and settled Lightning receive APIs.
- On-chain receives now board only the detected deposit UTXO by building and
  signing its funding transaction locally before passing it to
  `Wallet::board_psbt`.
- Payment event polling now rotates across Lightning and on-chain receives and
  sends (including arkoor), with bounded scans and persisted cursors for large
  state histories. Its interval is configurable with
  `BARK_EVENT_POLL_INTERVAL_MS`.
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
- Pinned the Regtest Cargo harness and Nix service environment to the exact
  upstream Bark commit corresponding to the tested `0.6.1` implementation.
- Kept the Regtest Nix shell runtime-only: it supplies the pinned service
  binaries while reusing the caller's Rust toolchain.

### Fixed

- The Regtest suite now provisions enough VTXO-pool entries for its complete
  set of Lightning receive scenarios, preventing the later Cashu flow from
  exhausting Bark's default pool.
- The payment processor now fails closed without TLS unless plaintext is
  explicitly enabled with `allow_insecure`, warning more strongly when the
  effective bind address is not loopback.
- The black-box Regtest launcher now explicitly enables cleartext for its
  loopback-only gRPC server.
- Lightning melt quotes now use Bark's current Ark fee estimate, payment
  execution fails closed when no fee cap is supplied or the current fee exceeds
  it, and settled `total_spent` uses Bark's actual recorded fee.
- The gRPC server now honors the configured bind address and port.
- The gRPC server now honors `tls_enable`, `tls_cert_path`, and
  `tls_key_path`.
- Backend environment variables use the `BARK_*` namespace consistently.
- Payment attempts are persisted before external Bark operations and
  reconciled after interruptions, reducing the risk of duplicate outgoing
  payments.
- Completed incoming and outgoing payments are recorded so they are not
  emitted repeatedly after polling or restart.
- Lightning receive events are now discovered from persisted quote mappings
  and Bark's settled receive state instead of its pending-receive list, and
  report the settled amount returned by Bark.
- Persisted scan cursors prevent fixed-prefix starvation, including when the
  cursor's previous record is no longer part of a filtered reconciliation set.
- Outgoing-payment reconciliation excludes terminal records and on-chain
  attempts that are not yet due for review from its per-tick budget, isolates
  failures per record, and advances its cursor only after processing the
  selected batch.
- On-chain status checks reconcile only the requested send instead of
  advancing the background reconciliation cursor on every poll.
- Corrupt stored quote ids, outpoints, transaction ids, and payment hashes are
  logged and skipped instead of aborting the whole event-polling pass.
- Concurrent on-chain receive processing and background outgoing
  reconciliation ticks are skipped instead of queued behind work already in
  progress.
- Unsupported `BARK_NETWORK` values now fail during startup instead of silently
  selecting Signet.
- BOLT11 receive responses now report an absolute Unix expiry, and expired
  outgoing invoices are rejected before quoting or payment.
- Replaced the unreachable attempt to parse a BOLT11 invoice as an Ark address
  with the explicit CDK custom `arkoor` method.
- Failed outgoing Lightning payments are now reconciled from Bark's durable
  movement history after Bark removes their payment checkpoint, allowing
  terminal failure events and Cashu proof compensation to complete.
- The Regtest Nix shell now inherits Bark's pinned native runtime library path
  and exposes its matching `bitcoin-cli`, allowing Esplora Electrs and Core
  Lightning to start on Linux CI runners.

### Removed

- The placeholder template backend and its unimplemented payment methods.
- Unused HTTP/2 keep-alive and connection-age configuration.
- Runtime wallet and payment database files from the tracked source tree.
