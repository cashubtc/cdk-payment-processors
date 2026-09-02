# CDK Payment Processor for Bark

A standalone gRPC payment processor that lets a CDK mint use a Bark wallet for
Lightning and on-chain Bitcoin payments through Ark.

The service implements CDK's `MintPayment` interface using `bark-wallet` and
runs it with `cdk-payment-processor`.

## Supported payments

- Create and track fixed-amount BOLT11 invoices.
- Quote, pay, and reconcile outgoing BOLT11 payments.
- Quote, send, reconcile, and stream zero-fee Ark-native payments through the
  custom `arkoor` payment method.
- Create on-chain deposit addresses, wait for one confirmation, and board the
  received funds into Ark.
- Quote and send on-chain payments by offboarding Bark funds.
- Stream incoming and outgoing payment events to the connected CDK mint.
- Persist payment intents and reconciliation state across restarts.

Only the `sat` unit is supported. BOLT11 MPP, amountless invoices, and BOLT12
are not supported.

Outgoing Lightning quotes use Bark's current Ark server fee schedule. Payment
requests must include `max_fee_amount`; the processor refuses to start an
uncapped payment or one whose current Bark fee exceeds that limit. Successful
payments report Bark's recorded fee rather than the earlier quote estimate.

## Requirements

- A stable Rust toolchain
- `protoc` (the Protocol Buffers compiler)
- Access to a Bark/Ark server and an Esplora API for the selected Bitcoin
  network

The included Nix flake provides the Rust toolchain and native dependencies:

```bash
nix develop
```

## Configuration

Copy the example configuration and replace its public example mnemonic:

```bash
cp config.toml.example config.toml
```

`config.toml` is optional. Environment variables override file values.

| `config.toml` key | Environment variable | Default |
| --- | --- | --- |
| `bark.mnemonic` | `BARK_MNEMONIC` | Required |
| `bark.server_address` | `BARK_SERVER_ADDRESS` | `https://ark.second.tech` |
| `bark.esplora_address` | `BARK_ESPLORA_ADDRESS` | `https://mempool.second.tech/api` |
| `bark.network` | `BARK_NETWORK` | `mainnet` |
| `bark.data_dir` | `BARK_DATA_DIR` | `.data/bark` |
| `bark.event_poll_interval_ms` | `BARK_EVENT_POLL_INTERVAL_MS` | `5000` |
| `bark.payment_methods` | `BARK_PAYMENT_METHODS` | all supported |
| `address` | `SERVER_ADDRESS` | `127.0.0.1` |
| `port` | `SERVER_PORT` | `50051` |
| `tls_enable` | `TLS_ENABLE` | `false` |
| `allow_insecure` | `ALLOW_INSECURE` | `false` |
| `tls_cert_path` | `TLS_CERT_PATH` | `certs/server.crt` |
| `tls_key_path` | `TLS_KEY_PATH` | `certs/server.key` |
| `tls_client_ca_path` | `TLS_CLIENT_CA_PATH` | `certs/ca.pem` |

Boolean environment variables accept only the literal values `true` and
`false`.

Supported network values are `mainnet`, `testnet`, `signet`, and `regtest`.
Any other value causes the processor to fail during startup. The Bark server,
Esplora endpoint, and network must refer to the same network.

The mnemonic controls the processor's funds. Never use the mnemonic from
`config.toml.example`, commit a real mnemonic, or expose it in logs.

### Advertised payment methods

A CDK mint selects its backend per `(unit, method)` pair, and registers a
backend for every method that backend advertises in its settings. By default
this processor advertises `bolt11`, `onchain`, and the custom `arkoor` method,
so a mint backed only by bark serves all three without extra configuration.

That default makes bark the only backend a mint can have, because a second
backend offering `bolt11` would collide on the same `(unit, method)` pair and
the mint rejects the duplicate. Set `bark.payment_methods` to advertise a
subset instead, which frees the remaining rails for another backend:

```toml
[bark]
# Core Lightning keeps bolt11; this processor serves on-chain deposits only.
payment_methods = ["onchain"]
```

The environment form takes a comma-separated list:

```bash
BARK_PAYMENT_METHODS=onchain
```

Supported values are `bolt11`, `onchain`, and `arkoor`. Names are
case-insensitive and surrounding whitespace is ignored. An unrecognised name
fails at startup rather than being skipped, so a typo cannot silently drop a
rail. Leaving the setting unset, empty, or absent advertises everything.

### Persistent state

The processor stores Bark wallet data in `<data_dir>/db.sqlite` and payment
state in `<data_dir>/onchain_state.redb`. Keep the mnemonic and the complete
data directory secure, and do not share one data directory between running
instances.

On-chain deposits are reported after they have one confirmation and have been
boarded into Ark. The amount reported to the mint is the received amount after
the Bark boarding fee.

Background payment polling rotates across Lightning and on-chain receives and
sends. Each pass is bounded and its scan position is persisted, so a busy
payment type or large state history cannot indefinitely delay other events,
including after a restart. `bark.event_poll_interval_ms` controls the delay
between passes and must be greater than zero.

### Arkoor payments

Arkoor is exposed as a CDK custom outgoing method instead of being inferred
from a BOLT11 invoice. Use method `arkoor`, put the destination Ark address in
`request`, and provide the standard CDK custom-payment `amount`. The legacy
`extra_json` representation remains accepted for compatibility:

```json
{"amount_sat": 10000}
```

The amount must be a positive integer. Arkoor quotes have zero fee and a
successful payment reports `total_spent` equal to that amount. Quote mappings,
send intents, terminal results, and event-delivery markers are persisted so a
retry or restart does not deliberately send the same quote twice.

## Run

```bash
cargo run --release
```

Set `RUST_LOG` to change logging verbosity:

```bash
RUST_LOG=debug cargo run
```

By default, the CDK payment processor endpoint is:

```text
http://127.0.0.1:50051
```

Configure the CDK mint to use that endpoint. Keep the processor and mint on a
trusted network.

For CDK 0.18, use a bare host without a URI scheme:

```toml
[payment_backend]
backend = "grpcprocessor"
unit = "sat"

[grpc_processor]
supported_units = ["sat"]
address = "127.0.0.1"
port = 50051
allow_insecure = true
```

Existing mint operators must first follow the
[CDK v0.18 migration guide](https://github.com/cashubtc/cdk/blob/main/docs/migrations/v0.18.md).

### Transport security

Set `tls_enable = true` to serve gRPC over mutual TLS. `tls_cert_path` and
`tls_key_path` configure the server identity; `tls_client_ca_path` must contain
the CA certificate that signed the mint's `client.pem`. Configure the mint's
`[grpc_processor].tls_dir` with `ca.pem`, `client.pem`, and `client.key`.
The process fails during startup if any configured file cannot be read.

Without TLS, startup fails unless `allow_insecure = true` (or
`ALLOW_INSECURE=true`) is explicitly configured. The opt-in permits cleartext
on any bind address so it can be used in containers; startup logs a warning
with the effective address and a stronger exposure warning for non-loopback
binds. Configure mutual TLS whenever the network is not fully trusted.

The process handles `SIGINT` and `SIGTERM` and stops the gRPC server
gracefully.

## Development

The `justfile` wraps the common Cargo commands:

```bash
just check
just test
just fmt
just lint
just ci
```

The equivalent commands can be run directly:

```bash
cargo check
cargo test
cargo fmt -- --check
cargo clippy -- -D warnings
```

### Full Regtest suite

The ordinary test command runs deterministic unit and persistence-contract
tests. The full suite is opt-in because it launches Bitcoin Core, Esplora,
PostgreSQL, two balanced Core Lightning nodes, a Bark server, the real
payment-processor binary, a CDK mint, and a CDK wallet.

Run it from this directory with Nix:

```bash
just regtest
```

The Cargo test harness is pinned to Bark tag `bark-X.Y.Z`, matching the Bark
`X.Y.Z` dependencies in `Cargo.toml`. Linux runs Core Lightning directly;
on macOS, Bark's upstream harness uses Docker, so a Docker daemon must be
running. To use an already prepared environment, run `just test-regtest`.
The integration shell supplies only the pinned service binaries, required
sibling tools such as `bitcoin-cli`, and their matching native runtime library
paths; it reuses `cargo` and `rustc` from the caller's PATH. Install stable Rust
first, or enter the default `nix develop` shell before running `just regtest`.

The suite covers:

- fresh-wallet validation, identity and restart behavior, data-directory
  locking in-process and from a second real processor, insufficient funds, and
  network/endpoint failures;
- Lightning receive/send through status polling and event streams, including
  payment-hash correlation, expired incoming invoices, concurrency, exact
  amounts, fee caps, preimages, idempotency verified against Bark's movement
  history, unreachable destinations with balance recovery, and held payments
  completed across backend restarts;
- on-chain deposit/boarding and offboard sends, confirmation boundaries,
  retries, restarts, fees, wrong-network addresses, fee-index rejection, and a
  one-block reorg;
- the custom arkoor route and zero-fee accounting;
- black-box calls through the real gRPC process; and
- full Cashu mint/melt transitions, proof accounting and compensation,
  out-of-order quote correlation, unused fee-reserve return, and a pending
  melt recovered after both the mint and processor restart.

All network waits use polling deadlines. The test is marked ignored so
`cargo test --all-features` compiles it without trying to launch services.
Runs keep the service logs and databases under
`../../target/bark-regtest/cdk-payment-processor/bark-regtest/` for diagnosis.

## Project layout

```text
src/
├── backend.rs   # Bark implementation of MintPayment
├── lib.rs       # Library entry point used by the black-box harness
├── main.rs      # gRPC server startup and shutdown
└── settings.rs  # config.toml and environment loading
```

## License

MIT. See [LICENSE](LICENSE).
