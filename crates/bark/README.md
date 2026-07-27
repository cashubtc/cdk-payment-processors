# CDK Payment Processor for Bark

A standalone gRPC payment processor that lets a CDK mint use a Bark wallet for
Lightning and on-chain Bitcoin payments through Ark.

The service implements CDK's `MintPayment` interface using `bark-wallet` and
runs it with `cdk-payment-processor`. It is currently built against CDK
`0.17.3` and Bark `0.3.0`.

## Supported payments

- Create and track fixed-amount BOLT11 invoices.
- Quote, pay, and reconcile outgoing BOLT11 payments.
- Use zero-fee arkoor routing when the destination is a valid Ark address,
  falling back to Lightning otherwise.
- Create on-chain deposit addresses, wait for one confirmation, and board the
  received funds into Ark.
- Quote and send on-chain payments by offboarding Bark funds.
- Stream incoming and outgoing payment events to the connected CDK mint.
- Persist payment intents and reconciliation state across restarts.

Only the `sat` unit is supported. BOLT11 MPP, amountless invoices, and BOLT12
are not supported.

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
| `backend.mnemonic` | `BARK_MNEMONIC` | Required |
| `backend.server_address` | `BARK_SERVER_ADDRESS` | `https://ark.second.tech` |
| `backend.esplora_address` | `BARK_ESPLORA_ADDRESS` | `https://mempool.second.tech/api` |
| `backend.network` | `BARK_NETWORK` | `mainnet` |
| `backend.data_dir` | `BARK_DATA_DIR` | `.data/bark` |
| `address` | `SERVER_ADDRESS` | `127.0.0.1` |
| `port` | `SERVER_PORT` | `50051` |
| `tls_enable` | `TLS_ENABLE` | `false` |
| `tls_cert_path` | `TLS_CERT_PATH` | `certs/server.crt` |
| `tls_key_path` | `TLS_KEY_PATH` | `certs/server.key` |

Supported network values are `mainnet`, `testnet`, `signet`, and `regtest`.
The Bark server, Esplora endpoint, and network must refer to the same network.

The mnemonic controls the processor's funds. Never use the mnemonic from
`config.toml.example`, commit a real mnemonic, or expose it in logs.

### Persistent state

The processor stores Bark wallet data in `<data_dir>/db.sqlite` and payment
state in `<data_dir>/onchain_state.redb`. Keep the mnemonic and the complete
data directory secure, and do not share one data directory between running
instances.

On-chain deposits are reported after they have one confirmation and have been
boarded into Ark. The amount reported to the mint is the received amount after
the Bark boarding fee.

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

### Transport security

Set `tls_enable = true` to serve gRPC over TLS. `tls_cert_path` must point to a
PEM-encoded server certificate or certificate chain, and `tls_key_path` must
point to its PEM-encoded private key. The process fails during startup if
either file cannot be read or the TLS identity is invalid.

When TLS is disabled, bind to loopback or terminate TLS in front of the
service; do not expose the plaintext gRPC port directly to an untrusted
network.

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

## Project layout

```text
src/
├── backend.rs   # Bark implementation of MintPayment
├── main.rs      # gRPC server startup and shutdown
└── settings.rs  # config.toml and environment loading
```

## License

MIT. See [LICENSE](LICENSE).
