# CDK Payment Processor - LNbits

A standalone CDK payment processor backed by an
[LNbits](https://github.com/lnbits/lnbits) wallet. It exposes LNbits to
`cdk-mintd` over the CDK payment processor gRPC protocol and supports BOLT11
mint and melt flows.

This crate adapts the former in-process `cdk-lnbits` backend that was
[removed from CDK](https://github.com/cashubtc/cdk/commit/f34de66323ebab42f0fa45b73706d8bf4ed3a409).
It targets CDK 0.18 and LNbits' v1 REST and websocket APIs.

```text
cdk-mintd (payment_backend = "grpcprocessor")
  -> gRPC/mTLS   cdk-payment-processor-lnbits (this crate)
  -> HTTPS/WSS   LNbits v1
```

## Capabilities

| Unit | BOLT11 | BOLT12 | On-chain | Amountless | MPP | Descriptions |
| --- | --- | --- | --- | --- | --- | --- |
| `sat` | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ |

Incoming payment events use the LNbits v1 websocket. If it disconnects, the
processor reconnects with bounded exponential backoff. Status polling remains
available through the LNbits REST API.

## Security model

LNbits is a custodial boundary for this processor. The configured admin API
key can spend every sat in its LNbits wallet. Keep the processor on a trusted
host, protect its configuration, use HTTPS/WSS for LNbits, and use mutual TLS
between the mint and this processor outside isolated development networks.

The processor validates the invoice/read key and establishes its websocket
subscription before opening the gRPC server. It never includes either API key
in `Debug` output. Websocket connection errors are deliberately redacted
because LNbits places the invoice key in the websocket URL.

## Usage

Copy the example configuration and fill in the LNbits wallet credentials:

```bash
cp config.toml.example config.toml
cargo run --release --locked
```

Both keys are available from the LNbits wallet's **API Info** page:

- The admin key pays invoices and is used for payment-status lookups.
- The invoice/read key creates invoices, validates wallet access, and
  authenticates the websocket subscription.

Point `cdk-mintd` at the processor:

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

CDK 0.18 chooses the URI scheme from `tls_dir`, so `address` must not include
`http://` or `https://`. Existing mint operators must migrate and initialize
their database-backed configuration before starting CDK 0.18; see the
[CDK v0.18 migration guide](https://github.com/cashubtc/cdk/blob/main/docs/migrations/v0.18.md).

## Configuration

A `config.toml` in the current directory is optional. Environment variables
override file values.

| `config.toml` key | Environment variable | Default |
| --- | --- | --- |
| `address` | `SERVER_ADDRESS` | `127.0.0.1` |
| `port` | `SERVER_PORT` | `50051` |
| `tls_enable` | `TLS_ENABLE` | `false` |
| `allow_insecure` | `ALLOW_INSECURE` | `false` |
| `tls_cert_path` | `TLS_CERT_PATH` | `certs/server.crt` |
| `tls_key_path` | `TLS_KEY_PATH` | `certs/server.key` |
| `tls_client_ca_path` | `TLS_CLIENT_CA_PATH` | `certs/ca.pem` |
| `lnbits.admin_api_key` | `LNBITS_ADMIN_API_KEY` | Required |
| `lnbits.invoice_api_key` | `LNBITS_INVOICE_API_KEY` | Required |
| `lnbits.api_url` | `LNBITS_API_URL` | Required |
| `lnbits.fee_reserve_min_sat` | `LNBITS_FEE_RESERVE_MIN_SAT` | `2` |
| `lnbits.fee_reserve_percent` | `LNBITS_FEE_RESERVE_PERCENT` | `0.02` |

Boolean environment variables accept only the literal values `true` and
`false`.

`lnbits.api_url` should normally be the instance root, for example
`https://lnbits.example.com/`. For compatibility with the former CDK backend,
a URL ending in `/api/v1` is normalized to the instance root. The legacy TOML
keys `lnbits_api`, `reserve_fee_min`, and `fee_percent` are also accepted.

### Fee reserve

LNbits does not provide a route-specific fee estimate through the client API
used here. Melt quotes therefore reserve the greater of:

- `fee_reserve_min_sat`; and
- the invoice amount multiplied by `fee_reserve_percent`.

For example, the defaults reserve at least 2 sats or 2 percent. Set these
values for the routing conditions and fee policy of the LNbits funding source.
The final paid response uses LNbits' actual amount and routing fee, rounded up
to whole satoshis. Pending, failed, and unknown payments report zero spent, as
required by the CDK 0.18 payment interface.

### Mutual TLS

With `tls_enable = true`, `tls_client_ca_path` must contain the CA certificate
that signed the mint's client certificate. Clients without a trusted
certificate are rejected. Configure the mint's `[grpc_processor].tls_dir`
with `ca.pem`, `client.pem`, and `client.key`.

Without TLS, startup fails unless `allow_insecure = true` (or
`ALLOW_INSECURE=true`) is explicitly configured. This opt-in permits cleartext
traffic on any bind address for local and container development. A stronger
warning is logged for non-loopback binds.

## Development

The crate is self-contained and has its own lockfile. Building it requires a
stable Rust toolchain and `protoc` (the Protocol Buffers compiler). Run its
standard checks from this directory:

```bash
just ci
```

Or run them directly from the repository root:

```bash
cargo fmt --manifest-path crates/lnbits/Cargo.toml -- --check
cargo clippy --locked --manifest-path crates/lnbits/Cargo.toml --all-targets -- -D warnings
cargo test --locked --manifest-path crates/lnbits/Cargo.toml
```

The unit tests do not require a live LNbits instance. End-to-end operation
requires LNbits v1 with working REST and websocket endpoints.

## License

Dual-licensed under the [MIT License](LICENSE-MIT) or the
[Apache License 2.0](LICENSE-APACHE), at your option. See
[LICENSE.md](LICENSE.md).
