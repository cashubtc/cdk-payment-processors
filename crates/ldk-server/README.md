# CDK Payment Processor - LDK Server

A CDK payment processor backed by an [LDK Server](https://github.com/lightningdevkit/ldk-server) node. Exposes the node to `cdk-mintd` over the CDK payment processor gRPC protocol, with BOLT11 and BOLT12 (offers) support.

The processor correlates outgoing LDK payment events with CDK melt quote IDs.
When a payment initially returns as pending, a later success event finalizes
the melt and a permanent failure event allows CDK to compensate it. Terminal
events that arrive before the quote ID is known are buffered briefly and
replayed when `make_payment` registers the payment, so the event stream never
sleeps waiting for correlation.

```text
cdk-mintd (payment_backend = "grpcprocessor")
  -> gRPC        cdk-payment-processor-ldk-server (this crate)
  -> gRPC+TLS    ldk-server
```

## Usage

```bash
cp config.toml.example config.toml   # fill in your LDK Server connection details
cargo run --release
```

Point `cdk-mintd` at the processor:

```toml
[payment_backend]
backend = "grpcprocessor"
unit = "msat"

[grpc_processor]
supported_units = ["msat"]
address = "127.0.0.1"
port = 50051
allow_insecure = true
```

CDK 0.18 chooses the URI scheme from `tls_dir`, so `address` must not contain
`http://` or `https://`. Existing mint operators must migrate and initialize
their database-backed configuration before starting 0.18; follow the
[CDK v0.18 migration guide](https://github.com/cashubtc/cdk/blob/main/docs/migrations/v0.18.md).

## Configuration

See [config.toml.example](config.toml.example). A `config.toml` in the current
directory is optional, and environment variables take precedence over its
values.

| `config.toml` key | Environment variable | Default |
| --- | --- | --- |
| `address` | `SERVER_ADDRESS` | `127.0.0.1` |
| `port` | `SERVER_PORT` | `50051` |
| `tls_enable` | `TLS_ENABLE` | `false` |
| `allow_insecure` | `ALLOW_INSECURE` | `false` |
| `tls_cert_path` | `TLS_CERT_PATH` | `certs/server.crt` |
| `tls_key_path` | `TLS_KEY_PATH` | `certs/server.key` |
| `tls_client_ca_path` | `TLS_CLIENT_CA_PATH` | `certs/ca.pem` |
| `backend.address` | `LDK_ADDRESS` | Required |
| `backend.api_key` | `LDK_API_KEY` | Required |
| `backend.tls_cert_path` | `LDK_TLS_CERT_PATH` | Required |
| `backend.fee_reserve_min_sat` | `LDK_FEE_RESERVE_MIN_SAT` | `2` |
| `backend.fee_reserve_percent` | `LDK_FEE_RESERVE_PERCENT` | `0.01` |
| `backend.max_payment_scan_pages` | `LDK_MAX_PAYMENT_SCAN_PAGES` | `32` |

Boolean environment variables accept only the literal values `true` and
`false`.

With TLS enabled, `tls_client_ca_path` must contain the CA certificate that
signed the mint's `client.pem`; clients without a trusted certificate are
rejected. Configure the mint's `[grpc_processor].tls_dir` with `ca.pem`,
`client.pem`, and `client.key`.

Without TLS, startup fails unless `allow_insecure = true` (or
`ALLOW_INSECURE=true`) is explicitly configured. The opt-in permits
cleartext on any bind address so it can be used in containers; startup logs a
warning with the effective address and a stronger exposure warning for
non-loopback binds. Configure mutual TLS whenever the network is not fully
trusted.

## Startup self-check

After binding, the processor calls its own `GetSettings` from the local host
(using loopback for unspecified addresses such as `0.0.0.0`) and **exits
non-zero** if it does not answer. This fails fast on port conflicts instead of
looking healthy while another service owns the port.

## Regtest integration tests

The crate ships an opt-in regtest suite that runs the backend and the real
processor binary against two live `ldk-server` daemons (a payer node funding a
channel to the mint-side node) on a regtest `bitcoind`. It covers BOLT11 and
BOLT12 receive/send, quote fee math, held-HTLC pending/failed semantics,
invoice expiry, processor restarts, event streaming, and a full Cashu
mint/melt round trip. It prints progress between scenario groups and applies a
timeout to Cashu melt confirmation so event-delivery regressions fail with a
clear error instead of waiting indefinitely.

```bash
just test-regtest
```

Prerequisites: `git`, `protoc` (required by the `cdk-signatory` build),
network access, and `bitcoind`. If `BITCOIND_EXE` is unset, bitcoind is
downloaded automatically at build time via `corepc-node`.
The `ldk-server` daemon itself is resolved as follows:

- Set `LDK_SERVER_EXE=/path/to/ldk-server` to use an existing binary.
- Otherwise the upstream repository is cloned into
  `target/ldk-server-src` at the same revision pinned in `Cargo.toml` and
  built once into `target/ldk-server-build/debug/ldk-server`.

Artifacts (daemon logs, configs, mint database) are kept under
`target/ldk-server-regtest/run-<timestamp>/`; set `TEST_DIRECTORY` to change
the root.

## Notes

- Depends on `ldk-server-client` via a git rev; this crate is a binary and is not published to crates.io.
- Battle-tested in production on the Hedwig mint (BOLT11 mint/melt, BOLT12 mint quotes, mainnet). Originally developed at [vincenzopalazzo/cdk-ldk-server-processor](https://github.com/vincenzopalazzo/cdk-ldk-server-processor).
