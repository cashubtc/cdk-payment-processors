# CDK Payment Processor - LDK Server

A CDK payment processor backed by an [LDK Server](https://github.com/lightningdevkit/ldk-server) node. Exposes the node to `cdk-mintd` over the CDK payment processor gRPC protocol, with BOLT11 and BOLT12 (offers) support.

```text
cdk-mintd (ln_backend = "grpcprocessor")
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
[ln]
ln_backend = "grpcprocessor"

[grpc_processor]
addr = "http://127.0.0.1"
port = 50051
```

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
| `backend.address` | `LDK_ADDRESS` | Required |
| `backend.api_key` | `LDK_API_KEY` | Required |
| `backend.tls_cert_path` | `LDK_TLS_CERT_PATH` | Required |
| `backend.fee_reserve_min_sat` | `LDK_FEE_RESERVE_MIN_SAT` | `2` |
| `backend.fee_reserve_percent` | `LDK_FEE_RESERVE_PERCENT` | `0.01` |
| `backend.max_payment_scan_pages` | `LDK_MAX_PAYMENT_SCAN_PAGES` | `32` |

Boolean environment variables accept only the literal values `true` and
`false`.

Without TLS, startup fails unless `allow_insecure = true` (or
`ALLOW_INSECURE=true`) is explicitly configured. The opt-in permits
cleartext on any bind address so it can be used in containers; startup logs a
warning with the effective address and a stronger exposure warning for
non-loopback binds. Configure TLS with `tls_enable = true` and
`tls_cert_path`/`tls_key_path` whenever the network is not fully trusted.

## Startup self-check

After binding, the processor calls its own `GetSettings` from the local host
(using loopback for unspecified addresses such as `0.0.0.0`) and **exits
non-zero** if it does not answer. This fails fast on port conflicts instead of
looking healthy while another service owns the port.

## Notes

- Depends on `ldk-server-client` via a git rev; this crate is a binary and is not published to crates.io.
- Battle-tested in production on the Hedwig mint (BOLT11 mint/melt, BOLT12 mint quotes, mainnet). Originally developed at [vincenzopalazzo/cdk-ldk-server-processor](https://github.com/vincenzopalazzo/cdk-ldk-server-processor).
