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

See [config.toml.example](config.toml.example). Every value can also be set via a `CDK_LDK_`-prefixed environment variable (environment wins over the file).

| Key | Description | Default |
|---|---|---|
| `address` / `port` | gRPC listen address for the processor | `127.0.0.1:50051` |
| `tls_enable` | TLS for the processor gRPC server | `false` |
| `backend.address` | LDK Server gRPC address (no scheme) | required |
| `backend.api_key` | LDK Server HMAC API key (hex) | required |
| `backend.tls_cert_path` | PEM certificate pinned for the LDK Server connection | required |
| `backend.fee_reserve_min_sat` | Minimum absolute melt fee reserve | `2` |
| `backend.fee_reserve_percent` | Relative melt fee reserve (`0.01` = 1%) | `0.01` |
| `backend.max_payment_scan_pages` | `ListPayments` pages scanned for status lookups | `32` |

## Startup self-check

After binding, the processor calls its own `GetSettings` over loopback and **exits non-zero** if it does not answer. This fails fast on port conflicts instead of looking healthy while another service owns the port.

## Notes

- Depends on `ldk-server-client` via a git rev; this crate is a binary and is not published to crates.io.
- Battle-tested in production on the Hedwig mint (BOLT11 mint/melt, BOLT12 mint quotes, mainnet). Originally developed at [vincenzopalazzo/cdk-ldk-server-processor](https://github.com/vincenzopalazzo/cdk-ldk-server-processor).
