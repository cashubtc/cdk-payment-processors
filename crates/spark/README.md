# CDK Payment Processor for Spark

A gRPC payment processor implementing CDK's `MintPayment` interface with a
self-custodial Spark wallet. It connects directly to the Spark operators and
Spark Service Provider (SSP) through the low-level Rust `spark-wallet` crate.
No Breez API key is required.

## Features

- Create and pay amount-bound BOLT11 invoices
- Stream incoming payment notifications
- Persist quote-to-Spark-request mappings across restarts
- Estimate Lightning fees before sending
- Use stable Spark transfer IDs for retry-safe outgoing payments
- Shut down Spark background processing cleanly

The project uses `spark-wallet` from the Breez Spark SDK `0.19.0` source tag.
It does not depend on the high-level `breez-sdk-spark` crate or Breez auxiliary
services.

## Prerequisites

- Rust 1.88 or newer
- `protoc` (Protocol Buffers compiler)
- A funded Spark wallet mnemonic (12 or 24 BIP-39 words)

Install `protoc` with `brew install protobuf` on macOS or
`apt-get install protobuf-compiler` on Debian/Ubuntu.

## Configuration

The processor reads `config.toml` from its current directory. Environment
variables override file values.

### Environment variables

```bash
export SPARK_MNEMONIC="your twelve or twenty four word mnemonic phrase"
# Optional; defaults to .data/spark
export SPARK_DATA_DIR=".data/spark"
export SERVER_ADDRESS="127.0.0.1"
export SERVER_PORT="50051"
export TLS_ENABLE="false"
export TLS_CERT_PATH="certs/server.crt"
export TLS_KEY_PATH="certs/server.key"
```

A mnemonic is required, either through `SPARK_MNEMONIC` or the
`backend.mnemonic` configuration value. Keep it secret: it controls the Spark
wallet.

### Configuration file

Copy `config.toml.example` to `config.toml` in the directory where the
processor is started, then edit it:

```toml
backend_type = "spark"
address = "127.0.0.1"
port = 50051

tls_enable = false
tls_cert_path = "certs/server.crt"
tls_key_path = "certs/server.key"

[backend]
mnemonic = "your twelve or twenty four word mnemonic phrase"
data_dir = ".data/spark"
```

Set `tls_enable = true` to serve gRPC over TLS. `tls_cert_path` must point to a
PEM-encoded server certificate or certificate chain, and `tls_key_path` must
point to its PEM-encoded private key. The process fails during startup if
either file cannot be read or the TLS identity is invalid.

When TLS is disabled, bind to loopback or terminate TLS in front of the
service; do not expose the plaintext gRPC port directly to an untrusted
network.

The processor stores `quotes.db` inside `data_dir`, which defaults to
`.data/spark`. It contains invoices together with their Spark SSP request IDs
and idempotent transfer IDs. Wallet balances and transfers are synchronized
from the Spark network at startup.

## Run

```bash
cargo check
RUST_LOG=info cargo run
```

The gRPC server listens on `127.0.0.1:50051` by default.
After connecting, the processor logs the current Spark balance at `info` level.
If the balance cannot be retrieved, it logs a warning and continues starting.

For detailed logs from the low-level wallet:

```bash
RUST_LOG=cdk_payment_processor_spark=debug,spark_wallet=info,spark=info cargo run
```

## Payment behavior

The backend advertises amount-bound BOLT11 support with multi-part payments
(MPP) disabled. Incoming invoices do not embed a Spark address, and outgoing
BOLT11 payments do not take the direct Spark-address shortcut. This ensures
that the BOLT11 payment hash—the lookup identifier required by CDK—is actually
settled.

Incoming payment notifications and status checks report the received amount
without adding fees.

Outgoing calls may initially return `Pending`. CDK can poll the payment status;
the backend queries the persisted Spark SSP send request and returns the
preimage once the payment succeeds.

## Development

```bash
cargo fmt -- --check
cargo check
cargo test
cargo clippy --all-targets -- -D warnings
```

See `CONTRIBUTING.md` for contribution guidelines.

## Resources

- [Spark documentation](https://docs.spark.money/)
- [Breez Spark SDK source](https://github.com/breez/spark-sdk)
- [CDK](https://github.com/cashubtc/cdk)

## License

MIT License. See `LICENSE`.
