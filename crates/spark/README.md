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

The project uses `spark-wallet` from the Breez Spark SDK. It does not depend
on the high-level `breez-sdk-spark` crate or Breez auxiliary services.

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
# Optional; one of mainnet (default), regtest, testnet, signet
export SPARK_NETWORK="mainnet"
# Optional; defaults to .data/spark
export SPARK_DATA_DIR=".data/spark"
export SERVER_ADDRESS="127.0.0.1"
export SERVER_PORT="50051"
export TLS_ENABLE="false"
export ALLOW_INSECURE="true" # Explicit cleartext opt-in
export TLS_CERT_PATH="certs/server.crt"
export TLS_KEY_PATH="certs/server.key"
export TLS_CLIENT_CA_PATH="certs/ca.pem"
```

Boolean environment variables accept only the literal values `true` and
`false`.

A mnemonic is required, either through `SPARK_MNEMONIC` or the
`backend.mnemonic` configuration value. Keep it secret: it controls the Spark
wallet.

### Configuration file

Copy `config.toml.example` to `config.toml` in the directory where the
processor is started, then edit it:

```toml
address = "127.0.0.1"
port = 50051

tls_enable = false
allow_insecure = true # Explicit cleartext opt-in
tls_cert_path = "certs/server.crt"
tls_key_path = "certs/server.key"
tls_client_ca_path = "certs/ca.pem"

[backend]
mnemonic = "your twelve or twenty four word mnemonic phrase"
network = "mainnet" # one of mainnet, regtest, testnet, signet
data_dir = ".data/spark"
```

`network` selects the Spark network. Only mainnet has public operators and an
SSP configured by default; regtest, testnet, and signet additionally require
running your own Spark operator federation and pointing the wallet at it.

### Custom operator federation and SSP

By default the wallet connects to the public operators and SSP for the
selected network. To connect to your own deployment instead, set
`backend.operators` (and optionally `backend.ssp`) in `config.toml`:

```toml
[backend]
mnemonic = "..."
network = "regtest"
split_secret_threshold = 2

[[backend.operators]]
address = "https://127.0.0.1:8535"
identifier = "0000000000000000000000000000000000000000000000000000000000000001"
identity_public_key = "03dfbdff4b6332c220f8fa2ba8ed496c698ceada563fa01b67d9983bfc5c95e763"
ca_cert_path = "certs/operator-0-ca.pem"

[[backend.operators]]
address = "https://127.0.0.1:8536"
identifier = "0000000000000000000000000000000000000000000000000000000000000002"
identity_public_key = "03e625e9768651c9be268e287245cc33f96a68ce9141b0b4769205db027ee8ed77"

[backend.ssp]
base_url = "https://localhost:8100"
identity_public_key = "022bf283544b16c0622daecb79422007d167eca6ce9f0c98c0c49833b1f7170bfe"
schema_endpoint = "graphql/spark/rc" # optional
```

- Operators are indexed by their position in the list (id 0, 1, ...); provide
  at least `split_secret_threshold` of them (default: 2, or the operator
  count if fewer).
- `identifier` is the operator's 32-byte FROST identifier in hex, and
  `identity_public_key` its 33-byte compressed public key in hex.
- `ca_cert_path` points to a PEM CA certificate used to verify the operator's
  TLS connection; omit it for publicly trusted certificates.
- Omitting `[backend.ssp]` keeps the default SSP; a custom SSP needs its base
  URL and identity public key.

These options are only available through `config.toml`, not environment
variables.

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

The processor stores `quotes.db` inside `data_dir`, which defaults to
`.data/spark`. It contains invoices together with their Spark SSP request IDs
and idempotent transfer IDs. Wallet balances and transfers are synchronized
from the Spark network at startup.

## Run

```bash
cargo check
RUST_LOG=info cargo run
```

With TLS configured, or with the explicit insecure opt-in shown above, the
gRPC server listens on `127.0.0.1:50051` by default.

For CDK 0.18, configure the mint with a bare host without a URI scheme:

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
After connecting, the processor logs the current Spark balance at `info` level.
If the balance cannot be retrieved, it logs a warning and continues starting.

For detailed logs from the low-level wallet:

```bash
RUST_LOG=cdk_payment_processor_spark=debug,spark_wallet=info,spark=info cargo run
```

## Payment behavior

The backend advertises amount-bound BOLT11 support with multi-part payments
(MPP) disabled. It supports satoshi-denominated requests only and rejects
other currency units. Incoming invoices do not embed a Spark address, and
outgoing BOLT11 payments do not take the direct Spark-address shortcut. This
ensures that the BOLT11 payment hash—the lookup identifier required by CDK—is
actually settled.

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

### Regtest integration tests

An opt-in regtest suite spins up a local Spark federation (regtest bitcoind
plus three `spark-so` operators with pre-seeded keyshares) using the upstream
`spark-itest` fixtures and Docker, then verifies network selection, custom
operator configuration, backend connectivity, event stream lifecycle, clean
failure paths when the Spark Service Provider is unreachable, on-chain
deposit claims, and the processor binary across restarts.

```bash
just test-regtest
```

Prerequisites: Docker (the first run builds the `spark-so` and
`spark-migrations` images from the pinned spark-sdk revision; they are cached
in `target/`) and `protoc`. Artifacts are kept under
`target/spark-regtest/run-<timestamp>/`; set `TEST_DIRECTORY` to change the
root. Because Lightning swaps require Lightspark's hosted SSP, which cannot
serve a local federation, payment settlement is covered by the failure-path
scenarios rather than live swaps.

See `CONTRIBUTING.md` for contribution guidelines.

## Resources

- [Spark documentation](https://docs.spark.money/)
- [Breez Spark SDK source](https://github.com/breez/spark-sdk)
- [CDK](https://github.com/cashubtc/cdk)

## License

MIT License. See `LICENSE`.
