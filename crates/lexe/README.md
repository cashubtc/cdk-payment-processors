# CDK Payment Processor - Lexe

A CDK payment processor backed by a [Lexe](https://github.com/lexe-app/lexe-public)
managed non-custodial Lightning node (SGX). Exposes the node to `cdk-mintd`
over the CDK payment processor gRPC protocol, with BOLT11 support.

The processor advertises `sat` and accepts new requests only in that unit.
The processor cannot enforce routing fee limits: new outgoing payments with
`max_fee_amount` are rejected before submission. This includes ordinary CDK
melts that supply a fee cap; they require SDK support for an enforceable limit
before they can be served by this processor. Incoming payments are supported.

Authentication follows the same model as
[lexe-mcp](https://github.com/vincenzopalazzo/lexe-mcp): a single base64
"SDK client credentials" blob created in the Lexe app (Menu → SDK clients).
An optional BIP39 seed phrase is supported as a fallback credential.

```text
cdk-mintd (payment_backend = "grpcprocessor")
  -> gRPC        cdk-payment-processor-lexe (this crate)
  -> Lexe SDK    Lexe managed node (gateway + node)
```

## Usage

```bash
cp config.toml.example config.toml   # import credentials, or leave them unset to create a wallet
cargo run --release
```

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
| `lexe.client_credentials` | `LEXE_CLIENT_CREDENTIALS` | unset |
| `lexe.client_credentials_path` | `LEXE_CLIENT_CREDENTIALS_PATH` | unset |
| `lexe.root_seed` | `LEXE_ROOT_SEED` or `LEXE_SEED_PHRASE` | unset |
| `lexe.root_seed_path` | `LEXE_ROOT_SEED_PATH` | unset |
| `lexe.create_wallet` | `LEXE_CREATE_WALLET` | `true` |
| `lexe.network` | `LEXE_NETWORK` | `mainnet` |
| `lexe.data_dir` | `LEXE_DATA_DIR` | `.data/lexe` |
| `lexe.fee_reserve_ppm` | `LEXE_FEE_RESERVE_PPM` | `10000` |
| `lexe.fee_reserve_min_sat` | `LEXE_FEE_RESERVE_MIN_SAT` | `2` |
| `lexe.payment_timeout_secs` | `LEXE_PAYMENT_TIMEOUT_SECS` | `300` |

Credential setup matches
[lexe-cli](https://github.com/lexe-app/lexe-public/tree/master/lexe-cli).
Set at most one source:

- import a client-credentials blob with `client_credentials` or
  `client_credentials_path` (Lexe app → Menu → SDK clients, or a `lexe-cli`
  export). This is the same base64 blob lexe-mcp uses as
  `Authorization: Bearer <credentials>`.
- import a root seed with `root_seed` or `root_seed_path`. The value may be
  a BIP39 mnemonic or a 64-character hex seed. `LEXE_SEED_PHRASE` is still
  accepted as an alias of `LEXE_ROOT_SEED`.
- leave every source unset. With `create_wallet = true` (the default) the
  processor does what `lexe init` does: generate a seed, persist it at the
  SDK seedphrase path under `data_dir`, then sign up and provision. Later
  starts reuse that file and do not generate another seed. Back it up before
  funding the node. Set `create_wallet = false` to refuse to start without
  an imported credential.

Imported client credentials do not provision; lexe-sdk 0.1.22 can only
provision with the root seed, and the app already keeps that node
provisioned. Root-seed wallets provision on every start.

When creating the SDK client in the Lexe app, grant the scopes the
processor needs: **Read info** + **Read payments** (status checks and the
payment-event stream), **Receive** (mint: `create_invoice`), and **Spend**
(melt: `pay_invoice`). A client without these is rejected by the node with
`Client lacks the required permission` (HTTP 403, error code 10).

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

## Behavior notes

- **Incoming (mint):** invoices and payment indexes are stored by payment
  hash in the local redb database. Completed incoming payments generate
  `PaymentReceived` events.
- **Quotes and fees:** fee estimates use
  `max(ceil(amount_sat * ppm / 1_000_000), min_sat)`. Defaults are 1%
  (10,000 ppm) and 2 sats, matching LDK Server's reserve defaults. These are
  estimates, not enforceable limits. Only requests with no `max_fee_amount`
  can initiate an outgoing payment; such requests explicitly have no fee cap.
  Principal and actual fees are summed in millisatoshis, then rounded up
  once to report a satoshi total.
- **Submission and retry safety:** a durable attempt record claims the
  payment hash and its original quote ID atomically before submission.
  Requesting another quote cannot replace the executing quote's event owner.
  Repeated or concurrent payment calls recover the existing attempt; they
  do not submit it again. A prepared quote with no attempt returns `Unpaid`,
  including after local rejection of a fee cap.
- **Status and recovery:** stored indexes are queried first. A missing index
  is recovered by paginating the full history for an outbound payment with
  the same hash. Remote `completed`/`failed`/`pending` states become
  `Paid`/`Failed`/`Pending`. Definite submission rejections (request-building,
  connection, request-validation, and authentication/permission errors) are
  persisted and return `Failed`, with zero spent, without a remote lookup.
  Retries and restarts preserve that result and the original quote owner.
  Submission is separate from settlement polling: the accepted payment's
  index is saved before polling, and errors during polling never mark it
  rejected. Timeouts, response-decoding errors, generic command/server errors,
  and other unclassified submission errors remain ambiguous. An unresolved
  attempt stays `Pending` (or returns a lookup error if the node cannot be
  reached); generic error messages and missing history do not prove rejection.
  A crash between recording intent and submission is also ambiguous and
  requires reconciliation. No existing attempt is automatically resubmitted.
- **Events and reconnects:** `get_updated_payments` reads cached and new
  updates in batches, polling every 5 seconds when caught up and backing off
  on errors. Each subscription replays history from the beginning, then
  advances its own cursor. This also reconciles after restart, and prevents
  SDK cache syncs from skipping events. Replays may duplicate terminal events;
  the mint must handle them idempotently by payment/quote ID. gRPC provides no
  event acknowledgment, so enqueueing an event is not treated as durable
  confirmation that the mint received it. Reconnect work scales with history.

Keep `lexe.data_dir` persistent. This schema has not shipped, so there is
no migration from an older quote database. Do not delete attempt records to
force a retry without first establishing the remote payment's final outcome.

## Startup self-check

With TLS disabled, after binding the processor calls its own `GetSettings`
from the local host
(using loopback for unspecified addresses such as `0.0.0.0`) and **exits
non-zero** if it does not answer. This fails fast on port conflicts instead of
looking healthy while another service owns the port.
The plaintext self-check is skipped when TLS is enabled; server errors are
still propagated.

## Development

Run `just ci` for formatting, Clippy, and tests. Tests mock the Lexe SDK
boundary and cover accounting, fee-cap rejection, concurrent submission,
definite versus ambiguous submission errors, settlement polling failures,
timeout/restart recovery, pagination, and event replay.
They do not provision a node or make payments. End-to-end validation requires
a real Lexe node and is not part of the test suite.

## Notes

- Depends on the `lexe` Rust SDK and `lexe-api-core` (crates.io, both pinned
  to `=0.1.22`). The SDK's `unstable` low-level node client is used only to
  separate submission from settlement and classify typed submission errors;
  upgrades must revalidate those error semantics. The crate ships a library
  (shared backend, used by its unit tests) plus the gRPC server binary; it is
  not published to crates.io.
- BOLT11 only; BOLT12 and on-chain payment options are not supported yet.
- A `Cargo.lock` is committed for reproducible standalone builds.
