# CDK Payment Processors

A collection of self-contained Cargo projects implementing CDK Payment
Processors (`MintPayment` interface over gRPC).

| Processor | Unit | BOLT11 | BOLT12 | On-chain | Custom |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [Bark](crates/bark/README.md) | `sat` | ✅ | ❌ | ✅ | `arkoor` |
| [LDK Server](crates/ldk-server/README.md) | `msat` | ✅ | ✅ | ❌ | - |
| [Spark](crates/spark/README.md) | `sat` | ✅ | ❌ | ❌ | - |
| [Template](crates/template/README.md) | `sat` | ✅ | ❌ | ❌ | - |

BOLT11 features:

| Processor | Amountless Invoices | MPP | Invoice Descriptions |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [Bark](crates/bark/README.md) | ❌ | ❌ | ✅ |
| [LDK Server](crates/ldk-server/README.md) | ✅ | ❌ | ✅ |
| [Spark](crates/spark/README.md) | ❌ | ❌ | ❌ |
| [Template](crates/template/README.md) | ❌ | ❌ | ✅ |

## Project structure

```text
crates/
├── bark/        # Payment processor backed by a Bark wallet
├── ldk-server/  # Payment processor backed by an LDK Server node
├── spark/       # Payment processor backed by a Spark wallet
└── template/    # Starting point for integrating a new payment backend
```

Each processor has its own `Cargo.toml`, `Cargo.lock`, configuration example,
and documentation. The crates intentionally do not share a Cargo workspace,
so dependency resolution and reproducible builds are independent.

Check or test every crate from the repository root:

```bash
for manifest in crates/*/Cargo.toml; do
  cargo check --locked --manifest-path "$manifest"
  cargo test --locked --manifest-path "$manifest"
done
```

Run an individual processor from its directory:

```bash
cd crates/template
cargo run --release
```
