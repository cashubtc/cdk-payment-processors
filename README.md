# CDK Payment Processors

A collection of self-contained Cargo projects implementing CDK Payment
Processors (`MintPayment` interface over gRPC).

See [CONTRIBUTING.md](CONTRIBUTING.md) for repository-wide development and
contribution guidelines.

## Support tiers

Payment processors are grouped by their level of support.

### Tier 1: maintained by the CDK team

| Processor | Backend |
| --- | --- |
| [Bark](crates/bark/README.md) | Bark wallet |
| [Spark](crates/spark/README.md) | Spark wallet |
| [LDK Server](crates/ldk-server/README.md) | LDK Server node |

### Tier 2: experimental, supported by external contributors only

| Processor | Backend |
| --- | --- |
| [LNbits](crates/lnbits/README.md) | LNbits wallet |

**Treat Tier 2 processors with caution: they are experimental and carry
additional risk.** Carefully review and test them before using them with real
funds.

> [!WARNING]
> **We do not recommend LNbits.** The LNbits processor is available in this
> repository, but we do not recommend using it. Prefer a Tier 1 processor
> instead.

### Unsupported: template

[Template](crates/template/README.md) is a starting point for new processors
and does not have a support tier.

## Feature comparison

| Processor | Tier | Unit | BOLT11 | BOLT12 | On-chain | Custom |
| --- | --- | --- | --- | --- | --- | --- |
| [Bark](crates/bark/README.md) | 1 | `sat` | ✅ | ❌ | ✅ | `arkoor` |
| [Spark](crates/spark/README.md) | 1 | `sat` | ✅ | ❌ | ❌ | - |
| [LDK Server](crates/ldk-server/README.md) | 1 | `msat` | ✅ | ✅ | ❌ | - |
| [LNbits](crates/lnbits/README.md) ⚠️ | 2 | `sat` | ✅ | ❌ | ❌ | - |
| [Template](crates/template/README.md) | - | `sat` | ✅ | ❌ | ❌ | - |

BOLT11 features:

| Processor | Tier | Amountless Invoices | MPP | Invoice Descriptions |
| --- | --- | --- | --- | --- |
| [Bark](crates/bark/README.md) | 1 | ❌ | ❌ | ✅ |
| [Spark](crates/spark/README.md) | 1 | ❌ | ❌ | ❌ |
| [LDK Server](crates/ldk-server/README.md) | 1 | ✅ | ❌ | ✅ |
| [LNbits](crates/lnbits/README.md) ⚠️ | 2 | ❌ | ❌ | ✅ |
| [Template](crates/template/README.md) | - | ❌ | ❌ | ✅ |

⚠️ Not recommended; see [Tier 2](#tier-2-experimental-supported-by-external-contributors-only).

## Project structure

```text
crates/
├── bark/        # Payment processor backed by a Bark wallet
├── ldk-server/  # Payment processor backed by an LDK Server node
├── lnbits/      # Payment processor backed by an LNbits wallet
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
