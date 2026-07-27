# CDK Payment Processors

A Cargo workspace containing payment processors that implement
CDK Payment Processors (`MintPayment` interface over gRPC).

## Project structure

```text
Cargo.toml       # Workspace manifest
crates/
├── bark/        # Payment processor backed by a Bark wallet
├── ldk-server/  # Payment processor backed by an LDK Server node
├── spark/       # Payment processor backed by a Spark wallet
└── template/    # Starting point for integrating a new payment backend
```

Each processor has its own `Cargo.toml`, configuration example, and
documentation:

- [Bark](crates/bark/README.md)
- [LDK Server](crates/ldk-server/README.md)
- [Spark](crates/spark/README.md)
- [Template](crates/template/README.md)

Check or test every crate from the repository root:

```bash
cargo check --workspace
cargo test --workspace
```

Run an individual processor from its directory:

```bash
cd crates/template
cargo run --release
```
