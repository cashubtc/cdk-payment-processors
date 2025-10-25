# CDK Payment Processor Template - Just commands

# List available commands
default:
    @just --list

# Check code compilation
check:
    cargo check

# Build the project
build:
    cargo build

# Build release version
build-release:
    cargo build --release

# Run the server
run:
    cargo run

# Run with release optimizations
run-release:
    cargo run --release

# Run tests
test:
    cargo test

# Run tests with output
test-verbose:
    cargo test -- --nocapture

# Run clippy linter
lint:
    cargo clippy -- -D warnings

# Format code
fmt:
    cargo fmt

# Check formatting without modifying files
fmt-check:
    cargo fmt -- --check

# Run all checks (fmt, clippy, test)
ci: fmt-check lint test
    @echo "✅ All checks passed!"

# Build Docker image
docker-build:
    docker build -t cdk-payment-processor-template .

# Run Docker container
docker-run:
    docker run -p 50051:50051 cdk-payment-processor-template

# Clean build artifacts
clean:
    cargo clean

# Update dependencies
update:
    cargo update

# Check for outdated dependencies
outdated:
    cargo outdated

# Watch and auto-recompile on changes
watch:
    cargo watch -x check -x test

# Run with debug logging
run-debug:
    RUST_LOG=debug cargo run

# Run with trace logging
run-trace:
    RUST_LOG=trace cargo run
