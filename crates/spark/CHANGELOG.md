# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Direct Spark operator and SSP integration through the low-level `spark-wallet` crate
- Persistent Spark receive request, send request, and transfer ID mappings
- Justfile with common development commands
- GitHub Actions CI/CD workflow
- Graceful shutdown handling (SIGTERM/SIGINT)
- CONTRIBUTING.md with contribution guidelines
- CHANGELOG.md for tracking changes
- Backend configuration structure matching config.toml
- HTTP/2 keep-alive configuration options

### Changed
- Replaced `breez-sdk-spark` 0.12.1 with `spark-wallet` from Breez Spark SDK 0.19.0
- Renamed the wallet mnemonic environment variable from `BREEZ_MNEMONIC` to `SPARK_MNEMONIC`
- Replaced `working_dir` and `WORKING_DIR` with the Bark-style `data_dir` setting and `SPARK_DATA_DIR` environment variable
- Removed the mnemonic passphrase configuration option
- Configuration is now loaded from `./config.toml`; quote mappings are stored in `<data_dir>/quotes.db`
- BOLT11 payments now preserve payment-hash settlement instead of using embedded Spark-address routing
- Fixed TemplateBackend Default implementation to not panic
- Updated README.md with correct trait name (MintPayment instead of PaymentBackend)
- Corrected file path references in documentation
- Wired up configuration properly in main.rs
- Updated project structure documentation to reflect actual files

### Fixed
- Removed the Breez API-key requirement
- Outgoing payments now report pending and failed states from the Spark SSP instead of always reporting paid
- Outgoing payments use stable Spark transfer IDs for retry safety
- Configuration mismatch between settings.rs and config.toml
- Server now uses configured port instead of hardcoded value
- Removed unused _cfg variable in main.rs

## [0.0.1] - 2024-10-18

### Added
- Initial template release
- Template backend with TODO placeholders for all MintPayment trait methods
- Configuration management with figment (file + environment variables)
- Comprehensive README with implementation guide
- Docker support
- Nix flake for development environment
- Pre-commit hooks configuration
- MIT License

### Features
- Complete gRPC server implementation via cdk-payment-processor
- Clean MintPayment trait interface from cdk-common
- TLS support configuration
- Extensive inline documentation
- Example configurations for different backends (Blink, LND, Core Lightning)

[Unreleased]: https://github.com/thesimplekid/cdk-template-payment-processor/compare/v0.0.1...HEAD
[0.0.1]: https://github.com/thesimplekid/cdk-template-payment-processor/releases/tag/v0.0.1
