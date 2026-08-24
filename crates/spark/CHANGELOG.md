# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Direct Spark operator and SSP integration through the low-level `spark-wallet` crate
- Persistent Spark receive request, send request, and transfer ID mappings
- Current Spark wallet balance logging after connection
- Justfile with common development commands
- GitHub Actions CI/CD workflow
- Graceful shutdown handling (SIGTERM/SIGINT)
- CONTRIBUTING.md with contribution guidelines
- CHANGELOG.md for tracking changes
- Backend configuration structure matching config.toml

### Changed
- Configuration environment variables are now applied through Figment
  providers, and boolean environment values accept only `true` or `false`.
- Updated `cdk-common` and `cdk-payment-processor` from 0.16.0-rc.1 to 0.17.3
- Updated `redb` from 3.1.0 to 4.1.0
- Made incoming quote persistence and outgoing transfer ID selection atomic
- Raised the minimum supported Rust version to 1.88
- BOLT11 settings now report multi-part payments as unsupported
- Renamed `server_addr` and `server_port` to `address` and `port`, and `SERVER_ADDR` to `SERVER_ADDRESS`
- Replaced `breez-sdk-spark` 0.12.1 with `spark-wallet` from Breez Spark SDK 0.22.0
- Outgoing payment recovery now queries persisted Spark transfer IDs directly before using the legacy invoice-history fallback
- Renamed the wallet mnemonic environment variable from `BREEZ_MNEMONIC` to `SPARK_MNEMONIC`
- Replaced `working_dir` and `WORKING_DIR` with the Bark-style `data_dir` setting and `SPARK_DATA_DIR` environment variable
- Removed the mnemonic passphrase configuration option
- Configuration is now loaded from `./config.toml`; quote mappings are stored in `<data_dir>/quotes.db`
- BOLT11 payments now preserve payment-hash settlement instead of using embedded Spark-address routing
- Updated README.md with correct trait name (MintPayment instead of PaymentBackend)
- Corrected file path references in documentation
- Wired up configuration properly in main.rs
- Updated project structure documentation to reflect actual files

### Fixed
- The payment processor now fails closed without TLS unless plaintext is
  explicitly enabled with `allow_insecure`, warning more strongly when the
  effective bind address is not loopback.
- Payment requests now reject non-satoshi units instead of accepting unsupported denominations or labeling satoshi amounts and fees with the caller's unit
- The gRPC server now honors `tls_enable`, `tls_cert_path`, and `tls_key_path`
- Removed the Breez API-key requirement
- Incoming payment events require a valid Lightning payment hash instead of constructing a fallback hash
- Incoming payment notifications and status checks report the received amount without adding fees
- Payment event subscriptions now clean up their activity state when each stream ends or is dropped
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

[Unreleased]: https://github.com/thesimplekid/cdk-spark-payment-processor/compare/v0.0.1...HEAD
[0.0.1]: https://github.com/thesimplekid/cdk-spark-payment-processor/releases/tag/v0.0.1
