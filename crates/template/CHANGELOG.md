# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Justfile with common development commands
- GitHub Actions CI/CD workflow
- Graceful shutdown handling (SIGTERM/SIGINT)
- CONTRIBUTING.md with contribution guidelines
- CHANGELOG.md for tracking changes
- Backend configuration structure matching config.toml
- HTTP/2 keep-alive configuration options

### Changed
- Fixed TemplateBackend Default implementation to not panic
- Updated README.md with correct trait name (MintPayment instead of PaymentBackend)
- Corrected file path references in documentation
- Wired up configuration properly in main.rs
- Updated project structure documentation to reflect actual files

### Fixed
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
