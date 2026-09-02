# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-09-02

### Added
- A crate-local `Cargo.lock` for reproducible standalone builds.
- Justfile with common development commands
- GitHub Actions CI/CD workflow
- Graceful shutdown handling (SIGTERM/SIGINT)
- CONTRIBUTING.md with contribution guidelines
- CHANGELOG.md for tracking changes
- Backend configuration structure matching config.toml

### Changed
- Renamed the backend-specific `config.toml` section from `[backend]` to
  `[template]`.
- Updated the template to CDK `0.18.0` and payment-processor protocol
  4.0.0.
- TLS mode now authenticates mint clients with `tls_client_ca_path`, and the
  documentation describes the CDK 0.18 mint configuration model.
- Configuration environment variables are now applied through reusable
  Figment providers, and boolean environment values accept only `true` or
  `false`.
- Made `TemplateBackend::new` asynchronous to match `BarkBackend::new` and
  support initialization that must be awaited.
- Fixed TemplateBackend Default implementation to not panic
- Updated README.md with correct trait name (MintPayment instead of PaymentBackend)
- Corrected file path references in documentation
- Wired up configuration properly in main.rs
- Updated project structure documentation to reflect actual files

### Fixed
- The payment processor now fails closed without TLS unless plaintext is
  explicitly enabled with `allow_insecure`, warning more strongly when the
  effective bind address is not loopback.
- Configuration mismatch between settings.rs and config.toml
- Server now uses configured port instead of hardcoded value
- The gRPC server now honors `tls_enable`, `tls_cert_path`, and
  `tls_key_path`.
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

[Unreleased]: https://github.com/cashubtc/cdk-payment-processors/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/cashubtc/cdk-payment-processors/releases/tag/v0.1.0
