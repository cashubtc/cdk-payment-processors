# Contributing to CDK Payment Processor Template

Thank you for your interest in contributing to the CDK Payment Processor Template! This document provides guidelines for contributing to the project.

## Code of Conduct

Be respectful and constructive in all interactions. We aim to create a welcoming environment for all contributors.

## How to Contribute

### Reporting Issues

If you find a bug or have a suggestion:

1. Check if an issue already exists in the GitHub Issues
2. If not, create a new issue with:
   - A clear, descriptive title
   - Detailed description of the problem or suggestion
   - Steps to reproduce (for bugs)
   - Expected vs actual behavior (for bugs)
   - Your environment (OS, Rust version, etc.)

### Pull Requests

1. **Fork the repository** and create a new branch from `main`
   ```bash
   git checkout -b feature/my-improvement
   ```

2. **Make your changes**
   - Write clear, documented code
   - Follow the existing code style
   - Add comments for complex logic
   - Update documentation if needed

3. **Test your changes**
   ```bash
   # Check compilation
   cargo check
   
   # Run tests (when they exist)
   cargo test
   
   # Run linter
   cargo clippy -- -D warnings
   
   # Check formatting
   cargo fmt -- --check
   ```

4. **Commit your changes**
   - Use conventional commit messages:
     ```
     feat: add new backend example
     fix: correct configuration loading
     docs: update README with new examples
     refactor: simplify error handling
     test: add integration tests
     chore: update dependencies
     ```

5. **Push and create a PR**
   ```bash
   git push origin feature/my-improvement
   ```
   - Provide a clear description of what your PR does
   - Reference any related issues
   - Explain any breaking changes

## Development Setup

### Prerequisites

- Rust stable toolchain
- `protoc` (Protocol Buffers compiler)
- Git

### Using Nix (Optional)

This project includes a Nix flake for reproducible development environments:

```bash
# Enter the development shell
nix develop

# Or use direnv
echo "use flake" > .envrc
direnv allow
```

### Quick Start

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/cdk-payment-processor-template.git
cd cdk-payment-processor-template

# Check everything works
just check

# Or manually
cargo check
cargo fmt
cargo clippy
```

## Code Style

### Rust Style

- Follow standard Rust conventions
- Use `rustfmt` (configuration in the project)
- Run `cargo clippy` and fix warnings
- Prefer explicit error handling over panics
- Add documentation comments (`///`) for public items

### Documentation

- Keep the README up to date
- Add inline comments for complex logic
- Use TODO comments with context when leaving items for later
- Update examples when changing APIs

### Naming Conventions

- Use descriptive variable names
- Follow Rust naming conventions:
  - `snake_case` for functions and variables
  - `PascalCase` for types and traits
  - `SCREAMING_SNAKE_CASE` for constants
- Prefer full words over abbreviations (except standard ones like `cfg`, `msg`, etc.)

## Project Structure

```
src/
├── template_backend.rs  # Template backend implementation
├── settings.rs         # Configuration management
└── main.rs            # Server entry point
```

## Testing

Currently, the template doesn't include tests (it's meant to be customized). However:

- When adding tests in the future, put them in the same file or in a `tests/` directory
- Use `#[cfg(test)]` modules for unit tests
- Integration tests go in `tests/` directory
- Mock external dependencies

## Documentation

### README Updates

When changing functionality:
- Update relevant sections in README.md
- Keep code examples working
- Add new sections for new features
- Update the FAQ if needed

### Inline Documentation

- Public items should have `///` doc comments
- Explain _why_ not just _what_
- Include examples in doc comments where helpful

## Commit Guidelines

We use conventional commits for clear history:

### Types

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code change that neither fixes a bug nor adds a feature
- `test`: Adding or updating tests
- `chore`: Maintenance tasks, dependency updates

### Examples

```
feat: add LND backend example
fix: correct configuration file parsing
docs: update installation instructions
refactor: simplify error handling in template backend
chore: update cdk-common to 0.14.0
```

## Review Process

1. All PRs require at least one review
2. CI must pass (formatting, clippy, build)
3. Documentation must be updated if APIs change
4. Breaking changes require discussion

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

## Questions?

- Open an issue for questions about contributing
- Tag with `question` label
- We'll try to respond quickly!

## Thank You!

Every contribution, no matter how small, helps make this template better for everyone. Thank you for taking the time to contribute!
