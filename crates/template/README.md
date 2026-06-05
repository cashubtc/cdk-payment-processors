# CDK Payment Processor Template

A template for building gRPC-based Lightning Network payment processors that implement the CDK payment processor protocol. This project provides the complete gRPC server infrastructure and a clear interface for integrating your chosen Lightning backend (Blink, LND, Core Lightning, LNbits, etc.).

## What is This?

This is a **template project** - it compiles successfully but won't run until you implement your Lightning backend. Think of it as a starting point that handles all the gRPC complexity, leaving you to focus solely on integrating with your Lightning infrastructure.

### What's Included

- Complete gRPC server implementation
- Protobuf definitions for CDK payment processor protocol
- Clean `MintPayment` trait interface
- Configuration management system
- TLS support with auto-generated certificates
- Extensive inline documentation and examples
- Template backend with `todo!()` placeholders  

### What You Need to Add

- Your Lightning backend implementation
- API integration code (HTTP, gRPC, WebSocket, etc.)
- Backend-specific configuration
- Authentication and connection management  

## Quick Start

### Prerequisites

- Rust stable toolchain
- `protoc` (Protocol Buffers compiler)
  - macOS: `brew install protobuf`
  - Ubuntu/Debian: `sudo apt-get install protobuf-compiler`
  - Fedora: `sudo dnf install protobuf-compiler`
- `just` (optional, for task runner)
  - macOS: `brew install just`
  - Other platforms: `cargo install just`

### 1. Clone and Verify

```bash
git clone <your-repo>
cd cdk-payment-processor-template
cargo check  # Should compile successfully
```

### 2. Implement Your Backend

See the [Implementation Guide](#implementation-guide) below for detailed steps.

### 3. Configure and Run

```bash
# Configure your backend
export API_KEY="your-api-key"
export API_URL="https://your-backend-api"

# Run the server
RUST_LOG=info cargo run --release
```

## Implementation Guide

### Overview of the MintPayment Trait

The `MintPayment` trait (from the `cdk-common` crate) defines the interface your backend must implement. It requires these key methods:

```rust
#[async_trait]
pub trait MintPayment: Send + Sync {
    type Err: std::error::Error + Send + Sync + 'static;

    // Get backend capabilities and settings
    async fn get_settings(&self) -> Result<serde_json::Value, Self::Err>;
    
    // Create an incoming payment request (invoice)
    async fn create_incoming_payment_request(
        &self,
        unit: &CurrencyUnit,
        options: IncomingPaymentOptions,
    ) -> Result<CreateIncomingPaymentResponse, Self::Err>;
    
    // Get a payment quote (fee estimation)
    async fn get_payment_quote(
        &self,
        unit: &CurrencyUnit,
        options: OutgoingPaymentOptions,
    ) -> Result<PaymentQuoteResponse, Self::Err>;
    
    // Make an outgoing payment
    async fn make_payment(
        &self,
        unit: &CurrencyUnit,
        options: OutgoingPaymentOptions,
    ) -> Result<MakePaymentResponse, Self::Err>;
    
    // Stream incoming payment events
    async fn wait_payment_event(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Event> + Send>>, Self::Err>;
    
    // Check if wait invoice is active
    fn is_wait_invoice_active(&self) -> bool;
    
    // Cancel waiting for invoices
    fn cancel_wait_invoice(&self);
    
    // Check incoming payment status
    async fn check_incoming_payment_status(
        &self,
        payment_identifier: &PaymentIdentifier,
    ) -> Result<Vec<WaitPaymentResponse>, Self::Err>;
    
    // Check outgoing payment status
    async fn check_outgoing_payment(
        &self,
        payment_identifier: &PaymentIdentifier,
    ) -> Result<MakePaymentResponse, Self::Err>;
}
```

### Step-by-Step Implementation

#### 1. Rename and Customize the Template

Open `src/template_backend.rs` and rename `TemplateBackend` to your backend name:

```rust
// Before
pub struct TemplateBackend {
    // ...
}

// After (example for Blink)
pub struct BlinkBackend {
    client: reqwest::Client,
    api_url: String,
    api_key: String,
}
```

#### 2. Add Dependencies

Add your backend's required dependencies to `Cargo.toml`:

```toml
[dependencies]
# For HTTP APIs
reqwest = { version = "0.11", features = ["json"] }

# For GraphQL (e.g., Blink)
graphql_client = "0.13"

# For gRPC backends (e.g., LND)
tonic = "0.10"

# For WebSockets
tokio-tungstenite = "0.21"

# Add what you need!
```

#### 3. Implement the Constructor

```rust
impl BlinkBackend {
    pub fn new(api_url: String, api_key: String) -> anyhow::Result<Self> {
        // Validate configuration
        if api_key.is_empty() {
            anyhow::bail!("API key is required");
        }
        
        // Create HTTP client
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;
        
        Ok(Self {
            client,
            api_url,
            api_key,
        })
    }
    
    pub async fn test_connection(&self) -> anyhow::Result<()> {
        // Test connectivity to your backend
        let response = self.client
            .get(&format!("{}/health", self.api_url))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .send()
            .await?;
        
        if !response.status().is_success() {
            anyhow::bail!("Connection test failed");
        }
        
        Ok(())
    }
}
```

#### 4. Implement Each Trait Method

Replace each `todo!()` with your actual implementation. Here's an example for `create_invoice`:

```rust
async fn create_invoice(
    &self,
    amount_sat: u64,
    description: Option<String>,
    expiry_secs: Option<u64>,
) -> BackendResult<Invoice> {
    // Call your backend API
    let response = self.client
        .post(&format!("{}/invoices", self.api_url))
        .header("Authorization", format!("Bearer {}", self.api_key))
        .json(&serde_json::json!({
            "amount": amount_sat,
            "memo": description.unwrap_or_default(),
            "expiry": expiry_secs.unwrap_or(3600),
        }))
        .send()
        .await
        .map_err(|e| BackendError::Network(e.to_string()))?;
    
    // Check response status
    if !response.status().is_success() {
        return Err(BackendError::InvoiceError(
            format!("Failed to create invoice: {}", response.status())
        ));
    }
    
    // Parse response
    let data: serde_json::Value = response.json().await
        .map_err(|e| BackendError::Internal(e.to_string()))?;
    
    // Map to Invoice struct
    Ok(Invoice {
        payment_request: data["payment_request"].as_str()
            .ok_or_else(|| BackendError::Internal("Missing payment_request".into()))?
            .to_string(),
        payment_hash: data["payment_hash"].as_str()
            .ok_or_else(|| BackendError::Internal("Missing payment_hash".into()))?
            .to_string(),
        payment_secret: data.get("payment_secret")
            .and_then(|v| v.as_str())
            .map(String::from),
        amount_sat,
        expiry: data.get("expires_at")
            .and_then(|v| v.as_u64()),
    })
}
```

#### 5. Update Configuration

Add your backend-specific configuration to `src/settings.rs`:

```rust
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    // Add your fields
    pub blink_api_url: String,
    pub blink_api_key: String,
    pub blink_wallet_id: String,
    
    // Existing gRPC server config
    pub server_port: u16,
    pub tls_enable: bool,
    // ...
}

impl Default for Config {
    fn default() -> Self {
        Self {
            blink_api_url: "https://api.blink.sv/graphql".to_string(),
            blink_api_key: String::new(),
            blink_wallet_id: String::new(),
            server_port: 50051,
            tls_enable: false,
            // ...
        }
    }
}

impl Config {
    pub fn load() -> Self {
        // ... existing code ...
        
        // Add environment variable loading
        if let Ok(v) = std::env::var("BLINK_API_URL") {
            cfg.blink_api_url = v;
        }
        if let Ok(v) = std::env::var("BLINK_API_KEY") {
            cfg.blink_api_key = v;
        }
        if let Ok(v) = std::env::var("BLINK_WALLET_ID") {
            cfg.blink_wallet_id = v;
        }
        
        cfg
    }
}
```

#### 6. Initialize Your Backend in main.rs

Update `src/main.rs` to use your backend instead of `TemplateBackend`:

```rust
use crate::blink_backend::BlinkBackend;  // Your backend

#[tokio::main]
async fn main() -> Result<()> {
    // ... logging setup ...
    
    let cfg = settings::Config::from_env();
    
    // Initialize your backend
    let backend = BlinkBackend::new(
        cfg.blink_api_url.clone(),
        cfg.blink_api_key.clone(),
    )?;
    
    // Test connection
    backend.test_connection().await?;
    tracing::info!("Successfully connected to Blink backend");
    
    let backend: Arc<dyn MintPayment<Err = _>> = Arc::new(backend);
    
    // ... rest of server setup ...
}
```

#### 7. Test Your Implementation

```bash
# Set up environment
export BLINK_API_KEY="your-api-key"
export BLINK_API_URL="https://api.blink.sv/graphql"
export RUST_LOG=debug

# Run the server
cargo run

# In another terminal, test with grpcurl
grpcurl -plaintext -d '{}' 127.0.0.1:50051 \
  cdk_payment_processor.CdkPaymentProcessor/GetSettings
```

## Project Structure

```
src/
├── template_backend.rs     # Template backend with todo!() placeholders
├── settings.rs            # Configuration management
└── main.rs                # Entry point and server setup

config.toml                # Configuration file (optional)
Cargo.toml                # Dependencies and project metadata
Dockerfile                # Docker build configuration
```

The `MintPayment` trait and related types are provided by the `cdk-common` crate.

## Configuration

### Environment Variables

The template provides these base configuration options:

- `SERVER_PORT` - gRPC server port (default: 50051)
- `TLS_ENABLE` - Enable TLS (true/false)
- `TLS_CERT_PATH` - Path to TLS certificate
- `TLS_KEY_PATH` - Path to TLS private key
- `KEEP_ALIVE_INTERVAL` - HTTP/2 keep-alive interval (e.g., "30s")
- `KEEP_ALIVE_TIMEOUT` - HTTP/2 keep-alive timeout (e.g., "10s")
- `MAX_CONNECTION_AGE` - Maximum connection age (e.g., "30m")

Add your own environment variables for backend-specific configuration.

### config.toml

You can also use a `config.toml` file:

```toml
server_port = 50051
tls_enable = false

# Add your backend configuration
blink_api_url = "https://api.blink.sv/graphql"
blink_api_key = "your-key-here"
```

## gRPC API

The server implements the CDK payment processor protocol with these RPCs:

### Service: `cdk_payment_processor.CdkPaymentProcessor`

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `GetSettings` | `EmptyRequest` | `SettingsResponse` | Get backend capabilities |
| `CreatePayment` | `CreatePaymentRequest` | `CreatePaymentResponse` | Create invoice |
| `GetPaymentQuote` | `PaymentQuoteRequest` | `PaymentQuoteResponse` | Get payment quote |
| `MakePayment` | `MakePaymentRequest` | `MakePaymentResponse` | Send payment |
| `CheckIncomingPayment` | `CheckIncomingPaymentRequest` | `CheckIncomingPaymentResponse` | Check invoice |
| `CheckOutgoingPayment` | `CheckOutgoingPaymentRequest` | `MakePaymentResponse` | Check payment |
| `WaitIncomingPayment` | `EmptyRequest` | `stream WaitIncomingPaymentResponse` | Stream payments |

### Example: Create Invoice

```bash
grpcurl -plaintext -d '{
  "unit": "sat",
  "options": {
    "bolt11": {
      "description": "Coffee",
      "amount": 5000,
      "unix_expiry": 300
    }
  }
}' 127.0.0.1:50051 \
  cdk_payment_processor.CdkPaymentProcessor/CreatePayment
```

### Example: Send Payment

```bash
grpcurl -plaintext -d '{
  "payment_options": {
    "bolt11": {
      "bolt11": "lnbc50u1..."
    }
  }
}' 127.0.0.1:50051 \
  cdk_payment_processor.CdkPaymentProcessor/MakePayment
```

## Docker

```dockerfile
# Build
docker build -t my-payment-processor .

# Run with environment variables
docker run -p 50051:50051 \
  -e API_KEY="your-key" \
  -e API_URL="https://api.example.com" \
  my-payment-processor
```

## Testing

```bash
# Run tests
cargo test

# Run with logging
RUST_LOG=debug cargo test

# Check compilation
cargo check

# Lint
cargo clippy -- -D warnings

# Format
cargo fmt
```

## Development Tools

### Using Just (Task Runner)

The project includes a `justfile` with common development commands:

```bash
# List all available commands
just

# Common commands
just check          # Check compilation
just build          # Build the project
just run            # Run the server
just test           # Run tests
just lint           # Run clippy
just fmt            # Format code
just ci             # Run all checks (fmt, lint, test)

# Docker commands
just docker-build   # Build Docker image
just docker-run     # Run Docker container

# Advanced
just run-debug      # Run with debug logging
just watch          # Auto-recompile on changes
```

### Pre-commit Hooks

The project uses pre-commit hooks to ensure code quality. If you're using Nix:

```bash
# Hooks are automatically installed in the nix shell
nix develop
```

The hooks will automatically run on `git commit`:
- `rustfmt` - Format Rust code
- `nixpkgs-fmt` - Format Nix files  
- `typos` - Check for typos
- `commitizen` - Enforce conventional commit messages

To run hooks manually:
```bash
# In nix shell
pre-commit run --all-files
```

## Graceful Shutdown

The server handles graceful shutdown when receiving `SIGTERM` or `SIGINT` (Ctrl+C) signals:

```bash
# Run the server
cargo run

# In another terminal, send SIGTERM
kill -TERM <pid>

# Or just press Ctrl+C in the running terminal
```

The server will:
1. Stop accepting new connections
2. Complete in-flight requests
3. Clean up resources
4. Exit cleanly

This is important for production deployments, especially in containerized environments where orchestrators send SIGTERM before forcefully killing processes.

## Security Best Practices

- Never commit API keys or credentials
- Use environment variables for sensitive data
- Enable TLS in production (`tls_enable = true`)
- Implement proper authentication (mTLS, API tokens)
- Run behind a firewall
- Validate all inputs (the server handles this)
- Use secrets management (Vault, AWS Secrets Manager, etc.)

## Additional Resources

### Lightning Network Documentation

- [LND Documentation](https://docs.lightning.engineering/)
- [Core Lightning](https://docs.corelightning.org/)
- [Blink API](https://dev.blink.sv/)
- [LNbits](https://lnbits.com/)
- [BOLT Specifications](https://github.com/lightning/bolts)

### Rust Resources

- [Async Book](https://rust-lang.github.io/async-book/)
- [Tokio Tutorial](https://tokio.rs/tokio/tutorial)
- [Tonic (gRPC)](https://github.com/hyperium/tonic)

## Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

MIT License - see [LICENSE](LICENSE) for details

## FAQ

### Q: Can I use this with multiple Lightning backends?

A: This template is designed for a single backend per deployment. If you need multiple backends, you can:
- Deploy multiple instances with different configurations
- Extend the template to support backend selection at runtime
- Implement a "multi-backend" that routes to different backends

### Q: Does this support BOLT12?

A: The gRPC protocol includes BOLT12 support, but implementation depends on your backend. Return `BackendError::Unsupported` if your backend doesn't support BOLT12.

### Q: How do I handle backend reconnections?

A: Implement reconnection logic in your backend, especially in the `stream_incoming_payments` method. See the template comments for examples.

### Q: Can I use this with a custom Lightning implementation?

A: Yes! As long as you can implement the `MintPayment` trait methods, you can integrate any Lightning infrastructure.

## Examples of What You Can Build

- Blink Payment Processor
- LND Payment Gateway
- Core Lightning Integration
- LNbits Proxy
- Multi-tenant Lightning Service
- Custom Lightning Infrastructure

## Next Steps

1. Choose your Lightning backend
2. Read your backend's API documentation
3. Follow the [Implementation Guide](#implementation-guide)
4. Test thoroughly
5. Deploy to production
6. Share your implementation!

---

**Ready to build?** Start by renaming `template_backend.rs` to `your_backend.rs` and replacing the `todo!()` macros with your Lightning backend integration code!
