mod settings;
mod template_backend;

use crate::template_backend::TemplateBackend;
use anyhow::Result;
use std::sync::Arc;
use tokio::signal;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    // Logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    // Load configuration from environment
    let cfg = settings::Config::from_env();

    // TODO: Initialize your Lightning backend here
    // For now, we use the template backend which will panic with todo!() on any method call
    let backend = Arc::new(TemplateBackend::new()?);

    // Optional: Test the connection
    // backend.test_connection().await?;

    let server_addr = format!("0.0.0.0:{}", cfg.server_port);
    tracing::info!("Starting CDK Payment Processor server on {}", server_addr);

    let mut server =
        cdk_payment_processor::PaymentProcessorServer::new(backend, &server_addr, cfg.server_port)?;

    server.start(None).await?;

    // Wait for shutdown signal
    match shutdown_signal().await {
        Ok(_) => tracing::info!("Shutdown signal received, stopping server..."),
        Err(e) => tracing::error!("Error waiting for shutdown signal: {}", e),
    }

    server.stop().await?;
    tracing::info!("Server stopped gracefully");
    Ok(())
}

/// Wait for shutdown signal (SIGTERM or SIGINT)
async fn shutdown_signal() -> Result<()> {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    Ok(())
}
