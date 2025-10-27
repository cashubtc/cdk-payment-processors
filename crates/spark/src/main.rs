mod breez_backend;
mod database;
mod settings;

use crate::breez_backend::BreezBackend;
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

    // Initialize Breez SDK backend
    let backend = Arc::new(BreezBackend::new(cfg.backend).await?);

    tracing::info!(
        "Starting CDK Payment Processor server on {}:{}",
        cfg.server_addr,
        cfg.server_port
    );

    let mut server = cdk_payment_processor::PaymentProcessorServer::new(
        backend,
        &cfg.server_addr,
        cfg.server_port,
    )?;

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
