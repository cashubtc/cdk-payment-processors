//! Template Lightning Backend Implementation
//!
//! This is a template/stub implementation that you should customize for your specific
//! Lightning backend (Blink, LND, Core Lightning, LDK, etc.).
//!
//! # Quick Start Guide
//!
//! 1. Replace `TemplateBackend` with your backend name (e.g., `BlinkBackend`, `LndBackend`)
//! 2. Add your backend-specific dependencies to Cargo.toml
//! 3. Add configuration fields (API keys, URLs, etc.) to the struct
//! 4. Replace all `todo!()` macros with actual implementations
//! 5. Update the constructor to initialize your backend client
//! 6. Test each method thoroughly

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use cdk_common::nuts::CurrencyUnit;
use cdk_common::payment::{
    CreateIncomingPaymentResponse, Event, IncomingPaymentOptions, MakePaymentResponse, MintPayment,
    OutgoingPaymentOptions, PaymentIdentifier, PaymentQuoteResponse, WaitPaymentResponse,
};
use futures_core::Stream;

/// Template backend implementation
///
/// Replace this with your actual Lightning backend name and add the necessary fields:
/// - API credentials (keys, macaroons, certificates)
/// - HTTP/gRPC clients
/// - Configuration settings
/// - Connection state
///
/// # TODO
/// - [ ] Rename struct to match your backend (e.g., BlinkBackend, LndBackend)
/// - [ ] Add backend-specific fields
/// - [ ] Add appropriate derives based on your needs
pub struct TemplateBackend {
    // TODO: Add your backend-specific fields here
    // Examples:
    // client: reqwest::Client,
    // api_url: String,
    // api_key: String,
    // node_url: String,
    // macaroon: Vec<u8>,
    // cert: Vec<u8>,
    wait_invoice_active: Arc<AtomicBool>,
}

impl TemplateBackend {
    /// Create a new backend instance
    ///
    /// # TODO
    /// - [ ] Accept necessary configuration parameters (e.g., api_url, api_key)
    /// - [ ] Initialize your HTTP/gRPC client
    /// - [ ] Validate credentials/connection
    /// - [ ] Set up any required background tasks
    ///
    /// # Example
    /// ```rust,ignore
    /// pub fn new(api_url: String, api_key: String) -> anyhow::Result<Self> {
    ///     if api_key.is_empty() {
    ///         anyhow::bail!("API key is required");
    ///     }
    ///
    ///     let client = reqwest::Client::builder()
    ///         .timeout(std::time::Duration::from_secs(30))
    ///         .build()?;
    ///
    ///     Ok(Self {
    ///         client,
    ///         api_url,
    ///         api_key,
    ///         wait_invoice_active: Arc::new(AtomicBool::new(false)),
    ///     })
    /// }
    /// ```
    pub fn new() -> anyhow::Result<Self> {
        // TODO: Replace this with your actual backend initialization
        // This default implementation allows the template to compile and run
        Ok(Self::default())
    }
}

impl Default for TemplateBackend {
    fn default() -> Self {
        Self {
            wait_invoice_active: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl MintPayment for TemplateBackend {
    type Err = cdk_common::payment::Error;

    /// Get backend settings - returns capabilities and supported features
    async fn get_settings(&self) -> Result<serde_json::Value, Self::Err> {
        // TODO: Update this to reflect your backend's actual capabilities
        //
        // Return a JSON value describing what your backend supports:
        // - bolt11: true if BOLT11 invoices supported
        // - bolt12: true if BOLT12 offers supported
        // - mpp: true if multi-path payments supported
        // - amp: true if atomic multi-path payments supported
        // - unit: "sat", "msat", "btc", etc.
        //
        // Example for a basic backend:
        Ok(serde_json::json!({
            "bolt11": true,
            "bolt12": false,
            "mpp": false,
            "amp": false,
            "unit": "sat",
        }))
    }

    /// Create an incoming payment request (invoice)
    async fn create_incoming_payment_request(
        &self,
        _unit: &CurrencyUnit,
        _options: IncomingPaymentOptions,
    ) -> Result<CreateIncomingPaymentResponse, Self::Err> {
        // TODO: Implement invoice creation for your backend
        //
        // Steps:
        // 1. Extract amount and description from options (match on Bolt11 vs Bolt12)
        // 2. Call your backend API to create invoice
        // 3. Parse the response
        // 4. Return CreateIncomingPaymentResponse with request_lookup_id and request
        //
        // Example for BOLT11:
        // if let IncomingPaymentOptions::Bolt11(opts) = options {
        //     let response = self.client
        //         .post(&format!("{}/invoice", self.api_url))
        //         .header("Authorization", format!("Bearer {}", self.api_key))
        //         .json(&json!({
        //             "amount": opts.amount.to_sat(),
        //             "memo": opts.description.unwrap_or_default(),
        //             "expiry": opts.expiry.unwrap_or(3600),
        //         }))
        //         .send()
        //         .await
        //         .map_err(|e| cdk_common::payment::Error::Lightning(Box::new(e)))?;
        //
        //     let data: YourInvoiceResponse = response.json().await
        //         .map_err(|e| cdk_common::payment::Error::Lightning(Box::new(e)))?;
        //
        //     Ok(CreateIncomingPaymentResponse {
        //         request_lookup_id: PaymentIdentifier::PaymentHash(data.payment_hash),
        //         request: data.payment_request,
        //         expiry: Some(data.expires_at),
        //     })
        // } else {
        //     Err(cdk_common::payment::Error::UnsupportedPaymentOption)
        // }

        todo!("Implement create_incoming_payment_request")
    }

    /// Get a payment quote (fee estimation for outgoing payment)
    async fn get_payment_quote(
        &self,
        _unit: &CurrencyUnit,
        _options: OutgoingPaymentOptions,
    ) -> Result<PaymentQuoteResponse, Self::Err> {
        // TODO: Implement payment quote/fee estimation
        //
        // Steps:
        // 1. Extract payment request from options
        // 2. Decode invoice to get amount
        // 3. Call backend fee estimation API (if available) or use conservative estimate
        // 4. Return PaymentQuoteResponse
        //
        // Example for BOLT11:
        // if let OutgoingPaymentOptions::Bolt11(opts) = options {
        //     let invoice = &opts.bolt11;
        //     let amount = invoice.amount_milli_satoshis()
        //         .ok_or(cdk_common::payment::Error::Custom("Amountless invoice".to_string()))?;
        //
        //     // Conservative fee estimate: 1% of amount or use backend API
        //     let fee_msat = amount / 100;
        //
        //     Ok(PaymentQuoteResponse {
        //         request_lookup_id: None,
        //         amount: Amount::from_msat(amount),
        //         fee: Amount::from_msat(fee_msat),
        //         unit: unit.clone(),
        //         state: MeltQuoteState::Unpaid,
        //     })
        // } else {
        //     Err(cdk_common::payment::Error::UnsupportedPaymentOption)
        // }

        todo!("Implement get_payment_quote")
    }

    /// Make an outgoing payment
    async fn make_payment(
        &self,
        _unit: &CurrencyUnit,
        _options: OutgoingPaymentOptions,
    ) -> Result<MakePaymentResponse, Self::Err> {
        // TODO: Implement payment sending
        //
        // Steps:
        // 1. Extract payment request and options
        // 2. Call your backend's pay API
        // 3. Wait for or poll payment status
        // 4. Return MakePaymentResponse with proof (preimage) and status
        //
        //     Ok(MakePaymentResponse {
        //         payment_lookup_id: PaymentIdentifier::PaymentHash(data.payment_hash),
        //         payment_proof: Some(data.preimage),
        //         status: MeltQuoteState::Paid,
        //         total_spent: Amount::from_sat(data.amount_sent + data.fee),
        //         unit: unit.clone(),
        //     })
        // } else {
        //     Err(cdk_common::payment::Error::UnsupportedPaymentOption)
        // }

        todo!("Implement make_payment")
    }

    /// Wait for payment events - returns a stream of incoming payment events
    async fn wait_payment_event(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Event> + Send>>, Self::Err> {
        // TODO: Implement payment event streaming
        //
        // This returns a stream of payment events. When an invoice is paid,
        // emit an Event::PaymentReceived with the payment details.
        //
        // Common approaches:
        // 1. WebSockets - connect to backend's websocket
        // 2. Server-Sent Events (SSE)
        // 3. gRPC streaming
        // 4. Polling (fallback)
        //
        // Example using channel + background task:
        // self.wait_invoice_active.store(true, Ordering::Relaxed);
        //
        // let (tx, rx) = mpsc::channel(100);
        // // TODO: Spawn background task to poll/stream payments
        // // When payment received, send:
        // // tx.send(Event::PaymentReceived(WaitPaymentResponse {
        // //     payment_identifier: PaymentIdentifier::PaymentHash(hash),
        // //     payment_amount: Amount::from_sat(amount),
        // //     payment_preimage: Some(preimage),
        // //     unit: CurrencyUnit::Sat,
        // // })).await;
        //
        // Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))

        todo!("Implement wait_payment_event")
    }

    /// Check if wait invoice is currently active
    fn is_wait_invoice_active(&self) -> bool {
        self.wait_invoice_active.load(Ordering::Relaxed)
    }

    /// Cancel waiting for invoice payments
    fn cancel_wait_invoice(&self) {
        self.wait_invoice_active.store(false, Ordering::Relaxed);
    }

    /// Check the status of an incoming payment
    async fn check_incoming_payment_status(
        &self,
        _payment_identifier: &PaymentIdentifier,
    ) -> Result<Vec<WaitPaymentResponse>, Self::Err> {
        // TODO: Implement incoming payment status check
        //
        // Steps:
        // 1. Query your backend for invoice status using identifier
        // 2. Return payment details if paid
        //
        // if data.is_paid {
        //     Ok(vec![WaitPaymentResponse {
        //         payment_identifier: payment_identifier.clone(),
        //         payment_amount: Amount::from_sat(data.amount),
        //         payment_preimage: data.preimage,
        //         unit: CurrencyUnit::Sat,
        //     }])
        // } else {
        //     Ok(vec![])
        // }

        todo!("Implement check_incoming_payment_status")
    }

    /// Check the status of an outgoing payment
    async fn check_outgoing_payment(
        &self,
        _payment_identifier: &PaymentIdentifier,
    ) -> Result<MakePaymentResponse, Self::Err> {
        // TODO: Implement outgoing payment status check
        //
        // Similar to check_incoming_payment_status but for outgoing payments
        //
        // Ok(MakePaymentResponse {
        //     payment_lookup_id: payment_identifier.clone(),
        //     payment_proof: data.preimage,
        //     status: if data.is_paid { MeltQuoteState::Paid } else { MeltQuoteState::Pending },
        //     total_spent: Amount::from_sat(data.total_amount),
        //     unit: CurrencyUnit::Sat,
        // })

        todo!("Implement check_outgoing_payment")
    }
}
