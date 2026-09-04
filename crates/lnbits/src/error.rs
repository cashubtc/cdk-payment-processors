//! LNbits backend errors.

use thiserror::Error;

/// Errors produced by the LNbits backend.
#[derive(Debug, Error)]
pub enum Error {
    /// The backend configuration is invalid.
    #[error("invalid LNbits configuration: {0}")]
    InvalidConfiguration(String),

    /// The LNbits API client could not be constructed.
    #[error("failed to create LNbits client: {0}")]
    Client(#[source] anyhow::Error),

    /// An LNbits API request failed.
    #[error("LNbits {operation} request failed: {source}")]
    Api {
        /// Description of the attempted operation.
        operation: &'static str,
        /// Error returned by the LNbits client.
        #[source]
        source: anyhow::Error,
    },

    /// The websocket connection could not be established.
    ///
    /// The underlying error is deliberately omitted because the websocket URL
    /// contains the invoice API key.
    #[error("failed to connect to the LNbits websocket")]
    WebsocketConnection,

    /// An invoice did not contain an amount.
    #[error("invoice amount is required")]
    UnknownInvoiceAmount,

    /// An LNbits amount could not be represented safely.
    #[error("LNbits payment amount overflow")]
    AmountOverflow,

    /// LNbits returned a malformed payment hash.
    #[error("invalid LNbits payment hash")]
    InvalidPaymentHash,

    /// LNbits returned a payment hash other than the invoice payment hash.
    #[error("LNbits response payment hash does not match the invoice")]
    PaymentHashMismatch,

    /// A status lookup used an identifier LNbits cannot query.
    #[error("unsupported LNbits payment identifier type")]
    UnsupportedPaymentIdentifier,

    /// Only one consumer may read the LNbits websocket receiver.
    #[error("an LNbits payment event stream is already active")]
    EventStreamAlreadyActive,

    /// An incoming payment record had a non-positive amount.
    #[error("LNbits incoming payment has an invalid amount or direction")]
    InvalidIncomingAmount,
}

impl From<Error> for cdk_common::payment::Error {
    fn from(error: Error) -> Self {
        Self::Backend(Box::new(error))
    }
}
