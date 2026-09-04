use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use cdk_common::amount::{Amount, MSAT_IN_SAT};
use cdk_common::bitcoin::hashes::Hash as _;
use cdk_common::nuts::{CurrencyUnit, MeltQuoteState};
use cdk_common::payment::{
    self, Bolt11OutgoingPaymentOptions, Bolt11Settings, CreateIncomingPaymentResponse, Event,
    IncomingPaymentOptions, MakePaymentResponse, MintPayment, OutgoingPaymentOptions,
    PaymentIdentifier, PaymentQuoteResponse, SettingsResponse, WaitPaymentResponse,
};
use cdk_common::util::{hex, unix_time};
use cdk_common::Bolt11Invoice;
use futures::{stream, Stream};
use lnbits_rs::api::invoice::CreateInvoiceRequest;
use lnbits_rs::api::payment::Payment;
use lnbits_rs::LNBitsClient;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::error::Error;
use crate::settings::BackendConfig;

/// LNbits-backed BOLT11 payment backend.
#[derive(Clone)]
pub struct LnbitsBackend {
    client: LNBitsClient,
    fee_reserve_min_sat: u64,
    fee_reserve_percent: f32,
    event_cancel_token: CancellationToken,
    event_stream_active: Arc<AtomicBool>,
    started: Arc<AtomicBool>,
    start_lock: Arc<Mutex<()>>,
}

impl std::fmt::Debug for LnbitsBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LnbitsBackend")
            .field("fee_reserve_min_sat", &self.fee_reserve_min_sat)
            .field("fee_reserve_percent", &self.fee_reserve_percent)
            .field("started", &self.started.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl LnbitsBackend {
    /// Connect to the configured LNbits wallet and subscribe to its v1
    /// websocket payment notifications.
    pub async fn new(config: BackendConfig) -> Result<Self, Error> {
        config
            .validate()
            .map_err(|error| Error::InvalidConfiguration(error.to_string()))?;
        install_rustls_provider();

        let api_url = normalize_api_url(&config.api_url);
        let client = LNBitsClient::new(
            "",
            &config.admin_api_key,
            &config.invoice_api_key,
            &api_url,
            None,
        )
        .map_err(Error::Client)?;

        let backend = Self {
            client,
            fee_reserve_min_sat: config.fee_reserve_min_sat,
            fee_reserve_percent: config.fee_reserve_percent,
            event_cancel_token: CancellationToken::new(),
            event_stream_active: Arc::new(AtomicBool::new(false)),
            started: Arc::new(AtomicBool::new(false)),
            start_lock: Arc::new(Mutex::new(())),
        };
        backend.ensure_started().await?;
        Ok(backend)
    }

    async fn ensure_started(&self) -> Result<(), Error> {
        if self.started.load(Ordering::Acquire) {
            return Ok(());
        }

        let _guard = self.start_lock.lock().await;
        if self.started.load(Ordering::Acquire) {
            return Ok(());
        }

        self.client
            .get_wallet_details()
            .await
            .map_err(|source| Error::Api {
                operation: "wallet-info",
                source,
            })?;
        self.subscribe_websocket().await?;
        self.started.store(true, Ordering::Release);
        Ok(())
    }

    async fn subscribe_websocket(&self) -> Result<(), Error> {
        self.client
            .subscribe_to_websocket()
            .await
            // The source can contain the websocket URL, whose final path
            // component is the invoice API key. Do not retain or log it.
            .map_err(|_| Error::WebsocketConnection)
    }

    async fn process_websocket_message(
        payment_hash: String,
        client: &LNBitsClient,
    ) -> Option<WaitPaymentResponse> {
        let payment = match client.get_payment_info(&payment_hash).await {
            Ok(payment) => payment,
            Err(error) => {
                tracing::warn!(
                    payment_hash,
                    error = %error,
                    "Could not fetch the LNbits payment announced over the websocket"
                );
                return None;
            }
        };

        match incoming_payment_response(&payment_hash, &payment) {
            Ok(Some(response)) => Some(response),
            Ok(None) => {
                tracing::debug!(
                    payment_hash,
                    status = %payment.details.status,
                    "Ignoring an LNbits notification that is not a settled incoming payment"
                );
                None
            }
            Err(error) => {
                tracing::warn!(
                    payment_hash,
                    error = %error,
                    "Could not map an LNbits websocket payment"
                );
                None
            }
        }
    }

    fn fee_reserve(&self, amount_sat: u64) -> u64 {
        let relative = (self.fee_reserve_percent * amount_sat as f32) as u64;
        relative.max(self.fee_reserve_min_sat)
    }
}

#[async_trait]
impl MintPayment for LnbitsBackend {
    type Err = payment::Error;

    async fn start(&self) -> Result<(), Self::Err> {
        self.ensure_started().await.map_err(Into::into)
    }

    async fn stop(&self) -> Result<(), Self::Err> {
        self.event_cancel_token.cancel();
        self.event_stream_active.store(false, Ordering::Release);
        Ok(())
    }

    async fn get_settings(&self) -> Result<SettingsResponse, Self::Err> {
        Ok(settings_response())
    }

    async fn create_incoming_payment_request(
        &self,
        options: IncomingPaymentOptions,
    ) -> Result<CreateIncomingPaymentResponse, Self::Err> {
        let IncomingPaymentOptions::Bolt11(options) = options else {
            return Err(payment::Error::UnsupportedPaymentOption);
        };

        ensure_sat_unit(options.amount.unit())?;
        let amount_sat = options.amount.to_sat()?;
        if amount_sat == 0 {
            return Err(payment::Error::AmountMismatch);
        }

        let expiry = options
            .unix_expiry
            .map(|expiry| {
                expiry
                    .checked_sub(unix_time())
                    .ok_or(payment::Error::InvalidExpiry)
            })
            .transpose()?;
        let request = CreateInvoiceRequest {
            amount: amount_sat,
            unit: CurrencyUnit::Sat.to_string(),
            memo: Some(options.description.unwrap_or_default()),
            expiry,
            internal: None,
            out: false,
        };

        let response = self
            .client
            .create_invoice(&request)
            .await
            .map_err(|source| Error::Api {
                operation: "create-invoice",
                source,
            })?;
        let invoice: Bolt11Invoice = response.bolt11().parse()?;
        let invoice_hash = invoice.payment_hash().to_byte_array();
        let response_hash = decode_payment_hash(response.payment_hash())?;
        if response_hash != invoice_hash {
            return Err(Error::PaymentHashMismatch.into());
        }

        Ok(CreateIncomingPaymentResponse {
            request_lookup_id: PaymentIdentifier::PaymentHash(invoice_hash),
            request: invoice.to_string(),
            expiry: invoice.expires_at().map(|expiry| expiry.as_secs()),
            extra_json: None,
        })
    }

    async fn get_payment_quote(
        &self,
        unit: &CurrencyUnit,
        options: OutgoingPaymentOptions,
    ) -> Result<PaymentQuoteResponse, Self::Err> {
        ensure_sat_unit(unit)?;
        let OutgoingPaymentOptions::Bolt11(options) = options else {
            return Err(payment::Error::UnsupportedPaymentOption);
        };
        let amount_sat = bolt11_amount_sat(&options)?;
        let payment_hash = options.bolt11.payment_hash().to_byte_array();

        Ok(PaymentQuoteResponse {
            request_lookup_id: Some(PaymentIdentifier::PaymentHash(payment_hash)),
            amount: Amount::new(amount_sat, CurrencyUnit::Sat),
            fee: Amount::new(self.fee_reserve(amount_sat), CurrencyUnit::Sat),
            state: MeltQuoteState::Unpaid,
            extra_json: None,
            estimated_blocks: None,
            fee_options: None,
        })
    }

    async fn make_payment(
        &self,
        unit: &CurrencyUnit,
        options: OutgoingPaymentOptions,
    ) -> Result<MakePaymentResponse, Self::Err> {
        ensure_sat_unit(unit)?;
        let OutgoingPaymentOptions::Bolt11(options) = options else {
            return Err(payment::Error::UnsupportedPaymentOption);
        };
        // This backend cannot supply an amount to LNbits for an amountless
        // invoice, and it cannot dispatch a partial MPP payment.
        let _amount_sat = bolt11_amount_sat(&options)?;
        let invoice_hash = options.bolt11.payment_hash().to_byte_array();
        let payment_identifier = PaymentIdentifier::PaymentHash(invoice_hash);
        let payment_hash_hex = hex::encode(invoice_hash);

        let pay_response = match self
            .client
            .pay_invoice(&options.bolt11.to_string(), None)
            .await
        {
            Ok(response) => response,
            Err(source) => {
                // The request may have reached LNbits even if its response was
                // lost. Recover a paid or in-flight attempt before returning an
                // error, so a retry does not obscure the payment outcome.
                if let Ok(payment) = self.client.get_payment_info(&payment_hash_hex).await {
                    if matches!(
                        lnbits_payment_state(&payment),
                        MeltQuoteState::Paid | MeltQuoteState::Pending | MeltQuoteState::Unknown
                    ) {
                        return outgoing_payment_response(payment_identifier, &payment);
                    }
                }
                return Err(Error::Api {
                    operation: "pay-invoice",
                    source,
                }
                .into());
            }
        };

        let returned_hash = decode_payment_hash(&pay_response.payment_hash)?;
        if returned_hash != invoice_hash {
            return Err(Error::PaymentHashMismatch.into());
        }

        let payment = self
            .client
            .get_payment_info(&payment_hash_hex)
            .await
            .map_err(|source| Error::Api {
                operation: "payment-status",
                source,
            })?;
        outgoing_payment_response(payment_identifier, &payment)
    }

    async fn wait_payment_event(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Event> + Send>>, Self::Err> {
        self.ensure_started().await?;
        self.event_stream_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::EventStreamAlreadyActive)?;

        let state = EventStreamState {
            client: self.client.clone(),
            cancel_token: self.event_cancel_token.clone(),
            retry_count: 0,
        };
        let inner = stream::unfold(state, |mut state| async move {
            loop {
                let receiver = state.client.receiver();
                let mut receiver = receiver.lock().await;
                let message = tokio::select! {
                    _ = state.cancel_token.cancelled() => return None,
                    message = receiver.recv() => message,
                };
                drop(receiver);

                match message {
                    Some(payment_hash) => {
                        state.retry_count = 0;
                        if let Some(response) =
                            LnbitsBackend::process_websocket_message(payment_hash, &state.client)
                                .await
                        {
                            return Some((Event::PaymentReceived(response), state));
                        }
                    }
                    None => {
                        let delay = state.retry_delay();
                        tracing::warn!(
                            retry_delay_secs = delay.as_secs(),
                            "LNbits websocket closed; reconnecting"
                        );
                        if !sleep_or_cancel(&state.cancel_token, delay).await {
                            return None;
                        }

                        match state.client.subscribe_to_websocket().await {
                            Ok(()) => {
                                tracing::info!("Reconnected to the LNbits websocket");
                                state.retry_count = 0;
                            }
                            Err(_) => {
                                // Do not log the source: it can contain the
                                // websocket URL and invoice API key.
                                tracing::warn!("Could not reconnect to the LNbits websocket");
                                state.retry_count = state.retry_count.saturating_add(1);
                            }
                        }
                    }
                }
            }
        });

        Ok(Box::pin(PaymentEventStream::new(
            Box::pin(inner),
            Arc::clone(&self.event_stream_active),
        )))
    }

    fn is_payment_event_stream_active(&self) -> bool {
        self.event_stream_active.load(Ordering::Acquire)
    }

    fn cancel_payment_event_stream(&self) {
        self.event_cancel_token.cancel();
        self.event_stream_active.store(false, Ordering::Release);
    }

    async fn check_incoming_payment_status(
        &self,
        payment_identifier: &PaymentIdentifier,
    ) -> Result<Vec<WaitPaymentResponse>, Self::Err> {
        let payment_hash = payment_hash_hex(payment_identifier)?;
        let payment = self
            .client
            .get_payment_info(&payment_hash)
            .await
            .map_err(|source| Error::Api {
                operation: "incoming-payment-status",
                source,
            })?;

        Ok(incoming_payment_response(&payment_hash, &payment)?
            .into_iter()
            .collect())
    }

    async fn check_outgoing_payment(
        &self,
        payment_identifier: &PaymentIdentifier,
    ) -> Result<MakePaymentResponse, Self::Err> {
        let payment_hash = payment_hash_hex(payment_identifier)?;
        let payment = self
            .client
            .get_payment_info(&payment_hash)
            .await
            .map_err(|source| Error::Api {
                operation: "outgoing-payment-status",
                source,
            })?;

        outgoing_payment_response(payment_identifier.clone(), &payment)
    }
}

fn settings_response() -> SettingsResponse {
    SettingsResponse {
        unit: CurrencyUnit::Sat.to_string(),
        bolt11: Some(Bolt11Settings {
            mpp: false,
            amountless: false,
            invoice_description: true,
        }),
        bolt12: None,
        onchain: None,
        custom: std::collections::HashMap::new(),
    }
}

fn ensure_sat_unit(unit: &CurrencyUnit) -> Result<(), payment::Error> {
    if matches!(unit, CurrencyUnit::Sat) {
        Ok(())
    } else {
        Err(payment::Error::UnsupportedUnit)
    }
}

fn bolt11_amount_sat(options: &Bolt11OutgoingPaymentOptions) -> Result<u64, payment::Error> {
    if options.melt_options.is_some() {
        return Err(payment::Error::UnsupportedPaymentOption);
    }

    let amount_msat = options
        .bolt11
        .amount_milli_satoshis()
        .ok_or(Error::UnknownInvoiceAmount)?;
    let amount_sat = amount_msat.div_ceil(MSAT_IN_SAT);
    if amount_sat == 0 {
        return Err(payment::Error::AmountMismatch);
    }
    Ok(amount_sat)
}

fn incoming_payment_response(
    payment_hash: &str,
    payment: &Payment,
) -> Result<Option<WaitPaymentResponse>, Error> {
    if lnbits_payment_state(payment) != MeltQuoteState::Paid {
        return Ok(None);
    }
    if payment.details.amount <= 0 {
        return Err(Error::InvalidIncomingAmount);
    }

    let hash = decode_payment_hash(payment_hash)?;
    let amount_msat = u64::try_from(payment.details.amount).map_err(|_| Error::AmountOverflow)?;
    let payment_id = if payment.details.checking_id.is_empty() {
        payment_hash.to_string()
    } else {
        payment.details.checking_id.clone()
    };

    Ok(Some(WaitPaymentResponse {
        payment_identifier: PaymentIdentifier::PaymentHash(hash),
        payment_amount: Amount::new(amount_msat, CurrencyUnit::Msat),
        payment_id,
    }))
}

fn outgoing_payment_response(
    payment_identifier: PaymentIdentifier,
    payment: &Payment,
) -> Result<MakePaymentResponse, payment::Error> {
    let status = lnbits_payment_state(payment);
    let (payment_proof, total_spent) = if status == MeltQuoteState::Paid {
        let amount_msat = signed_magnitude(payment.details.amount)?;
        let fee_msat = signed_magnitude(payment.details.fee)?;
        let total_msat = amount_msat
            .checked_add(fee_msat)
            .ok_or(Error::AmountOverflow)?;
        (
            payment
                .preimage
                .as_deref()
                .filter(|preimage| !preimage.is_empty())
                .map(str::to_owned)
                .or_else(|| {
                    payment
                        .details
                        .preimage
                        .as_deref()
                        .filter(|preimage| !preimage.is_empty())
                        .map(str::to_owned)
                }),
            Amount::new(total_msat.div_ceil(MSAT_IN_SAT), CurrencyUnit::Sat),
        )
    } else {
        (None, Amount::new(0, CurrencyUnit::Sat))
    };

    Ok(MakePaymentResponse {
        payment_lookup_id: payment_identifier,
        payment_proof,
        status,
        total_spent,
    })
}

fn lnbits_payment_state(payment: &Payment) -> MeltQuoteState {
    if payment.paid {
        return MeltQuoteState::Paid;
    }

    match payment.details.status.trim().to_ascii_lowercase().as_str() {
        "success" => MeltQuoteState::Paid,
        "failed" => MeltQuoteState::Failed,
        "pending" => MeltQuoteState::Pending,
        _ => MeltQuoteState::Unknown,
    }
}

fn signed_magnitude(value: i64) -> Result<u64, Error> {
    if value == i64::MIN {
        return Err(Error::AmountOverflow);
    }
    Ok(value.unsigned_abs())
}

fn decode_payment_hash(payment_hash: &str) -> Result<[u8; 32], Error> {
    hex::decode(payment_hash)
        .map_err(|_| Error::InvalidPaymentHash)?
        .try_into()
        .map_err(|_| Error::InvalidPaymentHash)
}

fn payment_hash_hex(payment_identifier: &PaymentIdentifier) -> Result<String, Error> {
    match payment_identifier {
        PaymentIdentifier::PaymentHash(hash) => Ok(hex::encode(hash)),
        _ => Err(Error::UnsupportedPaymentIdentifier),
    }
}

fn normalize_api_url(api_url: &str) -> String {
    let trimmed = api_url.trim().trim_end_matches('/');
    let root = trimmed.strip_suffix("/api/v1").unwrap_or(trimmed);
    format!("{root}/")
}

fn install_rustls_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }
}

async fn sleep_or_cancel(cancel_token: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        _ = cancel_token.cancelled() => false,
        _ = tokio::time::sleep(duration) => true,
    }
}

struct EventStreamState {
    client: LNBitsClient,
    cancel_token: CancellationToken,
    retry_count: u32,
}

impl EventStreamState {
    fn retry_delay(&self) -> Duration {
        Duration::from_secs(2_u64.saturating_pow(self.retry_count).min(10))
    }
}

struct PaymentEventStream {
    inner: Pin<Box<dyn Stream<Item = Event> + Send>>,
    active: Arc<AtomicBool>,
}

impl PaymentEventStream {
    fn new(inner: Pin<Box<dyn Stream<Item = Event> + Send>>, active: Arc<AtomicBool>) -> Self {
        Self { inner, active }
    }
}

impl Stream for PaymentEventStream {
    type Item = Event;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let poll = this.inner.as_mut().poll_next(context);
        if matches!(poll, Poll::Ready(None)) {
            this.active.store(false, Ordering::Release);
        }
        poll
    }
}

impl Drop for PaymentEventStream {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payment(paid: bool, status: &str, amount: i64, fee: i64) -> Payment {
        serde_json::from_value(serde_json::json!({
            "paid": paid,
            "preimage": "top-level-preimage",
            "details": {
                "status": status,
                "checking_id": "checking-id",
                "amount": amount,
                "fee": fee,
                "memo": "",
                "time": "2026-01-01T00:00:00Z",
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
                "bolt11": "",
                "preimage": "details-preimage",
                "payment_hash": "11".repeat(32),
                "expiry": "2026-01-01T01:00:00Z",
                "extra": {},
                "wallet_id": "wallet-id"
            }
        }))
        .unwrap()
    }

    #[test]
    fn settings_advertise_only_supported_capabilities() {
        let settings = settings_response();
        assert_eq!(settings.unit, "sat");
        assert_eq!(
            settings.bolt11,
            Some(Bolt11Settings {
                mpp: false,
                amountless: false,
                invoice_description: true,
            })
        );
        assert!(settings.bolt12.is_none());
        assert!(settings.onchain.is_none());
        assert!(settings.custom.is_empty());
    }

    #[test]
    fn api_urls_are_normalized_for_the_lnbits_client() {
        assert_eq!(
            normalize_api_url(" https://lnbits.example.com "),
            "https://lnbits.example.com/"
        );
        assert_eq!(
            normalize_api_url("https://lnbits.example.com/api/v1/"),
            "https://lnbits.example.com/"
        );
        assert_eq!(
            normalize_api_url("https://example.com/lnbits/api/v1"),
            "https://example.com/lnbits/"
        );
    }

    #[test]
    fn payment_hashes_require_exactly_32_bytes() {
        let hash = "11".repeat(32);
        assert_eq!(decode_payment_hash(&hash).unwrap(), [0x11; 32]);
        assert!(decode_payment_hash("11").is_err());
        assert!(decode_payment_hash(&"zz".repeat(32)).is_err());
    }

    #[test]
    fn signed_magnitude_rejects_the_unrepresentable_edge_case() {
        assert_eq!(signed_magnitude(-42).unwrap(), 42);
        assert_eq!(signed_magnitude(42).unwrap(), 42);
        assert!(signed_magnitude(i64::MIN).is_err());
    }

    #[test]
    fn paid_outgoing_response_uses_preimage_and_rounds_up_to_sats() {
        let identifier = PaymentIdentifier::PaymentHash([0x11; 32]);
        let mut payment = payment(true, "success", -100_001, -1_001);
        let response = outgoing_payment_response(identifier.clone(), &payment).unwrap();

        assert_eq!(response.payment_lookup_id, identifier);
        assert_eq!(
            response.payment_proof.as_deref(),
            Some("top-level-preimage")
        );
        assert_eq!(response.status, MeltQuoteState::Paid);
        assert_eq!(response.total_spent, Amount::new(102, CurrencyUnit::Sat));

        payment.preimage = Some(String::new());
        let response =
            outgoing_payment_response(PaymentIdentifier::PaymentHash([0x11; 32]), &payment)
                .unwrap();
        assert_eq!(response.payment_proof.as_deref(), Some("details-preimage"));
    }

    #[test]
    fn unsettled_outgoing_responses_never_report_spend_or_proof() {
        for (lnbits_status, expected) in [
            ("pending", MeltQuoteState::Pending),
            ("failed", MeltQuoteState::Failed),
            ("unexpected", MeltQuoteState::Unknown),
        ] {
            let response = outgoing_payment_response(
                PaymentIdentifier::PaymentHash([0x11; 32]),
                &payment(false, lnbits_status, -100_000, -1_000),
            )
            .unwrap();

            assert_eq!(response.status, expected);
            assert!(response.payment_proof.is_none());
            assert_eq!(response.total_spent, Amount::new(0, CurrencyUnit::Sat));
        }
    }

    #[test]
    fn incoming_response_requires_a_paid_positive_payment() {
        let payment_hash = "11".repeat(32);
        let response =
            incoming_payment_response(&payment_hash, &payment(true, "success", 100_001, 0))
                .unwrap()
                .unwrap();

        assert_eq!(
            response.payment_identifier,
            PaymentIdentifier::PaymentHash([0x11; 32])
        );
        assert_eq!(
            response.payment_amount,
            Amount::new(100_001, CurrencyUnit::Msat)
        );
        assert_eq!(response.payment_id, "checking-id");
        assert!(
            incoming_payment_response(&payment_hash, &payment(false, "pending", 100_001, 0))
                .unwrap()
                .is_none()
        );
        assert!(
            incoming_payment_response(&payment_hash, &payment(true, "success", -100_001, 0))
                .is_err()
        );
    }

    #[test]
    fn retry_backoff_is_bounded() {
        let client = LNBitsClient::new("", "admin", "invoice", "http://127.0.0.1/", None).unwrap();
        let mut state = EventStreamState {
            client,
            cancel_token: CancellationToken::new(),
            retry_count: 0,
        };

        assert_eq!(state.retry_delay(), Duration::from_secs(1));
        state.retry_count = 3;
        assert_eq!(state.retry_delay(), Duration::from_secs(8));
        state.retry_count = u32::MAX;
        assert_eq!(state.retry_delay(), Duration::from_secs(10));
    }
}
