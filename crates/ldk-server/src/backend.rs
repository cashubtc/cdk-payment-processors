use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bitcoin::hashes::Hash;
use cdk_common::amount::{Amount, MSAT_IN_SAT};
use cdk_common::common::FeeReserve;
use cdk_common::nuts::{CurrencyUnit, MeltOptions, MeltQuoteState};
use cdk_common::payment::{
    self, CreateIncomingPaymentResponse, Event, IncomingPaymentOptions, MakePaymentResponse,
    MintPayment, OutgoingPaymentOptions, PaymentIdentifier, PaymentQuoteResponse, SettingsResponse,
    WaitPaymentResponse,
};
use cdk_common::util::{hex, unix_time};
use cdk_common::QuoteId;
use futures::Stream;
use ldk_server_client::client::{EventStream, LdkServerClient};
use ldk_server_client::ldk_server_grpc::api::{
    Bolt11ReceiveRequest, Bolt11SendRequest, Bolt12ReceiveRequest, Bolt12SendRequest,
    GetNodeInfoRequest, GetPaymentDetailsRequest, ListPaymentsRequest,
};
use ldk_server_client::ldk_server_grpc::events::{event_envelope, EventEnvelope};
use ldk_server_client::ldk_server_grpc::types::{
    bolt11_invoice_description, payment_kind, Bolt11InvoiceDescription, PageToken, Payment,
    PaymentDirection, PaymentStatus, RouteParametersConfig,
};
use lightning::offers::offer::Amount as OfferAmount;
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

use crate::error::Error;
use crate::settings::AdvertisedMethod;

const DEFAULT_INVOICE_EXPIRY_SECS: u32 = 36_000;
const DEFAULT_PAYMENT_WAIT_SECS: u64 = 10;
const OUTGOING_CONTEXT_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const UNMATCHED_OUTGOING_EVENT_TTL: Duration = Duration::from_secs(60);
#[allow(dead_code)]
const DEFAULT_MAX_PAYMENT_SCAN_PAGES: u16 = 32;
const ROUTE_DEFAULT_MAX_TOTAL_CLTV_EXPIRY_DELTA: u32 = 1008;
const ROUTE_DEFAULT_MAX_PATH_COUNT: u32 = 10;
const ROUTE_DEFAULT_MAX_CHANNEL_SATURATION_POWER_OF_HALF: u32 = 2;

#[derive(Clone, Debug, PartialEq, Eq)]
struct OutgoingPaymentContext {
    quote_id: QuoteId,
    unit: CurrencyUnit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutgoingPaymentOutcome {
    Succeeded,
    Failed,
}

#[derive(Clone)]
struct PendingOutgoingContext {
    context: OutgoingPaymentContext,
    inserted_at: Instant,
}

struct UnmatchedOutgoingPayment {
    keys: Vec<String>,
    payment: Payment,
    outcome: OutgoingPaymentOutcome,
    received_at: Instant,
}

struct OutgoingPaymentCorrelatorState {
    by_key: HashMap<String, PendingOutgoingContext>,
    unmatched: VecDeque<UnmatchedOutgoingPayment>,
    ready: VecDeque<Event>,
    finalized: HashMap<QuoteId, Instant>,
}

impl OutgoingPaymentCorrelatorState {
    fn new() -> Self {
        Self {
            by_key: HashMap::new(),
            unmatched: VecDeque::new(),
            ready: VecDeque::new(),
            finalized: HashMap::new(),
        }
    }
}

#[derive(Clone)]
struct OutgoingPaymentCorrelator {
    state: Arc<Mutex<OutgoingPaymentCorrelatorState>>,
    notify: Arc<Notify>,
}

impl OutgoingPaymentCorrelator {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(OutgoingPaymentCorrelatorState::new())),
            notify: Arc::new(Notify::new()),
        }
    }
}

type OutgoingPaymentCorrelations = OutgoingPaymentCorrelator;

/// LDK Server backend configuration.
#[derive(Clone)]
pub struct Config {
    /// Server address without scheme, for example `127.0.0.1:3536`.
    pub address: String,
    /// HMAC API key expected by LDK Server.
    pub api_key: String,
    /// PEM encoded TLS certificate to pin.
    pub cert_pem: Vec<u8>,
    /// Fee reserve used for melt quotes.
    pub fee_reserve: FeeReserve,
    /// Maximum `ListPayments` pages to scan for incoming status lookups.
    pub max_payment_scan_pages: u16,
    /// Rails this backend reports in `get_settings`, and therefore the only ones
    /// a mint will register it for.
    pub advertised_methods: Vec<AdvertisedMethod>,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("address", &self.address)
            .field("api_key", &"[REDACTED]")
            .field("cert_pem", &format!("{} bytes", self.cert_pem.len()))
            .field("fee_reserve", &self.fee_reserve)
            .field("max_payment_scan_pages", &self.max_payment_scan_pages)
            .finish()
    }
}

#[allow(dead_code)]
impl Config {
    /// Create a new LDK Server backend configuration.
    pub fn new(
        address: String,
        api_key: String,
        cert_pem: Vec<u8>,
        fee_reserve: FeeReserve,
    ) -> Self {
        Self {
            address,
            api_key,
            cert_pem,
            fee_reserve,
            max_payment_scan_pages: DEFAULT_MAX_PAYMENT_SCAN_PAGES,
            advertised_methods: AdvertisedMethod::ALL.to_vec(),
        }
    }

    /// Restrict the methods this backend advertises to the mint.
    pub fn with_advertised_methods(mut self, methods: Vec<AdvertisedMethod>) -> Self {
        self.advertised_methods = methods;
        self
    }

    /// Set the maximum number of payment-history pages scanned for incoming payment status.
    pub fn with_max_payment_scan_pages(mut self, max_payment_scan_pages: u16) -> Self {
        self.max_payment_scan_pages = max_payment_scan_pages;
        self
    }
}

/// LDK Server payment backend.
#[derive(Clone)]
pub struct LdkServerBackend {
    client: LdkServerClient,
    fee_reserve: FeeReserve,
    wait_invoice_cancel_token: CancellationToken,
    wait_invoice_is_active: Arc<AtomicBool>,
    outgoing_payment_correlations: OutgoingPaymentCorrelations,
    max_payment_scan_pages: u16,
    advertised_methods: Vec<AdvertisedMethod>,
}

impl std::fmt::Debug for LdkServerBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LdkServerBackend")
            .field("fee_reserve", &self.fee_reserve)
            .field("max_payment_scan_pages", &self.max_payment_scan_pages)
            .finish_non_exhaustive()
    }
}

/// Build the settings a mint reads to decide which rails to register this
/// backend for.
///
/// A mint registers a backend per `(unit, method)` pair and claims every method
/// the backend advertises here, so restricting `methods` is what allows this
/// processor to sit alongside another one instead of competing with it.
fn settings_response(methods: &[AdvertisedMethod]) -> SettingsResponse {
    let advertises = |method: AdvertisedMethod| methods.contains(&method);

    SettingsResponse {
        unit: CurrencyUnit::Msat.to_string(),
        bolt11: advertises(AdvertisedMethod::Bolt11).then_some(payment::Bolt11Settings {
            mpp: false,
            amountless: true,
            invoice_description: true,
        }),
        bolt12: advertises(AdvertisedMethod::Bolt12).then_some(payment::Bolt12Settings {
            amountless: true,
            invoice_description: true,
        }),
        onchain: None,
        custom: std::collections::HashMap::new(),
    }
}

impl LdkServerBackend {
    /// Create a new LDK Server backend.
    ///
    /// # Errors
    ///
    /// Returns an error if the TLS certificate cannot be parsed or the client cannot be built.
    pub fn new(config: Config) -> Result<Self, Error> {
        let address = normalize_address(&config.address);
        let client = LdkServerClient::new(address, config.api_key, &config.cert_pem)
            .map_err(Error::Client)?;

        Ok(Self {
            client,
            fee_reserve: config.fee_reserve,
            wait_invoice_cancel_token: CancellationToken::new(),
            wait_invoice_is_active: Arc::new(AtomicBool::new(false)),
            outgoing_payment_correlations: OutgoingPaymentCorrelator::new(),
            max_payment_scan_pages: config.max_payment_scan_pages,
            advertised_methods: config.advertised_methods,
        })
    }

    async fn get_payment_details(&self, payment_id: &str) -> Result<Option<Payment>, Error> {
        let response = self
            .client
            .get_payment_details(GetPaymentDetailsRequest {
                payment_id: payment_id.to_string(),
            })
            .await?;

        Ok(response.payment)
    }

    async fn list_matching_payments<F>(&self, mut matches_payment: F) -> Result<Vec<Payment>, Error>
    where
        F: FnMut(&Payment) -> bool,
    {
        let mut page_token: Option<PageToken> = None;
        let mut payments = Vec::new();

        for _ in 0..self.max_payment_scan_pages {
            let response = self
                .client
                .list_payments(ListPaymentsRequest { page_token })
                .await?;

            payments.extend(response.payments.into_iter().filter(|p| matches_payment(p)));

            match response.next_page_token {
                Some(next_page_token) => page_token = Some(next_page_token),
                None => return Ok(payments),
            }
        }

        Err(Error::PaymentScanLimitExceeded {
            max_pages: self.max_payment_scan_pages,
        })
    }

    async fn wait_for_payment_details(
        &self,
        payment_id: &str,
        timeout_secs: Option<u64>,
    ) -> Result<Payment, Error> {
        let timeout = Duration::from_secs(timeout_secs.unwrap_or(DEFAULT_PAYMENT_WAIT_SECS));
        let start = std::time::Instant::now();

        loop {
            match self.get_payment_details(payment_id).await? {
                Some(payment) => return Ok(payment),
                None if start.elapsed() >= timeout => return Err(Error::PaymentNotFound),
                None => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    }

    fn make_payment_response_from_payment(
        unit: &CurrencyUnit,
        payment_lookup_id: PaymentIdentifier,
        payment: &Payment,
    ) -> Result<MakePaymentResponse, payment::Error> {
        let status = payment_status(payment)?;
        let status = match status {
            PaymentStatus::Pending => MeltQuoteState::Pending,
            PaymentStatus::Succeeded => MeltQuoteState::Paid,
            PaymentStatus::Failed => MeltQuoteState::Failed,
        };

        let payment_proof = match payment_kind(payment)? {
            payment_kind::Kind::Bolt11(bolt11) => bolt11.preimage.clone(),
            payment_kind::Kind::Bolt12Offer(bolt12) => bolt12.preimage.clone(),
            _ => return Err(Error::UnexpectedPaymentKind.into()),
        };

        let total_spent = if status == MeltQuoteState::Paid {
            let total_spent = payment
                .amount_msat
                .ok_or(Error::CouldNotGetAmountSpent)?
                .checked_add(payment.fee_paid_msat.unwrap_or_default())
                .ok_or(Error::AmountOverflow)?;
            msat_total_spent_for_unit(total_spent, unit)?
        } else {
            Amount::new(0, unit.clone())
        };

        Ok(MakePaymentResponse {
            payment_lookup_id,
            payment_proof,
            status,
            total_spent,
        })
    }

    fn wait_payment_response_from_payment(
        payment: &Payment,
    ) -> Result<Option<WaitPaymentResponse>, Error> {
        if payment_direction(payment)? != PaymentDirection::Inbound {
            return Ok(None);
        }

        if payment_status(payment)? != PaymentStatus::Succeeded {
            return Ok(None);
        }

        let payment_amount = Amount::new(
            payment.amount_msat.ok_or(Error::CouldNotGetPaymentAmount)?,
            CurrencyUnit::Msat,
        );

        let (payment_identifier, payment_id) = match payment_kind(payment)? {
            payment_kind::Kind::Bolt11(bolt11) => (
                PaymentIdentifier::PaymentHash(hex_to_array(&bolt11.hash)?),
                bolt11.hash.clone(),
            ),
            payment_kind::Kind::Bolt12Offer(bolt12) => (
                PaymentIdentifier::OfferId(bolt12.offer_id.clone()),
                bolt12.hash.clone().unwrap_or_else(|| payment.id.clone()),
            ),
            _ => return Ok(None),
        };

        Ok(Some(WaitPaymentResponse {
            payment_identifier,
            payment_amount,
            payment_id,
        }))
    }

    async fn event_from_envelope(
        envelope: EventEnvelope,
        correlations: &OutgoingPaymentCorrelations,
    ) -> Result<Option<Event>, payment::Error> {
        let event = match envelope.event {
            Some(event) => event,
            None => return Ok(None),
        };

        match event {
            event_envelope::Event::PaymentReceived(payment_received) => {
                let payment = match payment_received.payment {
                    Some(payment) => payment,
                    None => return Ok(None),
                };

                Ok(Self::wait_payment_response_from_payment(&payment)?.map(Event::PaymentReceived))
            }
            event_envelope::Event::PaymentSuccessful(payment_successful) => {
                let Some(payment) = payment_successful.payment else {
                    return Ok(None);
                };
                take_correlated_outgoing_event(
                    correlations,
                    payment,
                    OutgoingPaymentOutcome::Succeeded,
                )
                .await
            }
            event_envelope::Event::PaymentFailed(payment_failed) => {
                let Some(payment) = payment_failed.payment else {
                    return Ok(None);
                };
                take_correlated_outgoing_event(
                    correlations,
                    payment,
                    OutgoingPaymentOutcome::Failed,
                )
                .await
            }
            event_envelope::Event::PaymentForwarded(_)
            | event_envelope::Event::PaymentClaimable(_) => Ok(None),
        }
    }
}

#[async_trait]
impl MintPayment for LdkServerBackend {
    type Err = payment::Error;

    async fn start(&self) -> Result<(), Self::Err> {
        self.client
            .get_node_info(GetNodeInfoRequest {})
            .await
            .map_err(Error::from)?;
        Ok(())
    }

    async fn get_settings(&self) -> Result<SettingsResponse, Self::Err> {
        Ok(settings_response(&self.advertised_methods))
    }

    async fn create_incoming_payment_request(
        &self,
        options: IncomingPaymentOptions,
    ) -> Result<CreateIncomingPaymentResponse, Self::Err> {
        match options {
            IncomingPaymentOptions::Bolt11(bolt11_options) => {
                let amount_msat = bolt11_options
                    .amount
                    .convert_to(&CurrencyUnit::Msat)?
                    .value();
                let expiry_secs = expiry_secs_or_default(bolt11_options.unix_expiry)?;
                let response = self
                    .client
                    .bolt11_receive(Bolt11ReceiveRequest {
                        amount_msat: Some(amount_msat),
                        description: Some(Bolt11InvoiceDescription {
                            kind: Some(bolt11_invoice_description::Kind::Direct(
                                bolt11_options.description.unwrap_or_default(),
                            )),
                        }),
                        expiry_secs,
                    })
                    .await
                    .map_err(Error::from)?;

                Ok(CreateIncomingPaymentResponse {
                    request_lookup_id: PaymentIdentifier::PaymentHash(hex_to_array(
                        &response.payment_hash,
                    )?),
                    request: response.invoice,
                    expiry: Some(unix_time() + u64::from(expiry_secs)),
                    extra_json: None,
                })
            }
            IncomingPaymentOptions::Bolt12(bolt12_options) => {
                let amount_msat = bolt12_options
                    .amount
                    .map(|amount| amount.convert_to(&CurrencyUnit::Msat).map(|a| a.value()))
                    .transpose()?;
                let expiry_secs = bolt12_options.unix_expiry.map(expiry_secs).transpose()?;
                let response = self
                    .client
                    .bolt12_receive(Bolt12ReceiveRequest {
                        description: bolt12_options.description.unwrap_or_default(),
                        amount_msat,
                        expiry_secs,
                        quantity: None,
                    })
                    .await
                    .map_err(Error::from)?;

                Ok(CreateIncomingPaymentResponse {
                    request_lookup_id: PaymentIdentifier::OfferId(response.offer_id),
                    request: response.offer,
                    expiry: bolt12_options.unix_expiry,
                    extra_json: None,
                })
            }
            IncomingPaymentOptions::Custom(_) | IncomingPaymentOptions::Onchain(_) => {
                Err(payment::Error::UnsupportedPaymentOption)
            }
        }
    }

    async fn get_payment_quote(
        &self,
        unit: &CurrencyUnit,
        options: OutgoingPaymentOptions,
    ) -> Result<PaymentQuoteResponse, Self::Err> {
        match options {
            OutgoingPaymentOptions::Bolt11(bolt11_options) => {
                let amount_msat = bolt11_amount_msat_for_quote(&bolt11_options)?;
                let amount = Amount::new(amount_msat, CurrencyUnit::Msat).convert_to(unit)?;
                let fee = self.fee_reserve_for_amount(unit, amount.value());

                Ok(PaymentQuoteResponse {
                    request_lookup_id: Some(PaymentIdentifier::PaymentHash(
                        *bolt11_options.bolt11.payment_hash().as_byte_array(),
                    )),
                    amount,
                    fee,
                    state: MeltQuoteState::Unpaid,
                    extra_json: None,
                    estimated_blocks: None,
                    fee_options: None,
                })
            }
            OutgoingPaymentOptions::Bolt12(bolt12_options) => {
                let amount_msat = bolt12_amount_msat_for_quote(&bolt12_options)?;
                let amount = Amount::new(amount_msat, CurrencyUnit::Msat).convert_to(unit)?;
                let fee = self.fee_reserve_for_amount(unit, amount.value());

                Ok(PaymentQuoteResponse {
                    request_lookup_id: None,
                    amount,
                    fee,
                    state: MeltQuoteState::Unpaid,
                    extra_json: None,
                    estimated_blocks: None,
                    fee_options: None,
                })
            }
            OutgoingPaymentOptions::Custom(_) | OutgoingPaymentOptions::Onchain(_) => {
                Err(payment::Error::UnsupportedPaymentOption)
            }
        }
    }

    async fn make_payment(
        &self,
        unit: &CurrencyUnit,
        options: OutgoingPaymentOptions,
    ) -> Result<MakePaymentResponse, Self::Err> {
        match options {
            OutgoingPaymentOptions::Bolt11(bolt11_options) => {
                let amount_msat = bolt11_amount_msat_for_send(&bolt11_options)?;
                let route_parameters =
                    route_parameters_from_max_fee(bolt11_options.max_fee_amount.clone())?;
                let context = OutgoingPaymentContext {
                    quote_id: bolt11_options.quote_id.clone(),
                    unit: unit.clone(),
                };
                let payment_hash =
                    hex::encode(bolt11_options.bolt11.payment_hash().as_byte_array());
                remember_outgoing_payment_context(
                    &self.outgoing_payment_correlations,
                    &payment_hash,
                    context.clone(),
                )
                .await;

                let response = self
                    .client
                    .bolt11_send(Bolt11SendRequest {
                        invoice: bolt11_options.bolt11.to_string(),
                        amount_msat,
                        route_parameters,
                    })
                    .await
                    .map_err(Error::from)?;
                remember_outgoing_payment_context(
                    &self.outgoing_payment_correlations,
                    &response.payment_id,
                    context.clone(),
                )
                .await;
                let payment_id = hex_to_array(&response.payment_id)?;
                let payment = self
                    .wait_for_payment_details(&response.payment_id, bolt11_options.timeout_secs)
                    .await?;
                remember_outgoing_payment_keys(
                    &self.outgoing_payment_correlations,
                    &payment,
                    context.clone(),
                )
                .await;

                let payment_response = Self::make_payment_response_from_payment(
                    unit,
                    PaymentIdentifier::PaymentId(payment_id),
                    &payment,
                )?;
                if matches!(
                    payment_response.status,
                    MeltQuoteState::Paid | MeltQuoteState::Failed
                ) {
                    remove_outgoing_payment_context(
                        &self.outgoing_payment_correlations,
                        &context.quote_id,
                    )
                    .await;
                }
                Ok(payment_response)
            }
            OutgoingPaymentOptions::Bolt12(bolt12_options) => {
                let amount_msat = bolt12_amount_msat_for_send(&bolt12_options)?;
                let route_parameters =
                    route_parameters_from_max_fee(bolt12_options.max_fee_amount.clone())?;
                let context = OutgoingPaymentContext {
                    quote_id: bolt12_options.quote_id.clone(),
                    unit: unit.clone(),
                };
                let response = self
                    .client
                    .bolt12_send(Bolt12SendRequest {
                        offer: bolt12_options.offer.to_string(),
                        amount_msat,
                        quantity: None,
                        payer_note: None,
                        route_parameters,
                    })
                    .await
                    .map_err(Error::from)?;
                remember_outgoing_payment_context(
                    &self.outgoing_payment_correlations,
                    &response.payment_id,
                    context.clone(),
                )
                .await;
                let payment_id = hex_to_array(&response.payment_id)?;
                let payment = self
                    .wait_for_payment_details(&response.payment_id, bolt12_options.timeout_secs)
                    .await?;
                remember_outgoing_payment_keys(
                    &self.outgoing_payment_correlations,
                    &payment,
                    context.clone(),
                )
                .await;

                let payment_response = Self::make_payment_response_from_payment(
                    unit,
                    PaymentIdentifier::PaymentId(payment_id),
                    &payment,
                )?;
                if matches!(
                    payment_response.status,
                    MeltQuoteState::Paid | MeltQuoteState::Failed
                ) {
                    remove_outgoing_payment_context(
                        &self.outgoing_payment_correlations,
                        &context.quote_id,
                    )
                    .await;
                }
                Ok(payment_response)
            }
            OutgoingPaymentOptions::Custom(_) | OutgoingPaymentOptions::Onchain(_) => {
                Err(payment::Error::UnsupportedPaymentOption)
            }
        }
    }

    async fn wait_payment_event(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Event> + Send>>, Self::Err> {
        let stream = self.client.subscribe_events().await.map_err(Error::from)?;
        self.wait_invoice_is_active.store(true, Ordering::SeqCst);

        let state = EventStreamState {
            client: self.client.clone(),
            cancel_token: self.wait_invoice_cancel_token.clone(),
            is_active: Arc::clone(&self.wait_invoice_is_active),
            outgoing_payment_correlations: self.outgoing_payment_correlations.clone(),
            stream: Some(stream),
            retry_count: 0,
        };

        Ok(Box::pin(futures::stream::unfold(
            state,
            |mut state| async move {
                state.is_active.store(true, Ordering::SeqCst);

                loop {
                    if let Some(event) =
                        take_ready_outgoing_event(&state.outgoing_payment_correlations).await
                    {
                        return Some((event, state));
                    }

                    if state.stream.is_none() {
                        match state.client.subscribe_events().await {
                            Ok(stream) => {
                                state.stream = Some(stream);
                                state.retry_count = 0;
                            }
                            Err(err) => {
                                tracing::warn!("Failed to subscribe to LDK Server events: {}", err);
                                if !sleep_or_cancel(&state.cancel_token, state.retry_delay()).await
                                {
                                    state.is_active.store(false, Ordering::SeqCst);
                                    return None;
                                }
                                state.retry_count = state.retry_count.saturating_add(1);
                                continue;
                            }
                        }
                    }

                    let mut stream = match state.stream.take() {
                        Some(stream) => stream,
                        None => continue,
                    };

                    tokio::select! {
                        _ = state.cancel_token.cancelled() => {
                            state.is_active.store(false, Ordering::SeqCst);
                            return None;
                        }
                        _ = state.outgoing_payment_correlations.notify.notified() => {
                            state.stream = Some(stream);
                            continue;
                        }
                        message = stream.next_message() => {
                            state.stream = Some(stream);

                            match message {
                                Some(Ok(envelope)) => match Self::event_from_envelope(
                                    envelope,
                                    &state.outgoing_payment_correlations,
                                ).await {
                                    Ok(Some(event)) => return Some((event, state)),
                                    Ok(None) => continue,
                                    Err(err) => {
                                        tracing::warn!("Could not map LDK Server event: {}", err);
                                        continue;
                                    }
                                },
                                Some(Err(err)) => {
                                    tracing::warn!("LDK Server event stream error: {}", err);
                                    state.stream = None;
                                    if !sleep_or_cancel(&state.cancel_token, state.retry_delay()).await {
                                        state.is_active.store(false, Ordering::SeqCst);
                                        return None;
                                    }
                                    state.retry_count = state.retry_count.saturating_add(1);
                                    continue;
                                }
                                None => {
                                    tracing::warn!("LDK Server event stream ended");
                                    state.stream = None;
                                    if !sleep_or_cancel(&state.cancel_token, state.retry_delay()).await {
                                        state.is_active.store(false, Ordering::SeqCst);
                                        return None;
                                    }
                                    state.retry_count = state.retry_count.saturating_add(1);
                                    continue;
                                }
                            }
                        }
                    }
                }
            },
        )))
    }

    fn is_payment_event_stream_active(&self) -> bool {
        self.wait_invoice_is_active.load(Ordering::SeqCst)
    }

    fn cancel_payment_event_stream(&self) {
        self.wait_invoice_cancel_token.cancel();
    }

    async fn check_incoming_payment_status(
        &self,
        payment_identifier: &PaymentIdentifier,
    ) -> Result<Vec<WaitPaymentResponse>, Self::Err> {
        match payment_identifier {
            PaymentIdentifier::OfferId(offer_id) => {
                let payments = self
                    .list_matching_payments(|payment| {
                        matches!(payment_direction(payment), Ok(PaymentDirection::Inbound))
                            && matches!(payment_status(payment), Ok(PaymentStatus::Succeeded))
                            && matches!(
                                payment_kind(payment),
                                Ok(payment_kind::Kind::Bolt12Offer(bolt12))
                                    if bolt12.offer_id == *offer_id
                            )
                    })
                    .await?;

                Ok(payments
                    .iter()
                    .filter_map(
                        |payment| match Self::wait_payment_response_from_payment(payment) {
                            Ok(response) => response,
                            Err(err) => {
                                tracing::warn!(
                                    "Could not map LDK Server incoming payment: {}",
                                    err
                                );
                                None
                            }
                        },
                    )
                    .collect())
            }
            PaymentIdentifier::PaymentHash(hash) => {
                let hash = hex::encode(hash);
                let payments = self
                    .list_matching_payments(|payment| {
                        matches!(payment_direction(payment), Ok(PaymentDirection::Inbound))
                            && matches!(payment_status(payment), Ok(PaymentStatus::Succeeded))
                            && matches!(
                                payment_kind(payment),
                                Ok(payment_kind::Kind::Bolt11(bolt11))
                                    if bolt11.hash.eq_ignore_ascii_case(&hash)
                            )
                    })
                    .await?;

                Ok(payments
                    .iter()
                    .filter_map(
                        |payment| match Self::wait_payment_response_from_payment(payment) {
                            Ok(response) => response,
                            Err(err) => {
                                tracing::warn!(
                                    "Could not map LDK Server incoming payment: {}",
                                    err
                                );
                                None
                            }
                        },
                    )
                    .collect())
            }
            PaymentIdentifier::PaymentId(payment_id) => {
                let payment_id = hex::encode(payment_id);
                let payment = match self.get_payment_details(&payment_id).await? {
                    Some(payment) => payment,
                    None => return Ok(vec![]),
                };

                Ok(Self::wait_payment_response_from_payment(&payment)?
                    .into_iter()
                    .collect())
            }
            _ => Err(Error::UnsupportedPaymentIdentifierType.into()),
        }
    }

    async fn check_outgoing_payment(
        &self,
        payment_identifier: &PaymentIdentifier,
    ) -> Result<MakePaymentResponse, Self::Err> {
        match payment_identifier {
            PaymentIdentifier::PaymentId(payment_id) => {
                let payment_id = hex::encode(payment_id);
                let payment = self
                    .get_payment_details(&payment_id)
                    .await?
                    .ok_or(Error::PaymentNotFound)?;

                if payment_direction(&payment)? != PaymentDirection::Outbound {
                    return Err(Error::InvalidPaymentDirection.into());
                }

                Self::make_payment_response_from_payment(
                    &CurrencyUnit::Msat,
                    payment_identifier.clone(),
                    &payment,
                )
            }
            PaymentIdentifier::PaymentHash(hash) => {
                let hash = hex::encode(hash);
                let payments = self
                    .list_matching_payments(|payment| {
                        matches!(payment_direction(payment), Ok(PaymentDirection::Outbound))
                            && matches!(
                                payment_kind(payment),
                                Ok(payment_kind::Kind::Bolt11(bolt11))
                                    if bolt11.hash.eq_ignore_ascii_case(&hash)
                            )
                    })
                    .await?;
                let payment = select_bolt11_payment(payments).ok_or(Error::PaymentNotFound)?;

                Self::make_payment_response_from_payment(
                    &CurrencyUnit::Msat,
                    payment_identifier.clone(),
                    &payment,
                )
            }
            _ => Ok(MakePaymentResponse {
                payment_lookup_id: payment_identifier.clone(),
                payment_proof: None,
                status: MeltQuoteState::Unknown,
                total_spent: Amount::new(0, CurrencyUnit::Msat),
            }),
        }
    }
}

impl LdkServerBackend {
    fn fee_reserve_for_amount(&self, unit: &CurrencyUnit, amount: u64) -> Amount<CurrencyUnit> {
        let relative_fee_reserve = (self.fee_reserve.percent_fee_reserve * amount as f32) as u64;
        let absolute_fee_reserve: u64 = self.fee_reserve.min_fee_reserve.into();
        Amount::new(relative_fee_reserve.max(absolute_fee_reserve), unit.clone())
    }
}

impl Drop for LdkServerBackend {
    fn drop(&mut self) {
        self.wait_invoice_cancel_token.cancel();
    }
}

struct EventStreamState {
    client: LdkServerClient,
    cancel_token: CancellationToken,
    is_active: Arc<AtomicBool>,
    outgoing_payment_correlations: OutgoingPaymentCorrelations,
    stream: Option<EventStream>,
    retry_count: u32,
}

impl EventStreamState {
    fn retry_delay(&self) -> Duration {
        Duration::from_secs(2_u64.saturating_pow(self.retry_count).min(10))
    }
}

async fn remember_outgoing_payment_keys(
    correlations: &OutgoingPaymentCorrelations,
    payment: &Payment,
    context: OutgoingPaymentContext,
) {
    remember_outgoing_payment_context_keys(
        correlations,
        outgoing_payment_correlation_keys(payment),
        context,
    )
    .await;
}

async fn remember_outgoing_payment_context(
    correlations: &OutgoingPaymentCorrelations,
    payment_id: &str,
    context: OutgoingPaymentContext,
) {
    remember_outgoing_payment_context_keys(
        correlations,
        vec![normalize_payment_correlation_key(payment_id)],
        context,
    )
    .await;
}

async fn remember_outgoing_payment_context_keys(
    correlations: &OutgoingPaymentCorrelations,
    keys: Vec<String>,
    context: OutgoingPaymentContext,
) {
    let mut matched = false;
    {
        let mut state = correlations.state.lock().await;
        expire_correlator_state(&mut state, Instant::now());
        if state.finalized.contains_key(&context.quote_id) {
            drop_unmatched_for_keys(&mut state, &keys);
        } else if let Some(index) = state.unmatched.iter().position(|unmatched| {
            unmatched
                .keys
                .iter()
                .any(|existing| keys.iter().any(|key| key == existing))
        }) {
            let unmatched = state
                .unmatched
                .remove(index)
                .expect("unmatched outgoing payment index exists");
            match map_outgoing_payment_event(&context, &unmatched.payment, unmatched.outcome) {
                Ok(event) => {
                    finalize_quote(&mut state, &context.quote_id);
                    state.ready.push_back(event);
                    matched = true;
                }
                Err(err) => {
                    tracing::warn!(
                        "Could not map buffered LDK Server outgoing payment {}: {}",
                        unmatched.payment.id,
                        err
                    );
                    insert_outgoing_context_keys(&mut state, &keys, context);
                }
            }
        } else {
            insert_outgoing_context_keys(&mut state, &keys, context);
        }
    }
    if matched {
        correlations.notify.notify_one();
    }
}

async fn remove_outgoing_payment_context(
    correlations: &OutgoingPaymentCorrelations,
    quote_id: &QuoteId,
) {
    let mut state = correlations.state.lock().await;
    expire_correlator_state(&mut state, Instant::now());
    finalize_quote(&mut state, quote_id);
}

async fn take_correlated_outgoing_event(
    correlations: &OutgoingPaymentCorrelations,
    payment: Payment,
    outcome: OutgoingPaymentOutcome,
) -> Result<Option<Event>, payment::Error> {
    let mut state = correlations.state.lock().await;
    expire_correlator_state(&mut state, Instant::now());
    let keys = outgoing_payment_correlation_keys(&payment);
    if let Some(context) = keys
        .iter()
        .find_map(|key| state.by_key.get(key).map(|pending| pending.context.clone()))
    {
        finalize_quote(&mut state, &context.quote_id);
        return map_outgoing_payment_event(&context, &payment, outcome).map(Some);
    }

    tracing::debug!(
        "Buffering uncorrelated LDK Server outgoing payment {}: {:?}",
        payment.id,
        outcome
    );
    state.unmatched.push_back(UnmatchedOutgoingPayment {
        keys,
        payment,
        outcome,
        received_at: Instant::now(),
    });
    Ok(None)
}

async fn take_ready_outgoing_event(correlations: &OutgoingPaymentCorrelations) -> Option<Event> {
    let mut state = correlations.state.lock().await;
    expire_correlator_state(&mut state, Instant::now());
    state.ready.pop_front()
}

fn insert_outgoing_context_keys(
    state: &mut OutgoingPaymentCorrelatorState,
    keys: &[String],
    context: OutgoingPaymentContext,
) {
    let pending = PendingOutgoingContext {
        context,
        inserted_at: Instant::now(),
    };
    for key in keys {
        state.by_key.insert(key.clone(), pending.clone());
    }
}

fn remove_quote_keys(state: &mut OutgoingPaymentCorrelatorState, quote_id: &QuoteId) {
    state
        .by_key
        .retain(|_, pending| &pending.context.quote_id != quote_id);
}

fn finalize_quote(state: &mut OutgoingPaymentCorrelatorState, quote_id: &QuoteId) {
    remove_quote_keys(state, quote_id);
    state.finalized.insert(quote_id.clone(), Instant::now());
}

fn drop_unmatched_for_keys(state: &mut OutgoingPaymentCorrelatorState, keys: &[String]) {
    state.unmatched.retain(|unmatched| {
        !unmatched
            .keys
            .iter()
            .any(|existing| keys.iter().any(|key| key == existing))
    });
}

fn expire_correlator_state(state: &mut OutgoingPaymentCorrelatorState, now: Instant) {
    state.by_key.retain(|_, pending| {
        now.saturating_duration_since(pending.inserted_at) < OUTGOING_CONTEXT_TTL
    });
    state.finalized.retain(|_, finalized_at| {
        now.saturating_duration_since(*finalized_at) < OUTGOING_CONTEXT_TTL
    });
    while state.unmatched.front().is_some_and(|unmatched| {
        now.saturating_duration_since(unmatched.received_at) >= UNMATCHED_OUTGOING_EVENT_TTL
    }) {
        if let Some(unmatched) = state.unmatched.pop_front() {
            tracing::warn!(
                "Dropping uncorrelated LDK Server outgoing payment {} after {:?}",
                unmatched.payment.id,
                UNMATCHED_OUTGOING_EVENT_TTL
            );
        }
    }
}

fn map_outgoing_payment_event(
    context: &OutgoingPaymentContext,
    payment: &Payment,
    outcome: OutgoingPaymentOutcome,
) -> Result<Event, payment::Error> {
    match outcome {
        OutgoingPaymentOutcome::Succeeded => {
            let payment_lookup_id = PaymentIdentifier::PaymentId(hex_to_array(&payment.id)?);
            let details = LdkServerBackend::make_payment_response_from_payment(
                &context.unit,
                payment_lookup_id,
                payment,
            )?;
            tracing::debug!(
                "LDK Server outgoing payment succeeded for quote {}: {}",
                context.quote_id,
                payment.id
            );
            Ok(Event::PaymentSuccessful {
                quote_id: context.quote_id.clone(),
                details,
            })
        }
        OutgoingPaymentOutcome::Failed => {
            let reason = format!("LDK Server payment {} failed", payment.id);
            tracing::warn!(
                "LDK Server outgoing payment failed for quote {}: {}",
                context.quote_id,
                payment.id
            );
            Ok(Event::PaymentFailed {
                quote_id: context.quote_id.clone(),
                reason,
            })
        }
    }
}

fn outgoing_payment_correlation_keys(payment: &Payment) -> Vec<String> {
    let mut keys = vec![normalize_payment_correlation_key(&payment.id)];

    if let Some(kind) = payment.kind.as_ref().and_then(|kind| kind.kind.as_ref()) {
        match kind {
            payment_kind::Kind::Bolt11(bolt11) => {
                keys.push(normalize_payment_correlation_key(&bolt11.hash));
            }
            payment_kind::Kind::Bolt12Offer(bolt12) => {
                if let Some(hash) = &bolt12.hash {
                    keys.push(normalize_payment_correlation_key(hash));
                }
            }
            _ => {}
        }
    }

    keys.sort_unstable();
    keys.dedup();
    keys
}

fn normalize_payment_correlation_key(payment_id: &str) -> String {
    payment_id.trim().to_ascii_lowercase()
}

fn normalize_address(address: &str) -> String {
    address
        .trim()
        .strip_prefix("https://")
        .or_else(|| address.trim().strip_prefix("http://"))
        .unwrap_or_else(|| address.trim())
        .trim_end_matches('/')
        .to_string()
}

fn expiry_secs(unix_expiry: u64) -> Result<u32, payment::Error> {
    let seconds = unix_expiry
        .checked_sub(unix_time())
        .ok_or(payment::Error::InvalidExpiry)?;
    seconds
        .try_into()
        .map_err(|_| payment::Error::InvalidExpiry)
}

fn expiry_secs_or_default(unix_expiry: Option<u64>) -> Result<u32, payment::Error> {
    match unix_expiry {
        Some(unix_expiry) => expiry_secs(unix_expiry),
        None => Ok(DEFAULT_INVOICE_EXPIRY_SECS),
    }
}

fn hex_to_array(hex_value: &str) -> Result<[u8; 32], Error> {
    hex::decode(hex_value)?
        .try_into()
        .map_err(|_| Error::InvalidPaymentId)
}

fn payment_kind(payment: &Payment) -> Result<&payment_kind::Kind, Error> {
    payment
        .kind
        .as_ref()
        .and_then(|kind| kind.kind.as_ref())
        .ok_or(Error::MissingPaymentKind)
}

fn payment_direction(payment: &Payment) -> Result<PaymentDirection, Error> {
    PaymentDirection::from_i32(payment.direction)
        .ok_or(Error::UnknownPaymentDirection(payment.direction))
}

fn payment_status(payment: &Payment) -> Result<PaymentStatus, Error> {
    PaymentStatus::from_i32(payment.status).ok_or(Error::UnknownPaymentStatus(payment.status))
}

fn bolt11_amount_msat_for_quote(
    options: &cdk_common::payment::Bolt11OutgoingPaymentOptions,
) -> Result<u64, payment::Error> {
    match &options.melt_options {
        Some(MeltOptions::Amountless { amountless }) => {
            let amount_msat = u64::from(amountless.amount_msat);

            if let Some(invoice_amount) = options.bolt11.amount_milli_satoshis() {
                if invoice_amount != amount_msat {
                    return Err(payment::Error::AmountMismatch);
                }
            }

            Ok(amount_msat)
        }
        Some(MeltOptions::Mpp { mpp }) => Ok(mpp.amount.into()),
        None => options
            .bolt11
            .amount_milli_satoshis()
            .ok_or(Error::UnknownInvoiceAmount.into()),
    }
}

fn bolt11_amount_msat_for_send(
    options: &cdk_common::payment::Bolt11OutgoingPaymentOptions,
) -> Result<Option<u64>, payment::Error> {
    match &options.melt_options {
        Some(MeltOptions::Amountless { amountless }) => {
            let amount_msat = u64::from(amountless.amount_msat);

            if let Some(invoice_amount) = options.bolt11.amount_milli_satoshis() {
                if invoice_amount != amount_msat {
                    return Err(payment::Error::AmountMismatch);
                }
            }

            Ok(Some(amount_msat))
        }
        Some(MeltOptions::Mpp { mpp: _ }) => Err(payment::Error::UnsupportedPaymentOption),
        None => Ok(None),
    }
}

fn bolt12_amount_msat_for_quote(
    options: &cdk_common::payment::Bolt12OutgoingPaymentOptions,
) -> Result<u64, payment::Error> {
    match bolt12_melt_options_amount_msat(options)? {
        Some(amount_msat) => Ok(amount_msat),
        None => {
            let amount = options
                .offer
                .amount()
                .ok_or(payment::Error::AmountMismatch)?;

            match amount {
                OfferAmount::Bitcoin { amount_msats } => Ok(amount_msats),
                _ => Err(payment::Error::AmountMismatch),
            }
        }
    }
}

fn bolt12_amount_msat_for_send(
    options: &cdk_common::payment::Bolt12OutgoingPaymentOptions,
) -> Result<Option<u64>, payment::Error> {
    bolt12_melt_options_amount_msat(options)
}

fn bolt12_melt_options_amount_msat(
    options: &cdk_common::payment::Bolt12OutgoingPaymentOptions,
) -> Result<Option<u64>, payment::Error> {
    match &options.melt_options {
        Some(MeltOptions::Amountless { amountless }) => {
            let amount_msat = u64::from(amountless.amount_msat);
            validate_bolt12_amount_msat(&options.offer, amount_msat)?;
            Ok(Some(amount_msat))
        }
        Some(MeltOptions::Mpp { mpp: _ }) => Err(payment::Error::UnsupportedPaymentOption),
        None => Ok(None),
    }
}

fn validate_bolt12_amount_msat(
    offer: &lightning::offers::offer::Offer,
    amount_msat: u64,
) -> Result<(), payment::Error> {
    match offer.amount() {
        Some(OfferAmount::Bitcoin { amount_msats }) if amount_msats == amount_msat => Ok(()),
        Some(_) => Err(payment::Error::AmountMismatch),
        None => Ok(()),
    }
}

fn msat_total_spent_for_unit(
    total_msat: u64,
    unit: &CurrencyUnit,
) -> Result<Amount<CurrencyUnit>, payment::Error> {
    match unit {
        CurrencyUnit::Msat => Ok(Amount::new(total_msat, CurrencyUnit::Msat)),
        CurrencyUnit::Sat => Ok(Amount::new(
            total_msat.div_ceil(MSAT_IN_SAT),
            CurrencyUnit::Sat,
        )),
        _ => Amount::new(total_msat, CurrencyUnit::Msat)
            .convert_to(unit)
            .map_err(payment::Error::from),
    }
}

fn route_parameters_from_max_fee(
    max_fee_amount: Option<Amount<CurrencyUnit>>,
) -> Result<Option<RouteParametersConfig>, payment::Error> {
    max_fee_amount
        .map(|amount| {
            amount
                .convert_to(&CurrencyUnit::Msat)
                .map(|amount_msat| RouteParametersConfig {
                    max_total_routing_fee_msat: Some(amount_msat.value()),
                    max_total_cltv_expiry_delta: ROUTE_DEFAULT_MAX_TOTAL_CLTV_EXPIRY_DELTA,
                    max_path_count: ROUTE_DEFAULT_MAX_PATH_COUNT,
                    max_channel_saturation_power_of_half:
                        ROUTE_DEFAULT_MAX_CHANNEL_SATURATION_POWER_OF_HALF,
                })
        })
        .transpose()
        .map_err(payment::Error::from)
}

fn select_bolt11_payment(payments: Vec<Payment>) -> Option<Payment> {
    payments.into_iter().min_by_key(|payment| {
        let status_order = match payment_status(payment) {
            Ok(PaymentStatus::Succeeded) => 0_u8,
            Ok(PaymentStatus::Pending) => 1,
            Ok(PaymentStatus::Failed) | Err(_) => 2,
        };

        (
            status_order,
            std::cmp::Reverse(payment.latest_update_timestamp),
        )
    })
}

async fn sleep_or_cancel(cancel_token: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        _ = cancel_token.cancelled() => false,
        _ = tokio::time::sleep(duration) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::settings_response;
    use crate::settings::AdvertisedMethod;

    #[test]
    fn every_method_is_advertised_by_default() {
        let settings = settings_response(&AdvertisedMethod::ALL);
        assert!(settings.bolt11.is_some());
        assert!(settings.bolt12.is_some());
        // This backend has never served on-chain.
        assert!(settings.onchain.is_none());
    }

    #[test]
    fn bolt12_only_leaves_bolt11_free() {
        // The shape that lets another backend take bolt11 without colliding.
        let settings = settings_response(&[AdvertisedMethod::Bolt12]);
        assert!(settings.bolt12.is_some());
        assert!(settings.bolt11.is_none());
    }

    #[test]
    fn bolt11_only_leaves_bolt12_free() {
        let settings = settings_response(&[AdvertisedMethod::Bolt11]);
        assert!(settings.bolt11.is_some());
        assert!(settings.bolt12.is_none());
    }

    #[test]
    fn advertising_nothing_yields_no_registrable_method() {
        let settings = settings_response(&[]);
        assert!(settings.bolt11.is_none());
        assert!(settings.bolt12.is_none());
    }

    #[test]
    fn advertised_settings_keep_their_values() {
        let all = settings_response(&AdvertisedMethod::ALL);
        let bolt11_only = settings_response(&[AdvertisedMethod::Bolt11]);
        assert_eq!(all.unit, bolt11_only.unit);
        assert_eq!(
            all.bolt11
                .map(|s| (s.mpp, s.amountless, s.invoice_description)),
            bolt11_only
                .bolt11
                .map(|s| (s.mpp, s.amountless, s.invoice_description))
        );
    }

    use super::*;
    use bitcoin::secp256k1::{Keypair, PublicKey, Secp256k1, SecretKey};
    use cdk_common::payment::Bolt12OutgoingPaymentOptions;
    use ldk_server_client::ldk_server_grpc::events::{
        PaymentFailed as LdkPaymentFailed, PaymentSuccessful as LdkPaymentSuccessful,
    };
    use ldk_server_client::ldk_server_grpc::types::{Bolt11, PaymentKind};
    use lightning::offers::offer::OfferBuilder;

    fn test_bolt11_payment(status: PaymentStatus, amount_msat: Option<u64>) -> Payment {
        Payment {
            id: "02".repeat(32),
            kind: Some(PaymentKind {
                kind: Some(payment_kind::Kind::Bolt11(Bolt11 {
                    hash: "01".repeat(32),
                    preimage: Some("03".repeat(32)),
                    secret: None,
                })),
            }),
            amount_msat,
            fee_paid_msat: None,
            direction: PaymentDirection::Outbound as i32,
            status: status as i32,
            latest_update_timestamp: 0,
        }
    }

    fn test_bolt11_payment_with_id(
        id_byte: u8,
        status: PaymentStatus,
        latest_update_timestamp: u64,
    ) -> Payment {
        Payment {
            id: format!("{id_byte:02x}").repeat(32),
            latest_update_timestamp,
            ..test_bolt11_payment(status, None)
        }
    }

    fn test_offer(amount_msats: Option<u64>) -> lightning::offers::offer::Offer {
        let secp_ctx = Secp256k1::new();
        let secret_key = SecretKey::from_slice(&[42; 32]).expect("test secret key should be valid");
        let keys = Keypair::from_secret_key(&secp_ctx, &secret_key);
        let pubkey = PublicKey::from(keys);
        let builder = OfferBuilder::new(pubkey);

        match amount_msats {
            Some(amount_msats) => builder
                .amount_msats(amount_msats)
                .build()
                .expect("fixed test offer should build"),
            None => builder.build().expect("variable test offer should build"),
        }
    }

    fn bolt12_options(
        offer: lightning::offers::offer::Offer,
        melt_options: Option<MeltOptions>,
    ) -> Bolt12OutgoingPaymentOptions {
        Bolt12OutgoingPaymentOptions {
            offer,
            max_fee_amount: None,
            timeout_secs: None,
            melt_options,
            quote_id: cdk_common::QuoteId::new(),
        }
    }

    #[test]
    fn normalize_address_removes_scheme() {
        assert_eq!(
            normalize_address("https://127.0.0.1:3536/"),
            "127.0.0.1:3536"
        );
        assert_eq!(normalize_address("127.0.0.1:3536"), "127.0.0.1:3536");
    }

    async fn correlator_is_idle(correlations: &OutgoingPaymentCorrelations) -> bool {
        let state = correlations.state.lock().await;
        state.by_key.is_empty() && state.unmatched.is_empty() && state.ready.is_empty()
    }

    #[tokio::test]
    async fn successful_outgoing_event_includes_quote_and_payment_details() {
        let correlations = OutgoingPaymentCorrelator::new();
        let quote_id = QuoteId::new();
        remember_outgoing_payment_context(
            &correlations,
            &"02".repeat(32),
            OutgoingPaymentContext {
                quote_id: quote_id.clone(),
                unit: CurrencyUnit::Sat,
            },
        )
        .await;
        let payment = Payment {
            fee_paid_msat: Some(1),
            ..test_bolt11_payment(PaymentStatus::Succeeded, Some(1_000))
        };
        let envelope = EventEnvelope {
            event: Some(event_envelope::Event::PaymentSuccessful(
                LdkPaymentSuccessful {
                    payment: Some(payment),
                },
            )),
        };

        let event = LdkServerBackend::event_from_envelope(envelope, &correlations)
            .await
            .expect("successful event should map")
            .expect("successful event should be emitted");
        let Event::PaymentSuccessful {
            quote_id: event_quote_id,
            details,
        } = event
        else {
            panic!("expected outgoing payment success event");
        };

        assert_eq!(event_quote_id, quote_id);
        assert_eq!(
            details.payment_lookup_id,
            PaymentIdentifier::PaymentId([2; 32])
        );
        assert_eq!(details.status, MeltQuoteState::Paid);
        assert_eq!(details.total_spent, Amount::new(2, CurrencyUnit::Sat));
        assert!(correlator_is_idle(&correlations).await);
    }

    #[tokio::test]
    async fn failed_outgoing_event_can_correlate_by_bolt11_hash() {
        let correlations = OutgoingPaymentCorrelator::new();
        let quote_id = QuoteId::new();
        remember_outgoing_payment_context(
            &correlations,
            &"01".repeat(32),
            OutgoingPaymentContext {
                quote_id: quote_id.clone(),
                unit: CurrencyUnit::Msat,
            },
        )
        .await;
        let envelope = EventEnvelope {
            event: Some(event_envelope::Event::PaymentFailed(LdkPaymentFailed {
                payment: Some(test_bolt11_payment(PaymentStatus::Failed, None)),
            })),
        };

        let event = LdkServerBackend::event_from_envelope(envelope, &correlations)
            .await
            .expect("failed event should map")
            .expect("failed event should be emitted");
        let Event::PaymentFailed {
            quote_id: event_quote_id,
            reason,
        } = event
        else {
            panic!("expected outgoing payment failure event");
        };

        assert_eq!(event_quote_id, quote_id);
        assert!(reason.contains(&"02".repeat(32)));
        assert!(correlator_is_idle(&correlations).await);
    }

    #[tokio::test]
    async fn uncorrelated_outgoing_event_is_buffered_without_blocking() {
        let correlations = OutgoingPaymentCorrelator::new();
        let envelope = EventEnvelope {
            event: Some(event_envelope::Event::PaymentSuccessful(
                LdkPaymentSuccessful {
                    payment: Some(test_bolt11_payment(PaymentStatus::Succeeded, Some(1_000))),
                },
            )),
        };

        let started = Instant::now();
        let event = LdkServerBackend::event_from_envelope(envelope, &correlations)
            .await
            .expect("uncorrelated event should map");
        assert!(event.is_none());
        assert!(started.elapsed() < Duration::from_millis(50));
        assert!(take_ready_outgoing_event(&correlations).await.is_none());
        assert_eq!(correlations.state.lock().await.unmatched.len(), 1);
    }

    #[tokio::test]
    async fn remembering_quote_replays_buffered_outgoing_success() {
        let correlations = OutgoingPaymentCorrelator::new();
        let quote_id = QuoteId::new();
        let envelope = EventEnvelope {
            event: Some(event_envelope::Event::PaymentSuccessful(
                LdkPaymentSuccessful {
                    payment: Some(Payment {
                        fee_paid_msat: Some(1),
                        ..test_bolt11_payment(PaymentStatus::Succeeded, Some(1_000))
                    }),
                },
            )),
        };

        let event = LdkServerBackend::event_from_envelope(envelope, &correlations)
            .await
            .expect("buffered event should map");
        assert!(event.is_none());

        remember_outgoing_payment_context(
            &correlations,
            &"02".repeat(32),
            OutgoingPaymentContext {
                quote_id: quote_id.clone(),
                unit: CurrencyUnit::Sat,
            },
        )
        .await;

        let event = take_ready_outgoing_event(&correlations)
            .await
            .expect("buffered success should become ready after remember");
        let Event::PaymentSuccessful {
            quote_id: event_quote_id,
            details,
        } = event
        else {
            panic!("expected outgoing payment success event");
        };
        assert_eq!(event_quote_id, quote_id);
        assert_eq!(details.status, MeltQuoteState::Paid);
        assert_eq!(details.total_spent, Amount::new(2, CurrencyUnit::Sat));
        assert!(correlator_is_idle(&correlations).await);
    }

    #[test]
    fn failed_payment_response_does_not_require_amount() {
        let payment = test_bolt11_payment(PaymentStatus::Failed, None);

        let response = LdkServerBackend::make_payment_response_from_payment(
            &CurrencyUnit::Msat,
            PaymentIdentifier::PaymentId([2; 32]),
            &payment,
        )
        .expect("failed payment details should map without amount");

        assert_eq!(response.status, MeltQuoteState::Failed);
        assert_eq!(response.total_spent, Amount::new(0, CurrencyUnit::Msat));
    }

    #[test]
    fn paid_payment_response_requires_amount() {
        let payment = test_bolt11_payment(PaymentStatus::Succeeded, None);

        let err = LdkServerBackend::make_payment_response_from_payment(
            &CurrencyUnit::Msat,
            PaymentIdentifier::PaymentId([2; 32]),
            &payment,
        )
        .expect_err("paid payment details without amount should fail");

        assert!(matches!(err, payment::Error::Backend(_)));
    }

    #[test]
    fn paid_payment_response_rounds_msats_up_for_sat_unit() {
        let payment = Payment {
            fee_paid_msat: Some(1),
            ..test_bolt11_payment(PaymentStatus::Succeeded, Some(1000))
        };

        let response = LdkServerBackend::make_payment_response_from_payment(
            &CurrencyUnit::Sat,
            PaymentIdentifier::PaymentId([2; 32]),
            &payment,
        )
        .expect("paid payment details should map");

        assert_eq!(response.status, MeltQuoteState::Paid);
        assert_eq!(response.total_spent, Amount::new(2, CurrencyUnit::Sat));
    }

    #[test]
    fn bolt12_amountless_quote_rejects_mismatched_fixed_offer() {
        let offer = test_offer(Some(10_000));
        let options = bolt12_options(offer, Some(MeltOptions::new_amountless(1_000_u64)));

        let err = bolt12_amount_msat_for_quote(&options)
            .expect_err("mismatched fixed offer amount should fail");

        assert!(matches!(err, payment::Error::AmountMismatch));
    }

    #[test]
    fn bolt12_amountless_send_rejects_mismatched_fixed_offer() {
        let offer = test_offer(Some(10_000));
        let options = bolt12_options(offer, Some(MeltOptions::new_amountless(1_000_u64)));

        let err = bolt12_amount_msat_for_send(&options)
            .expect_err("mismatched fixed offer amount should fail");

        assert!(matches!(err, payment::Error::AmountMismatch));
    }

    #[test]
    fn bolt12_amountless_accepts_matching_fixed_offer() {
        let offer = test_offer(Some(10_000));
        let options = bolt12_options(offer, Some(MeltOptions::new_amountless(10_000_u64)));

        let amount_msat =
            bolt12_amount_msat_for_quote(&options).expect("matching fixed offer amount is valid");

        assert_eq!(amount_msat, 10_000);
    }

    #[test]
    fn bolt12_amountless_accepts_variable_offer() {
        let offer = test_offer(None);
        let options = bolt12_options(offer, Some(MeltOptions::new_amountless(10_000_u64)));

        let amount_msat =
            bolt12_amount_msat_for_send(&options).expect("variable offer amount is valid");

        assert_eq!(amount_msat, Some(10_000));
    }

    #[test]
    fn bolt11_payment_selection_prefers_pending_over_failed() {
        let failed = test_bolt11_payment_with_id(1, PaymentStatus::Failed, 2);
        let pending = test_bolt11_payment_with_id(2, PaymentStatus::Pending, 1);

        let selected = select_bolt11_payment(vec![failed, pending])
            .expect("payment details should be selected");

        assert_eq!(selected.id, "02".repeat(32));
        assert_eq!(
            payment_status(&selected).expect("valid status"),
            PaymentStatus::Pending
        );
    }

    #[test]
    fn bolt11_payment_selection_prefers_succeeded_over_pending() {
        let pending = test_bolt11_payment_with_id(1, PaymentStatus::Pending, 2);
        let succeeded = Payment {
            amount_msat: Some(1000),
            ..test_bolt11_payment_with_id(2, PaymentStatus::Succeeded, 1)
        };

        let selected = select_bolt11_payment(vec![pending, succeeded])
            .expect("payment details should be selected");

        assert_eq!(selected.id, "02".repeat(32));
        assert_eq!(
            payment_status(&selected).expect("valid status"),
            PaymentStatus::Succeeded
        );
    }
}
