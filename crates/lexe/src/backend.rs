//! Lexe backend for the CDK payment processor.
//!
//! Talks to a Lexe managed non-custodial Lightning node through the Lexe
//! Rust SDK. BOLT11 only. Retry safety comes from a local redb database
//! keyed by BOLT11 payment hash, since the Lexe node does not accept a
//! client idempotency token for payments.

use std::collections::HashMap;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::{anyhow, Context as _};
use async_trait::async_trait;
use cdk_common::bitcoin::hashes::Hash as _;
use cdk_common::nuts::{CurrencyUnit, MeltQuoteState};
use cdk_common::payment::{
    Bolt11Settings, CreateIncomingPaymentResponse, Error, Event, IncomingPaymentOptions,
    MakePaymentResponse, MintPayment, OutgoingPaymentOptions, PaymentIdentifier,
    PaymentQuoteResponse, SettingsResponse, WaitPaymentResponse,
};
use cdk_common::util::unix_time;
use cdk_common::{Amount, Bolt11Invoice};
use futures::Stream;
use lexe::config::WalletEnvConfig;
use lexe::types::auth::{ClientCredentials, CredentialsRef, RootSeed};
use lexe::types::bitcoin::{Amount as LexeAmount, Invoice as LexeInvoice};
use lexe::types::command::{
    CreateInvoiceRequest, GetPaymentRequest, GetUpdatedPaymentsRequest, PayInvoiceRequest,
};
use lexe::types::payment::{
    Payment, PaymentCreatedIndex, PaymentDirection, PaymentStatus, PaymentUpdatedIndex,
};
use lexe::util::ByteArray;
use lexe::wallet::LexeWallet;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;

use crate::client::{LexeClient, SubmissionError};
use crate::database::QuoteDatabase;
use crate::settings::{Config, CredentialSource};

/// Bound each SDK update fetch, including its network sync.
const POLL_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_secs(5);
const EVENT_PAGE_SIZE: usize = 100;
/// Initial backoff after a failed poll.
const POLL_BACKOFF_INITIAL: Duration = Duration::from_millis(500);
/// Maximum backoff after a failed poll.
const POLL_BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Number of records per page; recovery continues through all pages.
const RECOVERY_PAGE_SIZE: usize = 500;

/// Event stream that releases its Lexe poller activity on completion or drop.
struct PaymentEventStream {
    receiver: ReceiverStream<Event>,
    activity: Arc<PaymentEventStreamActivity>,
}

struct PaymentEventStreamActivity {
    active_streams: Arc<AtomicUsize>,
    active: AtomicBool,
}

impl PaymentEventStreamActivity {
    fn new(active_streams: Arc<AtomicUsize>) -> Self {
        active_streams.fetch_add(1, Ordering::Relaxed);
        Self {
            active_streams,
            active: AtomicBool::new(true),
        }
    }

    fn deactivate(&self) {
        if self.active.swap(false, Ordering::Relaxed) {
            self.active_streams.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl Drop for PaymentEventStreamActivity {
    fn drop(&mut self) {
        self.deactivate();
    }
}

impl PaymentEventStream {
    fn new(receiver: mpsc::Receiver<Event>, active_streams: Arc<AtomicUsize>) -> Self {
        Self {
            receiver: ReceiverStream::new(receiver),
            activity: Arc::new(PaymentEventStreamActivity::new(active_streams)),
        }
    }
}

impl Stream for PaymentEventStream {
    type Item = Event;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().receiver).poll_next(cx)
    }
}

impl Drop for PaymentEventStream {
    fn drop(&mut self) {
        self.activity.deactivate();
    }
}

/// CDK payment backend backed by a Lexe managed Lightning node.
pub struct LexeBackend {
    wallet: Arc<dyn LexeClient>,
    db: QuoteDatabase,
    active_payment_streams: Arc<AtomicUsize>,
    event_cancel: watch::Sender<()>,
    fee_reserve_ppm: u32,
    fee_reserve_min_sat: u64,
    payment_timeout: Duration,
}

impl LexeBackend {
    /// Connect to the Lexe node from configuration.
    pub async fn new(config: &Config) -> Result<Self, Error> {
        let backend = &config.lexe;
        let data_dir = std::path::PathBuf::from(&backend.data_dir);
        std::fs::create_dir_all(&data_dir).map_err(|e| {
            Error::Custom(format!(
                "failed to create data dir {}: {e}",
                data_dir.display()
            ))
        })?;

        let env_config =
            wallet_env_config(&backend.network).map_err(|e| Error::Custom(e.to_string()))?;

        let source =
            CredentialSource::from_backend(backend).map_err(|e| Error::Custom(e.to_string()))?;
        let wallet = open_wallet(&env_config, &data_dir, source, backend.create_wallet).await?;

        let db = QuoteDatabase::new(data_dir.join("quotes.db"))
            .map_err(|e| Error::Custom(format!("failed to open quote database: {e}")))?;

        Ok(Self {
            wallet: Arc::new(wallet),
            db,
            active_payment_streams: Arc::new(AtomicUsize::new(0)),
            event_cancel: watch::channel(()).0,
            fee_reserve_ppm: backend.fee_reserve_ppm,
            fee_reserve_min_sat: u64::from(backend.fee_reserve_min_sat),
            payment_timeout: Duration::from_secs(backend.payment_timeout_secs),
        })
    }

    /// Recover an outbound payment across the complete local history.
    async fn find_payment_by_hash(&self, hash: &[u8; 32]) -> Result<Option<Payment>, Error> {
        self.wallet
            .sync_payments()
            .await
            .map_err(|e| Error::Backend(anyhow!("Lexe payment sync failed: {e}").into()))?;
        let mut after = None;
        loop {
            let response = self
                .wallet
                .list_payments(RECOVERY_PAGE_SIZE, after.as_ref())
                .map_err(|e| Error::Backend(anyhow!("Lexe payment list failed: {e}").into()))?;
            if let Some(payment) = response.payments.into_iter().find(|p| {
                p.direction == PaymentDirection::Outbound
                    && p.hash.as_ref().map(|h| h.to_array()) == Some(*hash)
            }) {
                return Ok(Some(payment));
            }
            let Some(next) = response.next_index else {
                return Ok(None);
            };
            if after.as_ref() == Some(&next) {
                return Err(Error::Custom("Lexe recovery cursor did not advance".into()));
            }
            after = Some(next);
            tokio::task::yield_now().await;
        }
    }

    /// Fetch a payment by stored Lexe index string.
    async fn fetch_payment(&self, index_str: &str) -> Result<Option<Payment>, Error> {
        let index: PaymentCreatedIndex = index_str.parse().map_err(|e| {
            Error::Custom(format!(
                "invalid stored Lexe payment index `{index_str}`: {e}"
            ))
        })?;
        let response = self
            .wallet
            .get_payment(GetPaymentRequest { index })
            .await
            .map_err(|e| Error::Backend(anyhow!("Lexe get_payment failed: {e}").into()))?;
        Ok(response.payment)
    }

    fn payment_hash_of(identifier: &PaymentIdentifier) -> Result<&[u8; 32], Error> {
        match identifier {
            PaymentIdentifier::PaymentHash(hash) => Ok(hash),
            _ => Err(Error::UnsupportedPaymentOption),
        }
    }

    fn ensure_supported_unit(unit: &CurrencyUnit) -> Result<(), Error> {
        if unit != &CurrencyUnit::Sat {
            return Err(Error::UnsupportedUnit);
        }
        Ok(())
    }

    fn invoice_amount_sats(invoice: &Bolt11Invoice) -> Result<u64, Error> {
        let amount_msat = invoice
            .amount_milli_satoshis()
            .ok_or(Error::AmountMismatch)?;
        // Lexe invoices are whole sats; a fractional-msat invoice cannot be
        // paid exactly.
        if amount_msat % 1000 != 0 {
            return Err(Error::AmountMismatch);
        }
        let amount_sats = amount_msat / 1000;
        if amount_sats == 0 {
            return Err(Error::AmountMismatch);
        }
        Ok(amount_sats)
    }

    /// Estimated outgoing fee: `ppm` of the amount, at least `min_sat`.
    fn estimate_fee_sats(amount_sats: u64, ppm: u32, min_sat: u64) -> u64 {
        let estimated = (u128::from(amount_sats) * u128::from(ppm)).div_ceil(1_000_000);
        u64::try_from(estimated).unwrap_or(u64::MAX).max(min_sat)
    }

    fn melt_status(status: PaymentStatus) -> MeltQuoteState {
        match status {
            PaymentStatus::Completed => MeltQuoteState::Paid,
            PaymentStatus::Failed => MeltQuoteState::Failed,
            PaymentStatus::Pending => MeltQuoteState::Pending,
        }
    }

    fn outgoing_response(
        payment_identifier: PaymentIdentifier,
        unit: &CurrencyUnit,
        payment: &Payment,
    ) -> Result<MakePaymentResponse, Error> {
        let status = Self::melt_status(payment.status);
        let total_spent = if status == MeltQuoteState::Paid {
            let amount = payment
                .amount
                .ok_or_else(|| Error::Custom("Completed Lexe payment has no amount".into()))?;
            let msats = amount
                .msat()
                .checked_add(payment.fees.msat())
                .ok_or_else(|| Error::Custom("Lexe payment total overflow".into()))?;
            // ensure_supported_unit only accepts `sat`; report whole sats.
            Amount::new(msats.div_ceil(1000), CurrencyUnit::Sat)
        } else {
            Amount::new(0, unit.clone())
        };
        Ok(MakePaymentResponse {
            payment_lookup_id: payment_identifier,
            payment_proof: payment
                .preimage
                .map(|preimage| hex::encode(preimage.to_array())),
            status,
            total_spent,
        })
    }

    fn response_without_payment(
        identifier: PaymentIdentifier,
        unit: CurrencyUnit,
        status: MeltQuoteState,
    ) -> MakePaymentResponse {
        MakePaymentResponse {
            payment_lookup_id: identifier,
            payment_proof: None,
            status,
            total_spent: Amount::new(0, unit),
        }
    }
}

#[async_trait]
impl MintPayment for LexeBackend {
    type Err = Error;

    async fn start(&self) -> Result<(), Self::Err> {
        let node = self
            .wallet
            .node_info()
            .await
            .map_err(|e| Error::Backend(anyhow!("Lexe node_info failed: {e}").into()))?;
        tracing::info!(
            node_version = %node.version,
            lightning_balance_sats = node.lightning_balance.sats_u64(),
            "Lexe node reachable"
        );
        Ok(())
    }

    async fn get_settings(&self) -> Result<SettingsResponse, Self::Err> {
        Ok(SettingsResponse {
            unit: "sat".to_string(),
            bolt11: Some(Bolt11Settings {
                mpp: false,
                amountless: false,
                invoice_description: true,
            }),
            bolt12: None,
            onchain: None,
            custom: HashMap::new(),
        })
    }

    async fn create_incoming_payment_request(
        &self,
        options: IncomingPaymentOptions,
    ) -> Result<CreateIncomingPaymentResponse, Self::Err> {
        let IncomingPaymentOptions::Bolt11(opts) = options else {
            return Err(Error::UnsupportedPaymentOption);
        };

        Self::ensure_supported_unit(opts.amount.unit())?;
        let amount_sats = opts.amount.to_sat()?;
        if amount_sats == 0 {
            return Err(Error::AmountMismatch);
        }
        let description = opts.description.unwrap_or_default();
        let expiration_secs = match opts.unix_expiry {
            Some(expiry) => {
                let now = unix_time();
                if expiry <= now {
                    return Err(Error::InvalidExpiry);
                }
                Some(u32::try_from(expiry - now).map_err(|_| Error::InvalidExpiry)?)
            }
            None => None,
        };

        let lexe_amount = LexeAmount::try_from_sats_u64(amount_sats)
            .map_err(|e| Error::Custom(format!("invalid invoice amount: {e}")))?;

        let response = self
            .wallet
            .create_invoice(CreateInvoiceRequest {
                expiration_secs,
                amount: Some(lexe_amount),
                description: Some(description),
                personal_note: None,
                partner_pk: None,
                partner_prop_fee: None,
                partner_base_fee: None,
            })
            .await
            .map_err(|e| Error::Backend(anyhow!("Lexe create_invoice failed: {e}").into()))?;

        let invoice = response.invoice.to_string();
        let parsed = Bolt11Invoice::from_str(&invoice)?;
        let payment_hash = parsed.payment_hash().to_byte_array();

        // Write both mint mappings in one transaction so a crash cannot
        // leave an invoice without its payment index.
        self.db
            .insert_mint_quote_and_payment_id(&payment_hash, &invoice, &response.index.to_string())
            .map_err(|e| Error::Custom(e.to_string()))?;

        Ok(CreateIncomingPaymentResponse {
            request_lookup_id: PaymentIdentifier::PaymentHash(payment_hash),
            request: invoice,
            expiry: opts.unix_expiry,
            extra_json: None,
        })
    }

    async fn get_payment_quote(
        &self,
        unit: &CurrencyUnit,
        options: OutgoingPaymentOptions,
    ) -> Result<PaymentQuoteResponse, Self::Err> {
        Self::ensure_supported_unit(unit)?;
        let OutgoingPaymentOptions::Bolt11(opts) = options else {
            return Err(Error::UnsupportedPaymentOption);
        };

        let invoice = opts.bolt11.to_string();
        let amount_sats = Self::invoice_amount_sats(&opts.bolt11)?;
        let fee_sats =
            Self::estimate_fee_sats(amount_sats, self.fee_reserve_ppm, self.fee_reserve_min_sat);
        let payment_hash = opts.bolt11.payment_hash().to_byte_array();

        self.db
            .insert_melt_quote(&payment_hash, &invoice)
            .map_err(|e| Error::Custom(e.to_string()))?;

        Ok(PaymentQuoteResponse {
            request_lookup_id: Some(PaymentIdentifier::PaymentHash(payment_hash)),
            amount: Amount::new(amount_sats, CurrencyUnit::Sat),
            fee: Amount::new(fee_sats, CurrencyUnit::Sat),
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
        Self::ensure_supported_unit(unit)?;
        let OutgoingPaymentOptions::Bolt11(opts) = options else {
            return Err(Error::UnsupportedPaymentOption);
        };

        let invoice = opts.bolt11.to_string();
        Self::invoice_amount_sats(&opts.bolt11)?;
        let payment_hash = opts.bolt11.payment_hash().to_byte_array();
        let identifier = PaymentIdentifier::PaymentHash(payment_hash);

        // Record the invoice so a payment refused below still reads as a
        // known, unpaid quote instead of "not found".
        self.db
            .insert_melt_quote(&payment_hash, &invoice)
            .map_err(|e| Error::Custom(e.to_string()))?;
        // An existing attempt must be reconciled even if this retry carries
        // different options. Neither its event owner nor its unit can change.
        if self
            .db
            .get_melt_attempt(&payment_hash)
            .map_err(|e| Error::Custom(e.to_string()))?
            .is_some()
        {
            return self.check_outgoing_payment(&identifier).await;
        }

        // The stable SDK cannot constrain routing fees. Never turn a capped
        // request into an uncapped spend. Local rejection creates no attempt.
        if opts.max_fee_amount.is_some() {
            return Err(Error::Custom(
                "Lexe SDK cannot enforce max_fee_amount; capped outgoing payments are unsupported"
                    .into(),
            ));
        }
        if opts.melt_options.is_some() {
            return Err(Error::UnsupportedPaymentOption);
        }
        let lexe_invoice = LexeInvoice::from_str(&invoice).map_err(|e| {
            Error::Custom(format!("failed to parse outgoing invoice for Lexe: {e}"))
        })?;
        let timeout = opts
            .timeout_secs
            .map(Duration::from_secs)
            .unwrap_or(self.payment_timeout);

        // All local validation precedes this durable intent. If the process
        // stops anywhere after it, recovery remains conservative. The atomic
        // claim also serializes concurrent submissions of the same invoice.
        if !self
            .db
            .begin_melt_attempt(&payment_hash, &opts.quote_id, unit)
            .map_err(|e| Error::Custom(e.to_string()))?
        {
            return self.check_outgoing_payment(&identifier).await;
        }

        // Use one deadline for submission and settlement, not a fresh timeout
        // for each phase. Persist acceptance before starting any status polls.
        let deadline = tokio::time::Instant::now() + timeout;
        let index = match tokio::time::timeout_at(
            deadline,
            self.wallet.submit_invoice(PayInvoiceRequest {
                invoice: lexe_invoice,
                fallback_amount: None,
                personal_note: None,
            }),
        )
        .await
        {
            Ok(Ok(index)) => index,
            Ok(Err(SubmissionError::Rejected(e))) => {
                self.db
                    .reject_melt_attempt(&payment_hash)
                    .map_err(|e| Error::Custom(e.to_string()))?;
                tracing::warn!(error = %e, "Lexe payment submission was rejected");
                return self.check_outgoing_payment(&identifier).await;
            }
            Ok(Err(SubmissionError::Ambiguous(e))) => {
                tracing::warn!(error = %e, "Lexe payment submission is ambiguous; reconciling");
                return self.check_outgoing_payment(&identifier).await;
            }
            Err(_) => {
                tracing::warn!(
                    payment_hash = %hex::encode(payment_hash),
                    ?timeout,
                    "Lexe submission timed out; payment may still be in flight"
                );
                return self.check_outgoing_payment(&identifier).await;
            }
        };
        self.db
            .insert_melt_payment_id(&payment_hash, &index.to_string())
            .map_err(|e| Error::Custom(e.to_string()))?;

        match tokio::time::timeout_at(deadline, self.wallet.wait_for_payment(index)).await {
            Ok(Ok(payment)) => Self::outgoing_response(identifier, unit, &payment),
            Ok(Err(e)) => {
                // Even a typed auth/request error here occurred AFTER acceptance.
                tracing::warn!(error = %e, "Lexe settlement lookup failed; reconciling");
                self.check_outgoing_payment(&identifier).await
            }
            Err(_) => {
                tracing::warn!(
                    payment_hash = %hex::encode(payment_hash),
                    ?timeout,
                    "Lexe settlement timed out; payment may still be in flight"
                );
                self.check_outgoing_payment(&identifier).await
            }
        }
    }

    async fn wait_payment_event(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Event> + Send>>, Self::Err> {
        let (sender, receiver) = mpsc::channel(100);
        let stream = PaymentEventStream::new(receiver, Arc::clone(&self.active_payment_streams));
        let activity = Arc::clone(&stream.activity);
        let mut cancel = self.event_cancel.subscribe();
        let wallet = Arc::clone(&self.wallet);
        let db = self.db.clone();

        tokio::spawn(async move {
            let mut backoff = POLL_BACKOFF_INITIAL;
            // GetUpdatedPayments(None) replays cached history, unlike
            // WaitForNextPayment(None), which starts at the SDK cache tip.
            // Reconnects deliberately replay: gRPC has no consumer ack, so
            // persisting an enqueue cursor could lose undelivered events.
            let mut cursor: Option<PaymentUpdatedIndex> = None;
            'poll: loop {
                let result = tokio::select! {
                    _ = cancel.changed() => break,
                    _ = sender.closed() => break,
                    result = tokio::time::timeout(POLL_TIMEOUT, wallet.get_updated_payments(GetUpdatedPaymentsRequest {
                        start_index: cursor,
                        limit: Some(EVENT_PAGE_SIZE),
                    })) => result.map_err(anyhow::Error::from).and_then(|result| result),
                };
                let delay = match result {
                    Ok(response) => {
                        let empty = response.payments.is_empty();
                        let mut failed = false;
                        for payment in response.payments {
                            match map_payment_event(&db, &payment) {
                                Ok(Some(event)) => {
                                    tokio::select! {
                                        _ = cancel.changed() => break 'poll,
                                        result = sender.send(event) => if result.is_err() { break 'poll; },
                                    }
                                }
                                Ok(None) => {}
                                Err(err) => {
                                    // start_index is exclusive. Leave the cursor
                                    // on the previous payment so this update is
                                    // fetched again instead of skipped.
                                    tracing::warn!("Could not map Lexe payment update: {err}");
                                    failed = true;
                                    break;
                                }
                            }
                            cursor = Some(payment.updated_index());
                        }
                        if failed {
                            backoff
                        } else {
                            backoff = POLL_BACKOFF_INITIAL;
                            if empty {
                                POLL_INTERVAL
                            } else {
                                Duration::ZERO
                            }
                        }
                    }
                    Err(err) => {
                        tracing::warn!("Could not fetch Lexe payment updates: {err}");
                        let delay = backoff;
                        backoff = (backoff * 2).min(POLL_BACKOFF_MAX);
                        delay
                    }
                };
                tokio::select! {
                    _ = cancel.changed() => break,
                    _ = sender.closed() => break,
                    _ = tokio::time::sleep(delay) => {}
                }
            }
            activity.deactivate();
        });

        Ok(Box::pin(stream))
    }

    fn is_payment_event_stream_active(&self) -> bool {
        self.active_payment_streams.load(Ordering::Relaxed) > 0
    }

    fn cancel_payment_event_stream(&self) {
        let _ = self.event_cancel.send(());
    }

    async fn check_incoming_payment_status(
        &self,
        payment_identifier: &PaymentIdentifier,
    ) -> Result<Vec<WaitPaymentResponse>, Self::Err> {
        let payment_hash = Self::payment_hash_of(payment_identifier)?;
        let Some(index) = self
            .db
            .get_mint_payment_id(payment_hash)
            .map_err(|e| Error::Custom(e.to_string()))?
        else {
            return Ok(vec![]);
        };
        let Some(payment) = self.fetch_payment(&index).await? else {
            return Ok(vec![]);
        };
        if payment.status != PaymentStatus::Completed {
            return Ok(vec![]);
        }

        let amount_sats = payment.amount.map(|a| a.sats_u64()).ok_or_else(|| {
            Error::Custom(format!(
                "inbound payment {} is completed but its amount cannot be determined",
                payment.index
            ))
        })?;

        Ok(vec![WaitPaymentResponse {
            payment_id: payment.index.to_string(),
            payment_identifier: payment_identifier.clone(),
            payment_amount: Amount::new(amount_sats, CurrencyUnit::Sat),
        }])
    }

    async fn check_outgoing_payment(
        &self,
        payment_identifier: &PaymentIdentifier,
    ) -> Result<MakePaymentResponse, Self::Err> {
        let payment_hash = *Self::payment_hash_of(payment_identifier)?;

        let attempt = self
            .db
            .get_melt_attempt(&payment_hash)
            .map_err(|e| Error::Custom(e.to_string()))?;
        let Some(attempt) = attempt else {
            if self
                .db
                .get_melt_quote(&payment_hash)
                .map_err(|e| Error::Custom(e.to_string()))?
                .is_none()
            {
                return Err(Error::Custom("Outgoing payment not found".into()));
            }
            return Ok(Self::response_without_payment(
                payment_identifier.clone(),
                CurrencyUnit::Sat,
                MeltQuoteState::Unpaid,
            ));
        };

        let index = self
            .db
            .get_melt_payment_id(&payment_hash)
            .map_err(|e| Error::Custom(e.to_string()))?;
        if index.is_none() && attempt.submission_rejected {
            // A rejected submission cannot settle. This result is durable and
            // does not depend on history or on the node being reachable.
            return Ok(Self::response_without_payment(
                payment_identifier.clone(),
                attempt.unit,
                MeltQuoteState::Failed,
            ));
        }
        if let Some(index) = index {
            if let Some(payment) = self.fetch_payment(&index).await? {
                return Self::outgoing_response(
                    payment_identifier.clone(),
                    &attempt.unit,
                    &payment,
                );
            }
        }

        // The remote index may have been lost with the submission response.
        if let Some(payment) = self.find_payment_by_hash(&payment_hash).await? {
            self.db
                .insert_melt_payment_id(&payment_hash, &payment.index.to_string())
                .map_err(|e| Error::Custom(e.to_string()))?;
            return Self::outgoing_response(payment_identifier.clone(), &attempt.unit, &payment);
        }

        // Conservatively Pending: the invoice may still settle remotely.
        Ok(Self::response_without_payment(
            payment_identifier.clone(),
            attempt.unit,
            MeltQuoteState::Pending,
        ))
    }
}

/// Import one credential, or create a wallet like `lexe init`.
///
/// Client credentials do not provision: only the root seed can sign. A generated
/// seed is written before signup, so a failed signup retries the same identity.
async fn open_wallet(
    env_config: &WalletEnvConfig,
    data_dir: &std::path::Path,
    source: Option<CredentialSource<'_>>,
    create_wallet: bool,
) -> Result<LexeWallet, Error> {
    enum Owned {
        Client(ClientCredentials),
        Seed(RootSeed, bool),
    }

    let owned = match source {
        Some(CredentialSource::ClientCredentials(raw)) => {
            Owned::Client(decode_client_credentials(raw).map_err(custom)?)
        }
        Some(CredentialSource::ClientCredentialsPath(path)) => Owned::Client(
            decode_client_credentials(&std::fs::read_to_string(path).map_err(custom)?)
                .map_err(custom)?,
        ),
        Some(CredentialSource::RootSeed(raw)) => {
            Owned::Seed(parse_root_seed(raw).map_err(custom)?, false)
        }
        Some(CredentialSource::RootSeedPath(path)) => Owned::Seed(
            RootSeed::read_from_path_as_seedphrase_or_hex(std::path::Path::new(path))
                .map_err(custom)?,
            false,
        ),
        None => {
            let (seed, created) =
                root_seed_from_data_dir(env_config, data_dir, create_wallet).map_err(custom)?;
            Owned::Seed(seed, created)
        }
    };
    let wallet = LexeWallet::load_or_fresh(
        env_config.clone(),
        match &owned {
            Owned::Client(client) => CredentialsRef::ClientCredentials(client),
            Owned::Seed(seed, _) => CredentialsRef::RootSeed(seed),
        },
        Some(data_dir.to_path_buf()),
    )
    .map_err(custom)?;
    if let Owned::Seed(seed, created) = &owned {
        if *created {
            wallet.signup(seed, None).await.map_err(custom)?;
            tracing::info!("Signed up new Lexe node");
        }
        wallet
            .provision(CredentialsRef::RootSeed(seed))
            .await
            .map_err(custom)?;
    }
    Ok(wallet)
}

fn custom(error: impl std::fmt::Display) -> Error {
    Error::Custom(error.to_string())
}

/// Reuse the seed `lexe init` would have written, or create one.
///
/// The bool is true only when this call generated the seed.
fn root_seed_from_data_dir(
    env_config: &WalletEnvConfig,
    data_dir: &std::path::Path,
    create_wallet: bool,
) -> anyhow::Result<(RootSeed, bool)> {
    let seed_path = env_config.seedphrase_path(data_dir);
    if let Some(seed) = RootSeed::read_from_path(&seed_path)
        .with_context(|| format!("failed to read {}", seed_path.display()))?
    {
        tracing::info!(path = %seed_path.display(), "Using persisted Lexe seedphrase");
        return Ok((seed, false));
    }
    if !create_wallet {
        anyhow::bail!(
            "no Lexe credentials configured and {} does not exist",
            seed_path.display()
        );
    }
    let seed = RootSeed::generate();
    seed.write_to_path(&seed_path)
        .with_context(|| format!("failed to persist {}", seed_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&seed_path, std::fs::Permissions::from_mode(0o600)).with_context(
            || format!("failed to restrict permissions on {}", seed_path.display()),
        )?;
    }
    tracing::info!(
        path = %seed_path.display(),
        "Created a new Lexe wallet seed; back it up before funding the node"
    );
    Ok((seed, true))
}

fn parse_root_seed(raw: &str) -> anyhow::Result<RootSeed> {
    let raw = raw.trim();
    if raw.len() == 64 && raw.chars().all(|c| c.is_ascii_hexdigit()) {
        return RootSeed::from_hex(raw).context("invalid hex root seed");
    }
    let mnemonic = lexe::bip39::Mnemonic::parse_normalized(raw).map_err(|error| {
        anyhow::anyhow!(
            "root seed is neither a 64-character hex string nor a BIP39 mnemonic: {error}"
        )
    })?;
    RootSeed::from_mnemonic(mnemonic).context("invalid BIP39 mnemonic")
}

/// Decode a base64 Lexe SDK client-credentials blob.
fn decode_client_credentials(raw: &str) -> anyhow::Result<ClientCredentials> {
    // Use the SDK's own blob format (base64 STANDARD_NO_PAD of the typed
    // JSON credentials, with a client key/public-key consistency check).
    // Deserializing the wrapper type from the raw JSON fails because its
    // Deserialize impl expects the blob string, not the JSON object.
    ClientCredentials::from_str(raw.trim())
        .context("client credentials are not a valid SDK credentials blob")
}

/// Wallet environment for the configured network.
fn wallet_env_config(network: &str) -> std::result::Result<WalletEnvConfig, anyhow::Error> {
    let network = network.to_ascii_lowercase();
    let config = match network.as_str() {
        "mainnet" => WalletEnvConfig::mainnet(),
        "testnet3" | "testnet" => WalletEnvConfig::testnet3(),
        "regtest" => WalletEnvConfig::regtest(true, None::<String>),
        other => anyhow::bail!("invalid network `{other}`"),
    };
    Ok(config)
}

/// Map a Lexe payment update to a CDK event, if it is one of our payments.
fn map_payment_event(db: &QuoteDatabase, payment: &Payment) -> Result<Option<Event>, Error> {
    let Some(hash) = payment.hash.as_ref().map(|hash| hash.to_array()) else {
        return Ok(None);
    };

    match payment.direction {
        PaymentDirection::Inbound => {
            if payment.status != PaymentStatus::Completed {
                return Ok(None);
            }
            // Only emit for invoices we created.
            if db
                .get_mint_quote(&hash)
                .map_err(|e| Error::Custom(e.to_string()))?
                .is_none()
            {
                return Ok(None);
            }
            let amount_sats = payment.amount.map(|a| a.sats_u64()).ok_or_else(|| {
                Error::Custom(format!(
                    "inbound payment {} completed without an amount",
                    payment.index
                ))
            })?;
            Ok(Some(Event::PaymentReceived(WaitPaymentResponse {
                payment_id: payment.index.to_string(),
                payment_identifier: PaymentIdentifier::PaymentHash(hash),
                payment_amount: Amount::new(amount_sats, CurrencyUnit::Sat),
            })))
        }
        PaymentDirection::Outbound => {
            let Some(attempt) = db
                .get_melt_attempt(&hash)
                .map_err(|e| Error::Custom(e.to_string()))?
            else {
                return Ok(None);
            };
            db.insert_melt_payment_id(&hash, &payment.index.to_string())
                .map_err(|e| Error::Custom(e.to_string()))?;
            let Some(quote_id) = attempt.quote_id else {
                return Ok(None);
            };
            match payment.status {
                PaymentStatus::Completed => {
                    let details = LexeBackend::outgoing_response(
                        PaymentIdentifier::PaymentHash(hash),
                        &attempt.unit,
                        payment,
                    )?;
                    Ok(Some(Event::PaymentSuccessful { quote_id, details }))
                }
                PaymentStatus::Failed => Ok(Some(Event::PaymentFailed {
                    quote_id,
                    reason: payment.status_msg.clone(),
                })),
                PaymentStatus::Pending => Ok(None),
            }
        }
        PaymentDirection::Info => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use cdk_common::QuoteId;
    use lexe::types::payment::{PaymentKind, PaymentRail};
    use lexe::types::util::TimestampMs;

    #[test]
    fn imports_a_credentials_file_and_creates_a_wallet_like_lexe_init() {
        let dir = tempfile::tempdir().expect("tempdir");
        let env = wallet_env_config("regtest").expect("regtest env");
        let blob_path = dir.path().join("client-credentials.txt");
        std::fs::write(&blob_path, "not-a-real-blob\n").expect("write blob");

        let error = decode_client_credentials(std::fs::read_to_string(&blob_path).unwrap().trim())
            .expect_err("invalid blob must fail before wallet creation");
        assert!(error.to_string().contains("credentials blob"), "{error}");
        assert!(!env.seedphrase_path(dir.path()).exists());

        let mnemonic = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon art";
        parse_root_seed(mnemonic).expect("mnemonic import");

        let (_created, generated) =
            root_seed_from_data_dir(&env, dir.path(), true).expect("create wallet");
        let seed_path = env.seedphrase_path(dir.path());
        assert!(
            seed_path.is_file(),
            "lexe init persists the seed before signup"
        );
        assert!(generated);
        let first = std::fs::read_to_string(&seed_path).expect("read created seed");
        let (_again, generated) =
            root_seed_from_data_dir(&env, dir.path(), true).expect("reuse persisted seed");
        assert!(!generated);
        assert_eq!(first, std::fs::read_to_string(&seed_path).unwrap());

        let missing = dir.path().join("missing-seed");
        let error = root_seed_from_data_dir(&env, &missing, false)
            .expect_err("create_wallet = false must not generate a seed");
        assert!(error.to_string().contains("no Lexe credentials"), "{error}");
    }

    #[test]
    fn fee_estimate_uses_ppm_and_minimum() {
        // 100 ppm of 1000 sats = 0.1 sats -> ceil to 1.
        assert_eq!(LexeBackend::estimate_fee_sats(1000, 100, 1), 1);
        // 100 ppm of 50000 sats = 5 sats.
        assert_eq!(LexeBackend::estimate_fee_sats(50000, 100, 1), 5);
        // Minimum applies below the computed value.
        assert_eq!(LexeBackend::estimate_fee_sats(10, 100, 1), 1);
        // 1000 ppm (0.1%) of 100 sats = 0.1 -> rounds up to 1.
        assert_eq!(LexeBackend::estimate_fee_sats(100, 1000, 1), 1);
        // 1000 ppm of 10000 sats = 10.
        assert_eq!(LexeBackend::estimate_fee_sats(10000, 1000, 1), 10);
        // Zero amount still costs at least the minimum.
        assert_eq!(LexeBackend::estimate_fee_sats(0, 100, 2), 2);
    }

    #[test]
    fn invoice_amount_is_whole_sats() {
        // lnbc100n = 10 sats = 10_000 msat.
        let invoice = Bolt11Invoice::from_str(
            "lnbc100n1p5z3a63pp56854ytysg7e5z9fl3w5mgvrlqjfcytnjv8ff5hm5qt6gl6alxesqdqqcqzzsxqyz5vqsp5p0x0dlhn27s63j4emxnk26p7f94u0lyarnfp5yqmac9gzy4ngdss9qxpqysgqne3v0hnzt2lp0hc69xpzckk0cdcar7glvjhq60lsrfe8gejdm8c564prrnsft6ctxxyrewp4jtezrq3gxxqnfjj0f9tw2qs9y0lslmqpfu7et9",
        )
        .expect("valid invoice");
        assert_eq!(
            LexeBackend::invoice_amount_sats(&invoice).expect("whole-sat amount"),
            10
        );
    }

    #[test]
    fn maps_lex_statuses_to_melt_states() {
        assert_eq!(
            LexeBackend::melt_status(PaymentStatus::Completed),
            MeltQuoteState::Paid
        );
        assert_eq!(
            LexeBackend::melt_status(PaymentStatus::Failed),
            MeltQuoteState::Failed
        );
        assert_eq!(
            LexeBackend::melt_status(PaymentStatus::Pending),
            MeltQuoteState::Pending
        );
    }

    #[test]
    fn payment_index_round_trips_through_string() {
        // Index strings are "<19-digit-timestamp>-ln_<64-hex>".
        let raw = "0002683862736062841-ln_3ddc5b1e2a9c8f0e1d2b4a6c8e0f1d3b5a7c9e1f2d4b6a8c0e2f4a6c8d0b2e4f";
        let index: PaymentCreatedIndex = raw.parse().expect("valid index string");
        assert_eq!(index.to_string(), raw);
    }

    #[test]
    fn malformed_payment_index_rejected() {
        assert!("not-an-index".parse::<PaymentCreatedIndex>().is_err());
        assert!("0002683862736062841-"
            .parse::<PaymentCreatedIndex>()
            .is_err());
    }

    #[test]
    fn client_credentials_reject_invalid_base64() {
        assert!(decode_client_credentials("!!!not-base64!!!").is_err());
    }

    #[test]
    fn client_credentials_reject_non_json_payload() {
        let b64 = base64::engine::general_purpose::STANDARD.encode("this is not json");
        assert!(decode_client_credentials(&b64).is_err());
    }

    fn test_event_db_path() -> std::path::PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "cdk-lexe-events-{}-{unique}.redb",
            std::process::id()
        ))
    }

    pub(super) fn test_payment(
        direction: PaymentDirection,
        status: PaymentStatus,
        hash: [u8; 32],
        amount_sats: u64,
        fees_sats: u64,
        status_msg: &str,
    ) -> Payment {
        Payment {
            index: "0002683862736062841-ln_3ddc5b1e2a9c8f0e1d2b4a6c8e0f1d3b5a7c9e1f2d4b6a8c0e2f4a6c8d0b2e4f"
                .parse()
                .expect("valid index string"),
            rail: PaymentRail::Invoice,
            kind: PaymentKind::Invoice,
            direction,
            hash: Some(hex::encode(hash).parse().expect("valid hex payment hash")),
            preimage: Some(
                hex::encode([9_u8; 32])
                    .parse()
                    .expect("valid hex preimage"),
            ),
            offer_id: None,
            txid: None,
            amount: Some(
                LexeAmount::try_from_sats_u64(amount_sats).expect("valid amount"),
            ),
            fees: LexeAmount::try_from_sats_u64(fees_sats).expect("valid fees"),
            partner_pk: None,
            partner_prop_fee: None,
            partner_base_fee: None,
            status,
            status_msg: status_msg.to_string(),
            address: None,
            invoice: None,
            tx: None,
            payer_name: None,
            message: None,
            personal_note: None,
            priority: None,
            expires_at: None,
            finalized_at: None,
            created_at: TimestampMs::from_secs_u32(1_700_000_000),
            updated_at: TimestampMs::from_secs_u32(1_700_000_001),
        }
    }

    #[test]
    fn inbound_completed_payment_becomes_payment_received() {
        let path = test_event_db_path();
        let db = QuoteDatabase::new(&path).expect("create quote database");
        let hash = [7_u8; 32];
        db.insert_mint_quote_and_payment_id(&hash, "lnbc1incoming-invoice", "0001-ln_test")
            .expect("insert mint quote");

        let payment = test_payment(
            PaymentDirection::Inbound,
            PaymentStatus::Completed,
            hash,
            21_000,
            0,
            "received",
        );
        match map_payment_event(&db, &payment)
            .unwrap()
            .expect("event for our completed inbound payment")
        {
            Event::PaymentReceived(response) => {
                assert_eq!(
                    response.payment_identifier,
                    PaymentIdentifier::PaymentHash(hash)
                );
                assert_eq!(
                    response.payment_amount.to_sat().expect("sat amount"),
                    21_000
                );
            }
            other => panic!("expected PaymentReceived, got {other:?}"),
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn inbound_payments_we_did_not_create_or_pending_are_ignored() {
        let path = test_event_db_path();
        let db = QuoteDatabase::new(&path).expect("create quote database");
        let ours = [10_u8; 32];
        let foreign = [11_u8; 32];
        db.insert_mint_quote_and_payment_id(&ours, "lnbc1incoming-invoice", "0001-ln_test")
            .expect("insert mint quote");

        // Settled, but for an invoice this processor did not create.
        let foreign_payment = test_payment(
            PaymentDirection::Inbound,
            PaymentStatus::Completed,
            foreign,
            21_000,
            0,
            "received",
        );
        assert!(map_payment_event(&db, &foreign_payment).unwrap().is_none());

        // Our invoice, but not settled yet.
        let pending = test_payment(
            PaymentDirection::Inbound,
            PaymentStatus::Pending,
            ours,
            21_000,
            0,
            "pending",
        );
        assert!(map_payment_event(&db, &pending).unwrap().is_none());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn outbound_completed_payment_becomes_payment_successful() {
        let path = test_event_db_path();
        let db = QuoteDatabase::new(&path).expect("create quote database");
        let hash = [8_u8; 32];
        let quote_id: QuoteId = "018f8b3e-8c1a-7d2e-9f4b-6a1c3e5d7f90"
            .parse()
            .expect("valid quote id");
        db.begin_melt_attempt(&hash, &quote_id, &CurrencyUnit::Sat)
            .expect("begin melt attempt");

        let payment = test_payment(
            PaymentDirection::Outbound,
            PaymentStatus::Completed,
            hash,
            5_000,
            3,
            "settled",
        );
        match map_payment_event(&db, &payment)
            .unwrap()
            .expect("event for our settled outbound payment")
        {
            Event::PaymentSuccessful {
                quote_id: event_quote_id,
                details,
            } => {
                assert_eq!(event_quote_id, quote_id);
                assert_eq!(details.status, MeltQuoteState::Paid);
                // 5000 sats principal + 3 sats fee
                assert_eq!(details.total_spent.to_sat().expect("sat amount"), 5_003);
                assert!(details.payment_proof.is_some());
            }
            other => panic!("expected PaymentSuccessful, got {other:?}"),
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn outbound_failed_payment_becomes_payment_failed() {
        let path = test_event_db_path();
        let db = QuoteDatabase::new(&path).expect("create quote database");
        let hash = [9_u8; 32];
        let quote_id: QuoteId = "018f8b3e-8c1a-7d2e-9f4b-6a1c3e5d7f91"
            .parse()
            .expect("valid quote id");
        db.begin_melt_attempt(&hash, &quote_id, &CurrencyUnit::Sat)
            .expect("insert melt quote id");

        let payment = test_payment(
            PaymentDirection::Outbound,
            PaymentStatus::Failed,
            hash,
            5_000,
            0,
            "timed out",
        );
        match map_payment_event(&db, &payment)
            .unwrap()
            .expect("event for our failed outbound payment")
        {
            Event::PaymentFailed {
                quote_id: event_quote_id,
                reason,
            } => {
                assert_eq!(event_quote_id, quote_id);
                assert_eq!(reason, "timed out");
            }
            other => panic!("expected PaymentFailed, got {other:?}"),
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn outbound_pending_payment_is_ignored_until_terminal() {
        let path = test_event_db_path();
        let db = QuoteDatabase::new(&path).expect("create quote database");
        let hash = [12_u8; 32];
        let quote_id: QuoteId = "018f8b3e-8c1a-7d2e-9f4b-6a1c3e5d7f92"
            .parse()
            .expect("valid quote id");
        db.begin_melt_attempt(&hash, &quote_id, &CurrencyUnit::Sat)
            .expect("insert melt quote id");

        let payment = test_payment(
            PaymentDirection::Outbound,
            PaymentStatus::Pending,
            hash,
            5_000,
            0,
            "in flight",
        );
        assert!(map_payment_event(&db, &payment).unwrap().is_none());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn completed_inbound_without_amount_is_an_error() {
        let path = test_event_db_path();
        let db = QuoteDatabase::new(&path).expect("create quote database");
        let hash = [14_u8; 32];
        db.insert_mint_quote_and_payment_id(&hash, "lnbc1incoming-invoice", "0001-ln_test")
            .expect("insert mint quote");

        let mut payment = test_payment(
            PaymentDirection::Inbound,
            PaymentStatus::Completed,
            hash,
            21_000,
            0,
            "received",
        );
        payment.amount = None;

        assert!(
            map_payment_event(&db, &payment).is_err(),
            "a settled inbound payment without an amount must not be emitted as zero"
        );
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn completed_outbound_without_amount_is_an_error() {
        let path = test_event_db_path();
        let db = QuoteDatabase::new(&path).expect("create quote database");
        let hash = [15_u8; 32];
        let quote_id: QuoteId = "018f8b3e-8c1a-7d2e-9f4b-6a1c3e5d7f92"
            .parse()
            .expect("valid quote id");
        db.begin_melt_attempt(&hash, &quote_id, &CurrencyUnit::Sat)
            .expect("begin melt attempt");

        let mut payment = test_payment(
            PaymentDirection::Outbound,
            PaymentStatus::Completed,
            hash,
            5_000,
            3,
            "settled",
        );
        payment.amount = None;

        assert!(
            map_payment_event(&db, &payment).is_err(),
            "a settled outbound payment without an amount must not settle at zero"
        );
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn outgoing_response_paid_reports_amount_plus_fees() {
        let payment = test_payment(
            PaymentDirection::Outbound,
            PaymentStatus::Completed,
            [16_u8; 32],
            5_000,
            3,
            "settled",
        );
        let response = LexeBackend::outgoing_response(
            PaymentIdentifier::PaymentHash([16_u8; 32]),
            &CurrencyUnit::Sat,
            &payment,
        )
        .expect("paid melt with an amount is reported");
        assert_eq!(response.status, MeltQuoteState::Paid);
        assert_eq!(response.total_spent.to_sat().expect("sat unit"), 5_003);
        assert!(response.payment_proof.is_some());
    }

    #[test]
    fn outgoing_response_paid_without_amount_is_an_error() {
        let mut payment = test_payment(
            PaymentDirection::Outbound,
            PaymentStatus::Completed,
            [17_u8; 32],
            5_000,
            3,
            "settled",
        );
        payment.amount = None;

        assert!(LexeBackend::outgoing_response(
            PaymentIdentifier::PaymentHash([17_u8; 32]),
            &CurrencyUnit::Sat,
            &payment,
        )
        .is_err());
    }
}

#[cfg(test)]
#[path = "backend_tests.rs"]
mod regression_tests;
