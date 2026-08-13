use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bip39::Mnemonic;
use cdk_common::bitcoin::hashes::Hash;
use cdk_common::nuts::{CurrencyUnit, MeltQuoteState};
use cdk_common::payment::{
    Bolt11Settings, CreateIncomingPaymentResponse, Error, Event, IncomingPaymentOptions,
    MakePaymentResponse, MintPayment, OutgoingPaymentOptions, PaymentIdentifier,
    PaymentQuoteResponse, SettingsResponse, WaitPaymentResponse,
};
use cdk_common::util::unix_time;
use cdk_common::{Amount, Bolt11Invoice};
use futures_core::Stream;
use spark_wallet::{
    DefaultSigner, InvoiceDescription, LightningReceiveRequestStatus, LightningSendPayment,
    LightningSendStatus, ListTransfersRequest, Network, PagingFilter, SparkSigner,
    SparkSignerAdapter, SparkWallet, SparkWalletConfig, TransferDirection, TransferId,
    TransferStatus, WalletBuilder, WalletEvent, WalletTransfer,
};
use tokio::sync::{broadcast, mpsc, watch, Mutex};
use tokio_stream::wrappers::ReceiverStream;

use crate::database::QuoteDatabase;
use crate::settings::BackendConfig;

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

/// Event stream that releases its Spark subscription activity on completion or drop.
struct PaymentEventStream {
    receiver: ReceiverStream<Event>,
    activity: Arc<PaymentEventStreamActivity>,
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

/// Low-level Spark wallet backend implementation.
pub struct SparkBackend {
    wallet: Arc<SparkWallet>,
    active_payment_streams: Arc<AtomicUsize>,
    initial_event_receiver: Arc<Mutex<Option<broadcast::Receiver<WalletEvent>>>>,
    event_cancel: watch::Sender<()>,
    db: QuoteDatabase,
    shutdown_sender: watch::Sender<()>,
}

impl SparkBackend {
    fn ensure_supported_unit(unit: &CurrencyUnit) -> Result<(), Error> {
        if matches!(unit, CurrencyUnit::Sat) {
            Ok(())
        } else {
            Err(Error::UnsupportedUnit)
        }
    }

    /// Connect a wallet directly to the Spark operators and SSP.
    pub async fn new(config: BackendConfig) -> anyhow::Result<Self> {
        if config.mnemonic.is_empty() {
            anyhow::bail!("Mnemonic seed is required");
        }

        let mnemonic = Mnemonic::parse(&config.mnemonic)
            .map_err(|e| anyhow::anyhow!("Invalid BIP-39 mnemonic: {e}"))?;
        let seed = mnemonic.to_seed("");
        let signer = DefaultSigner::new(&seed, Network::Mainnet)
            .map_err(|e| anyhow::anyhow!("Failed to initialize Spark signer: {e}"))?;
        let spark_signer: Arc<dyn SparkSigner> =
            Arc::new(SparkSignerAdapter::new(Arc::new(signer)));

        let mut wallet_config = SparkWalletConfig::default_config(Network::Mainnet);
        wallet_config.leaf_auto_optimize_enabled = true;
        wallet_config.leaf_optimization_options.multiplicity = 5;
        wallet_config.max_concurrent_claims = 5;

        let (shutdown_sender, shutdown_receiver) = watch::channel(());
        let (event_cancel, _) = watch::channel(());
        let wallet = Arc::new(
            WalletBuilder::new(wallet_config, spark_signer)
                .with_cancellation_token(shutdown_receiver)
                .build()
                .await
                .map_err(|e| anyhow::anyhow!("Spark wallet connection failed: {e}"))?,
        );

        // Subscribe before starting background processing so no startup events are lost.
        let event_receiver = wallet.subscribe_events();
        wallet.start_background_processing().await;

        tracing::info!(
            identity_public_key = %wallet.get_identity_public_key(),
            "Connected directly to Spark"
        );

        match wallet.get_balance().await {
            Ok(balance) => tracing::info!("Current Spark balance: {} sats", balance),
            Err(e) => tracing::warn!("Failed to get current Spark balance: {}", e),
        }

        let data_dir = PathBuf::from(&config.data_dir);
        std::fs::create_dir_all(&data_dir)
            .map_err(|e| anyhow::anyhow!("Failed to create data directory: {e}"))?;

        let db_path = data_dir.join("quotes.db");
        let db = QuoteDatabase::new(db_path)?;

        Ok(Self {
            wallet,
            active_payment_streams: Arc::new(AtomicUsize::new(0)),
            initial_event_receiver: Arc::new(Mutex::new(Some(event_receiver))),
            event_cancel,
            db,
            shutdown_sender,
        })
    }

    /// Stop Spark background processing.
    pub async fn disconnect(&self) -> anyhow::Result<()> {
        self.cancel_payment_event_stream();
        let _ = self.shutdown_sender.send(());
        tracing::info!("Spark wallet disconnected");
        Ok(())
    }

    fn store_mint_quote(
        &self,
        payment_hash: &[u8; 32],
        payment_request: &str,
        payment_id: &str,
    ) -> Result<(), Error> {
        self.db
            .insert_mint_quote_and_payment_id(payment_hash, payment_request, payment_id)
            .map_err(|e| Error::Custom(e.to_string()))
    }

    fn store_melt_quote(
        &self,
        payment_hash: &[u8; 32],
        payment_request: &str,
    ) -> Result<(), Error> {
        self.db
            .insert_melt_quote(payment_hash, payment_request)
            .map_err(|e| Error::Custom(e.to_string()))
    }

    fn get_or_create_melt_transfer_id(&self, payment_hash: &[u8; 32]) -> Result<TransferId, Error> {
        let candidate = TransferId::generate().to_string();
        let transfer_id = self
            .db
            .get_or_insert_melt_transfer_id(payment_hash, &candidate)
            .map_err(|e| Error::Custom(e.to_string()))?;
        TransferId::from_str(&transfer_id)
            .map_err(|e| Error::Custom(format!("Invalid stored Spark transfer ID: {e}")))
    }

    fn get_mint_quote(&self, payment_hash: &[u8; 32]) -> Result<Option<String>, Error> {
        self.db
            .get_mint_quote(payment_hash)
            .map_err(|e| Error::Custom(e.to_string()))
    }

    fn get_melt_quote(&self, payment_hash: &[u8; 32]) -> Result<Option<String>, Error> {
        self.db
            .get_melt_quote(payment_hash)
            .map_err(|e| Error::Custom(e.to_string()))
    }

    fn invoice_amount_sats(invoice: &Bolt11Invoice) -> Result<u64, Error> {
        let amount_msat = invoice
            .amount_milli_satoshis()
            .ok_or(Error::AmountMismatch)?;
        let amount_sats = amount_msat.div_ceil(1000);
        if amount_sats == 0 {
            return Err(Error::AmountMismatch);
        }
        Ok(amount_sats)
    }

    fn payment_hash(payment_identifier: &PaymentIdentifier) -> Result<&[u8; 32], Error> {
        match payment_identifier {
            PaymentIdentifier::PaymentHash(hash) => Ok(hash),
            _ => Err(Error::Custom(
                "Unsupported payment identifier type".to_string(),
            )),
        }
    }

    fn send_status(status: LightningSendStatus, has_preimage: bool) -> MeltQuoteState {
        if has_preimage || matches!(status, LightningSendStatus::LightningPaymentSucceeded) {
            return MeltQuoteState::Paid;
        }

        match status {
            LightningSendStatus::LightningPaymentFailed
            | LightningSendStatus::TransferFailed
            | LightningSendStatus::PreimageProvidingFailed
            | LightningSendStatus::UserSwapReturnFailed
            | LightningSendStatus::UserSwapReturned => MeltQuoteState::Unpaid,
            _ => MeltQuoteState::Pending,
        }
    }

    fn outgoing_response(
        payment_identifier: PaymentIdentifier,
        invoice_amount_sats: u64,
        payment: &LightningSendPayment,
    ) -> MakePaymentResponse {
        let status = Self::send_status(payment.status, payment.payment_preimage.is_some());
        let total_spent = Amount::new(
            invoice_amount_sats.saturating_add(payment.fee_sat),
            CurrencyUnit::Sat,
        );

        MakePaymentResponse {
            payment_lookup_id: payment_identifier,
            payment_proof: payment.payment_preimage.clone(),
            status,
            total_spent,
        }
    }

    fn event_to_cdk(event: WalletEvent) -> Option<Event> {
        let WalletEvent::TransferClaimed(transfer) = event else {
            return None;
        };
        if transfer.direction != TransferDirection::Incoming {
            return None;
        }

        let payment_hash = transfer
            .user_request
            .as_ref()
            .and_then(|request| request.get_lightning_invoice())
            .and_then(|invoice| Bolt11Invoice::from_str(&invoice).ok())
            .map(|invoice| invoice.payment_hash().to_byte_array())
            .or_else(|| {
                transfer
                    .htlc_preimage_request
                    .as_ref()
                    .map(|request| request.payment_hash.to_byte_array())
            })?;

        Some(Event::PaymentReceived(WaitPaymentResponse {
            payment_id: transfer.id.to_string(),
            payment_identifier: PaymentIdentifier::PaymentHash(payment_hash),
            payment_amount: Amount::new(transfer.total_value_sat, CurrencyUnit::Sat),
        }))
    }

    async fn find_transfer_for_invoice(
        &self,
        invoice: &str,
        direction: TransferDirection,
    ) -> Result<Option<WalletTransfer>, Error> {
        let invoice = invoice.to_lowercase();
        let mut paging = Some(PagingFilter::default());

        while let Some(filter) = paging {
            let result = self
                .wallet
                .list_transfers(ListTransfersRequest {
                    paging: Some(filter),
                    transfer_ids: vec![],
                })
                .await
                .map_err(|e| Error::Lightning(Box::new(e)))?;
            paging = result.next;

            if let Some(transfer) = result.items.into_iter().find(|transfer| {
                transfer.direction == direction
                    && transfer
                        .user_request
                        .as_ref()
                        .and_then(|request| request.get_lightning_invoice())
                        .is_some_and(|request_invoice| request_invoice == invoice)
            }) {
                return Ok(Some(transfer));
            }
        }

        Ok(None)
    }

    async fn find_outgoing_transfer_by_id(
        &self,
        transfer_id: TransferId,
    ) -> Result<Option<WalletTransfer>, Error> {
        let result = self
            .wallet
            .list_transfers(ListTransfersRequest {
                paging: None,
                transfer_ids: vec![transfer_id],
            })
            .await
            .map_err(|e| Error::Lightning(Box::new(e)))?;

        Ok(result
            .items
            .into_iter()
            .find(|transfer| transfer.direction == TransferDirection::Outgoing))
    }
}

#[async_trait]
impl MintPayment for SparkBackend {
    type Err = Error;

    async fn get_settings(&self) -> Result<SettingsResponse, Self::Err> {
        Ok(SettingsResponse {
            unit: "sat".to_string(),
            bolt11: Some(Bolt11Settings {
                mpp: false,
                amountless: false,
                invoice_description: false,
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

        let description = opts.description.as_deref().unwrap_or("Payment");
        let expiry_secs = opts
            .unix_expiry
            .map(|expiry| {
                expiry
                    .checked_sub(unix_time())
                    .ok_or(Error::AmountMismatch)
                    .and_then(|seconds| u32::try_from(seconds).map_err(|_| Error::AmountMismatch))
            })
            .transpose()?;

        // Do not embed a Spark address in CDK BOLT11 invoices. A direct Spark
        // transfer does not settle the invoice payment hash, which CDK uses as
        // its payment identifier.
        let payment = self
            .wallet
            .create_lightning_invoice(
                amount_sats,
                Some(InvoiceDescription::Memo(description.to_string())),
                None,
                expiry_secs,
                false,
            )
            .await
            .map_err(|e| Error::Lightning(Box::new(e)))?;

        let invoice = Bolt11Invoice::from_str(&payment.invoice)?;
        let payment_hash = invoice.payment_hash().to_byte_array();
        let payment_identifier = PaymentIdentifier::PaymentHash(payment_hash);
        self.store_mint_quote(&payment_hash, &payment.invoice, &payment.id)?;

        Ok(CreateIncomingPaymentResponse {
            request_lookup_id: payment_identifier,
            request: payment.invoice,
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

        let bolt11 = opts.bolt11.to_string();
        let amount_sats = Self::invoice_amount_sats(&opts.bolt11)?;
        let fee_sats = self
            .wallet
            .fetch_lightning_send_fee_estimate(&bolt11, None)
            .await
            .map_err(|e| Error::Lightning(Box::new(e)))?;
        let payment_hash = opts.bolt11.payment_hash().to_byte_array();
        self.store_melt_quote(&payment_hash, &bolt11)?;

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

        let bolt11 = opts.bolt11.to_string();
        let amount_sats = Self::invoice_amount_sats(&opts.bolt11)?;
        let payment_hash = opts.bolt11.payment_hash().to_byte_array();
        let payment_identifier = PaymentIdentifier::PaymentHash(payment_hash);
        self.store_melt_quote(&payment_hash, &bolt11)?;

        if let Some(payment_id) = self
            .db
            .get_melt_payment_id(&payment_hash)
            .map_err(|e| Error::Custom(e.to_string()))?
        {
            if let Some(payment) = self
                .wallet
                .fetch_lightning_send_payment(&payment_id)
                .await
                .map_err(|e| Error::Lightning(Box::new(e)))?
            {
                return Ok(Self::outgoing_response(
                    payment_identifier,
                    amount_sats,
                    &payment,
                ));
            }
        }

        let transfer_id = self.get_or_create_melt_transfer_id(&payment_hash)?;
        let max_fee_sats = opts
            .max_fee_amount
            .as_ref()
            .map(Amount::to_sat)
            .transpose()?;

        let result = self
            .wallet
            .pay_lightning_invoice(&bolt11, None, max_fee_sats, false, Some(transfer_id))
            .await
            .map_err(|e| Error::Lightning(Box::new(e)))?;

        let Some(payment) = result.lightning_payment else {
            tracing::warn!(
                transfer_id = %result.transfer.id,
                "Spark recovered the outgoing transfer without an SSP payment record"
            );
            return Ok(MakePaymentResponse {
                payment_lookup_id: payment_identifier,
                payment_proof: None,
                status: MeltQuoteState::Pending,
                total_spent: Amount::new(amount_sats, CurrencyUnit::Sat),
            });
        };

        self.db
            .insert_melt_payment_id(&payment_hash, &payment.id)
            .map_err(|e| Error::Custom(e.to_string()))?;

        Ok(Self::outgoing_response(
            payment_identifier,
            amount_sats,
            &payment,
        ))
    }

    async fn wait_payment_event(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Event> + Send>>, Self::Err> {
        let mut spark_events = self
            .initial_event_receiver
            .lock()
            .await
            .take()
            .unwrap_or_else(|| self.wallet.subscribe_events());
        let mut cancel = self.event_cancel.subscribe();
        let (sender, receiver) = mpsc::channel(100);
        let stream = PaymentEventStream::new(receiver, Arc::clone(&self.active_payment_streams));
        let activity = Arc::clone(&stream.activity);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.changed() => break,
                    _ = sender.closed() => break,
                    event = spark_events.recv() => {
                        match event {
                            Ok(event) => {
                                if let Some(event) = SparkBackend::event_to_cdk(event) {
                                    if sender.send(event).await.is_err() {
                                        break;
                                    }
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                tracing::warn!(skipped, "Spark payment event listener lagged");
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
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
        let payment_hash = Self::payment_hash(payment_identifier)?;
        let Some(payment_id) = self
            .db
            .get_mint_payment_id(payment_hash)
            .map_err(|e| Error::Custom(e.to_string()))?
        else {
            // Quotes created by the former high-level backend do not have an
            // SSP request ID. Once paid, recover them through transfer history.
            let Some(invoice) = self.get_mint_quote(payment_hash)? else {
                return Ok(vec![]);
            };
            let Some(transfer) = self
                .find_transfer_for_invoice(&invoice, TransferDirection::Incoming)
                .await?
            else {
                return Ok(vec![]);
            };
            if transfer.status != TransferStatus::Completed {
                return Ok(vec![]);
            }
            return Ok(vec![WaitPaymentResponse {
                payment_id: transfer.id.to_string(),
                payment_identifier: payment_identifier.clone(),
                payment_amount: Amount::new(transfer.total_value_sat, CurrencyUnit::Sat),
            }]);
        };

        let Some(payment) = self
            .wallet
            .fetch_lightning_receive_payment(&payment_id)
            .await
            .map_err(|e| Error::Lightning(Box::new(e)))?
        else {
            return Ok(vec![]);
        };

        if payment.status != LightningReceiveRequestStatus::TransferCompleted {
            return Ok(vec![]);
        }

        let amount_sats = match payment.transfer_amount_sat {
            Some(amount) => amount,
            None => {
                let invoice = self
                    .get_mint_quote(payment_hash)?
                    .ok_or_else(|| Error::Custom("Incoming quote not found".to_string()))?;
                Self::invoice_amount_sats(&Bolt11Invoice::from_str(&invoice)?)?
            }
        };

        Ok(vec![WaitPaymentResponse {
            payment_id: payment
                .transfer_id
                .map(|id| id.to_string())
                .unwrap_or(payment.id),
            payment_identifier: payment_identifier.clone(),
            payment_amount: Amount::new(amount_sats, CurrencyUnit::Sat),
        }])
    }

    async fn check_outgoing_payment(
        &self,
        payment_identifier: &PaymentIdentifier,
    ) -> Result<MakePaymentResponse, Self::Err> {
        let payment_hash = Self::payment_hash(payment_identifier)?;
        let invoice = self
            .get_melt_quote(payment_hash)?
            .ok_or_else(|| Error::Custom("Outgoing payment not found".to_string()))?;
        let amount_sats = Self::invoice_amount_sats(&Bolt11Invoice::from_str(&invoice)?)?;
        let payment_id = self
            .db
            .get_melt_payment_id(payment_hash)
            .map_err(|e| Error::Custom(e.to_string()))?;

        let Some(payment_id) = payment_id else {
            let transfer_id = self
                .db
                .get_melt_transfer_id(payment_hash)
                .map_err(|e| Error::Custom(e.to_string()))?;

            let transfer = match transfer_id.as_deref() {
                Some(transfer_id) => {
                    let transfer_id = TransferId::from_str(transfer_id).map_err(|e| {
                        Error::Custom(format!("Invalid stored Spark transfer ID: {e}"))
                    })?;
                    self.find_outgoing_transfer_by_id(transfer_id).await?
                }
                None => {
                    // Records created before transfer IDs were persisted can only
                    // be recovered by scanning transfer history for the invoice.
                    self.find_transfer_for_invoice(&invoice, TransferDirection::Outgoing)
                        .await?
                }
            };

            if let Some(transfer) = transfer {
                let payment_proof = transfer
                    .user_request
                    .as_ref()
                    .and_then(|request| request.get_lightning_preimage());
                let status = if payment_proof.is_some() {
                    MeltQuoteState::Paid
                } else {
                    MeltQuoteState::Pending
                };
                return Ok(MakePaymentResponse {
                    payment_lookup_id: payment_identifier.clone(),
                    payment_proof,
                    status,
                    total_spent: Amount::new(transfer.total_value_sat, CurrencyUnit::Sat),
                });
            }

            return Ok(MakePaymentResponse {
                payment_lookup_id: payment_identifier.clone(),
                payment_proof: None,
                status: if transfer_id.is_some() {
                    MeltQuoteState::Pending
                } else {
                    MeltQuoteState::Unpaid
                },
                total_spent: Amount::new(0, CurrencyUnit::Sat),
            });
        };

        let Some(payment) = self
            .wallet
            .fetch_lightning_send_payment(&payment_id)
            .await
            .map_err(|e| Error::Lightning(Box::new(e)))?
        else {
            return Ok(MakePaymentResponse {
                payment_lookup_id: payment_identifier.clone(),
                payment_proof: None,
                status: MeltQuoteState::Pending,
                total_spent: Amount::new(0, CurrencyUnit::Sat),
            });
        };

        Ok(Self::outgoing_response(
            payment_identifier.clone(),
            amount_sats,
            &payment,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::{PaymentEventStream, SparkBackend};
    use cdk_common::nuts::{CurrencyUnit, MeltQuoteState};
    use cdk_common::payment::Error;
    use spark_wallet::LightningSendStatus;

    #[test]
    fn accepts_only_the_advertised_sat_unit() {
        assert!(SparkBackend::ensure_supported_unit(&CurrencyUnit::Sat).is_ok());
        assert!(matches!(
            SparkBackend::ensure_supported_unit(&CurrencyUnit::Msat),
            Err(Error::UnsupportedUnit)
        ));
        assert!(matches!(
            SparkBackend::ensure_supported_unit(&CurrencyUnit::Usd),
            Err(Error::UnsupportedUnit)
        ));
    }

    #[test]
    fn tracks_each_payment_event_stream_until_completion_or_drop() {
        let active_streams = Arc::new(AtomicUsize::new(0));
        let (_first_sender, first_receiver) = tokio::sync::mpsc::channel(1);
        let (_second_sender, second_receiver) = tokio::sync::mpsc::channel(1);
        let first = PaymentEventStream::new(first_receiver, Arc::clone(&active_streams));
        let second = PaymentEventStream::new(second_receiver, Arc::clone(&active_streams));

        assert_eq!(active_streams.load(Ordering::Relaxed), 2);

        first.activity.deactivate();
        assert_eq!(active_streams.load(Ordering::Relaxed), 1);

        drop(first);
        assert_eq!(active_streams.load(Ordering::Relaxed), 1);

        drop(second);
        assert_eq!(active_streams.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn maps_terminal_send_statuses() {
        assert_eq!(
            SparkBackend::send_status(LightningSendStatus::LightningPaymentSucceeded, false),
            MeltQuoteState::Paid
        );
        assert_eq!(
            SparkBackend::send_status(LightningSendStatus::Created, true),
            MeltQuoteState::Paid
        );
        assert_eq!(
            SparkBackend::send_status(LightningSendStatus::LightningPaymentFailed, false),
            MeltQuoteState::Unpaid
        );
        assert_eq!(
            SparkBackend::send_status(LightningSendStatus::Created, false),
            MeltQuoteState::Pending
        );
    }
}
