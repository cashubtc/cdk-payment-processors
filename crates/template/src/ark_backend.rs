use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ark::lightning::{Invoice, PaymentHash};
use async_trait::async_trait;
use bark::persist::sqlite::SqliteClient;
use bitcoin::hashes::Hash as BitcoinHash;
use cdk_common::amount::Amount;
use cdk_common::nuts::CurrencyUnit;
use cdk_common::payment::{
    Bolt11Settings, CreateIncomingPaymentResponse, Event, IncomingPaymentOptions,
    MakePaymentResponse, MintPayment, OutgoingPaymentOptions, PaymentIdentifier,
    PaymentQuoteResponse, SettingsResponse, WaitPaymentResponse,
};
use cdk_common::MeltQuoteState;
use futures::stream::{self, Stream, StreamExt};
use tracing::{debug, info, warn};

use crate::settings::BackendConfig;

/// Ark payment processor backend using the Bark wallet library
pub struct ArkBackend {
    wallet: Arc<bark::Wallet>,
    wait_invoice_active: Arc<AtomicBool>,
}

impl ArkBackend {
    /// Create a new Ark backend with initialized wallet
    pub async fn new(config: &BackendConfig) -> anyhow::Result<Self> {
        info!("Initializing Ark backend");

        // Parse the mnemonic
        let mnemonic = config
            .mnemonic
            .parse::<bip39::Mnemonic>()
            .map_err(|e| anyhow::anyhow!("Invalid mnemonic: {}", e))?;

        // Parse the network
        let network = match config.network.to_lowercase().as_str() {
            "mainnet" => bitcoin::Network::Bitcoin,
            "testnet" => bitcoin::Network::Testnet,
            "signet" => bitcoin::Network::Signet,
            "regtest" => bitcoin::Network::Regtest,
            _ => {
                warn!("Unknown network '{}', defaulting to Signet", config.network);
                bitcoin::Network::Signet
            }
        };

        // Build bark config
        let bark_config = bark::Config {
            server_address: config.server_address.clone(),
            esplora_address: Some(config.esplora_address.clone()),
            ..bark::Config::network_default(network)
        };

        // Create data directory if it doesn't exist
        let data_dir = PathBuf::from(&config.data_dir);
        std::fs::create_dir_all(&data_dir)
            .map_err(|e| anyhow::anyhow!("Failed to create data directory: {}", e))?;

        // Open SQLite database
        let db_path = data_dir.join("db.sqlite");
        let db = Arc::new(
            SqliteClient::open(&db_path)
                .map_err(|e| anyhow::anyhow!("Failed to open SQLite database: {}", e))?,
        );

        // Try to open existing wallet first, fall back to creating new one
        let wallet = match bark::Wallet::open(&mnemonic, db.clone(), bark_config.clone()).await {
            Ok(wallet) => {
                info!("Opened existing Ark wallet");
                wallet
            }
            Err(e) => {
                info!("Creating new Ark wallet (open failed: {})", e);
                bark::Wallet::create(&mnemonic, network, bark_config, db, false)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to create wallet: {}", e))?
            }
        };

        info!("Ark backend initialized successfully");

        Ok(Self {
            wallet: Arc::new(wallet),
            wait_invoice_active: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Convert bitcoin::Amount to CDK Amount (instance method)
    fn btc_amount_to_cdk(&self, amount: bitcoin::Amount) -> Amount<CurrencyUnit> {
        Amount::new(amount.to_sat(), CurrencyUnit::Sat)
    }

    /// Convert bitcoin::Amount to CDK Amount (static method)
    fn btc_amount_to_cdk_static(amount: bitcoin::Amount) -> Amount<CurrencyUnit> {
        Amount::new(amount.to_sat(), CurrencyUnit::Sat)
    }

    /// Get zero CDK amount
    fn cdk_amount_zero() -> Amount<CurrencyUnit> {
        Amount::new(0, CurrencyUnit::Sat)
    }
}

#[async_trait]
impl MintPayment for ArkBackend {
    type Err = cdk_common::payment::Error;

    async fn get_settings(&self) -> Result<SettingsResponse, Self::Err> {
        debug!("Getting Ark wallet settings");
        Ok(SettingsResponse {
            unit: "sat".to_string(),
            bolt11: Some(Bolt11Settings {
                mpp: false,
                amountless: false,
                invoice_description: true,
            }),
            bolt12: None,
            onchain: None,
            custom: Default::default(),
        })
    }

    async fn create_incoming_payment_request(
        &self,
        options: IncomingPaymentOptions,
    ) -> Result<CreateIncomingPaymentResponse, Self::Err> {
        debug!("Creating incoming payment request");

        // Only support BOLT11 for now
        let bolt11_options = match options {
            IncomingPaymentOptions::Bolt11(opts) => opts,
            _ => {
                return Err(cdk_common::payment::Error::UnsupportedPaymentOption);
            }
        };

        // Only support sat unit
        if bolt11_options.amount.unit().to_string() != "sat" {
            return Err(cdk_common::payment::Error::UnsupportedUnit);
        }

        // Convert amount to bitcoin::Amount - use to_u64() to get raw value from Amount<()>
        let amount = bitcoin::Amount::from_sat(bolt11_options.amount.to_u64());

        // Generate BOLT11 invoice using bark wallet
        let invoice = self
            .wallet
            .bolt11_invoice(amount, bolt11_options.description)
            .await
            .map_err(|e| {
                cdk_common::payment::Error::Custom(format!("Failed to create invoice: {}", e))
            })?;

        // Extract payment hash from the invoice - bark returns lightning_invoice::Bolt11Invoice
        let payment_hash_bytes = *invoice.payment_hash().as_ref();
        let payment_identifier = PaymentIdentifier::PaymentHash(payment_hash_bytes);

        // Get expiry - convert Duration to seconds
        let expiry = Some(invoice.expiry_time().as_secs());

        // Convert invoice to string
        let invoice_str = invoice.to_string();

        info!(
            "Created BOLT11 invoice for {} sat, payment_hash: {}",
            amount.to_sat(),
            hex::encode(payment_hash_bytes)
        );

        Ok(CreateIncomingPaymentResponse {
            request_lookup_id: payment_identifier,
            request: invoice_str,
            expiry,
            extra_json: None,
        })
    }

    async fn get_payment_quote(
        &self,
        unit: &CurrencyUnit,
        options: OutgoingPaymentOptions,
    ) -> Result<PaymentQuoteResponse, Self::Err> {
        debug!("Getting payment quote");

        // Only support sat unit
        if unit.to_string() != "sat" {
            return Err(cdk_common::payment::Error::UnsupportedUnit);
        }

        // Only support BOLT11 for now
        let bolt11_options = match options {
            OutgoingPaymentOptions::Bolt11(opts) => opts,
            _ => {
                return Err(cdk_common::payment::Error::UnsupportedPaymentOption);
            }
        };

        // bolt11_options.bolt11 is already a parsed Bolt11Invoice from cdk_common
        let invoice = &bolt11_options.bolt11;

        let amount_msat = invoice.amount_milli_satoshis().ok_or_else(|| {
            cdk_common::payment::Error::Custom("Invoice has no amount".to_string())
        })?;
        let amount_sat = amount_msat / 1000;

        // For Ark, fees are typically minimal for lightning payments
        // We'll estimate a small fee (e.g., 0.1% or minimum 1 sat)
        let fee_sats = std::cmp::max(1, amount_sat / 1000);

        // Extract payment hash for lookup ID
        let payment_hash = *invoice.payment_hash().as_ref();
        let request_lookup_id = PaymentIdentifier::PaymentHash(payment_hash);

        debug!("Payment quote: {} sat + {} sat fee", amount_sat, fee_sats);

        Ok(PaymentQuoteResponse {
            request_lookup_id: Some(request_lookup_id),
            amount: Amount::new(amount_sat, CurrencyUnit::Sat),
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
        debug!("Making payment");

        // Only support sat unit
        if unit.to_string() != "sat" {
            return Err(cdk_common::payment::Error::UnsupportedUnit);
        }

        // Only support BOLT11 for now
        let bolt11_options = match options {
            OutgoingPaymentOptions::Bolt11(opts) => opts,
            _ => {
                return Err(cdk_common::payment::Error::UnsupportedPaymentOption);
            }
        };

        // bolt11_options.bolt11 is already a parsed invoice
        let invoice = &bolt11_options.bolt11;

        // Extract payment hash
        let payment_hash = *invoice.payment_hash().as_ref();
        let payment_lookup_id = PaymentIdentifier::PaymentHash(payment_hash);

        // Get the amount from the invoice
        let amount_msat = invoice.amount_milli_satoshis().ok_or_else(|| {
            cdk_common::payment::Error::Custom("Invoice has no amount".to_string())
        })?;
        let amount_sat = amount_msat / 1000;

        // Pay the lightning invoice using bark wallet
        let invoice_str = invoice.to_string();
        let _lightning_send = self
            .wallet
            .pay_lightning_invoice(invoice_str.as_str(), None)
            .await
            .map_err(|e| {
                cdk_common::payment::Error::Custom(format!("Failed to pay invoice: {}", e))
            })?;

        // Check the payment status to get the preimage
        let bark_payment_hash = PaymentHash::from(payment_hash);
        let preimage = self
            .wallet
            .check_lightning_payment(bark_payment_hash, true)
            .await
            .map_err(|e| {
                cdk_common::payment::Error::Custom(format!("Failed to check payment: {}", e))
            })?;

        // Convert preimage to hex if available - bark's Preimage can be converted to bytes
        let payment_proof =
            preimage.and_then(|p| p.preimage.map(|preimage| hex::encode(preimage.as_ref())));

        // Calculate total spent (amount + estimated fee)
        let fee_sats = std::cmp::max(1, amount_sat / 1000);
        let total_spent = Amount::new(amount_sat + fee_sats, CurrencyUnit::Sat);

        info!(
            "Payment completed: {} sat, payment_hash: {}, preimage: {:?}",
            amount_sat,
            hex::encode(payment_hash),
            payment_proof.as_ref().map(|_| "present")
        );

        Ok(MakePaymentResponse {
            payment_lookup_id,
            payment_proof,
            status: MeltQuoteState::Paid,
            total_spent,
        })
    }

    async fn wait_payment_event(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Event> + Send>>, Self::Err> {
        debug!("Starting payment event stream");
        self.wait_invoice_active.store(true, Ordering::SeqCst);

        let wallet = self.wallet.clone();
        let active = self.wait_invoice_active.clone();

        // Create a stream that polls for incoming payments
        let stream = stream::unfold((wallet, active, false), |(wallet, active, _)| async move {
            // Check if we should stop
            if !active.load(Ordering::SeqCst) {
                return None;
            }

            // Wait for the polling interval
            tokio::time::sleep(Duration::from_secs(5)).await;

            // Try to claim all lightning receives (non-blocking)
            if let Err(e) = wallet.try_claim_all_lightning_receives(false).await {
                debug!("Failed to claim lightning receives: {}", e);
            }

            // Get pending lightning receives
            let pending = match wallet.pending_lightning_receives().await {
                Ok(pending) => pending,
                Err(e) => {
                    debug!("Failed to get pending receives: {}", e);
                    return Some((None, (wallet, active, false)));
                }
            };

            // Check for completed receives
            for receive in pending {
                // If the receive has finished_at set, it's complete
                if receive.finished_at.is_some() {
                    let payment_hash = receive.payment_hash;
                    let amount = receive
                        .invoice
                        .amount_milli_satoshis()
                        .map(|msat| {
                            Self::btc_amount_to_cdk_static(bitcoin::Amount::from_sat(msat / 1000))
                        })
                        .unwrap_or(Self::cdk_amount_zero());

                    // Convert PaymentHash to bytes for PaymentIdentifier
                    let payment_hash_bytes: [u8; 32] = payment_hash.into();
                    let event = Event::PaymentReceived(WaitPaymentResponse {
                        payment_identifier: PaymentIdentifier::PaymentHash(payment_hash_bytes),
                        payment_amount: amount,
                        payment_id: hex::encode(payment_hash_bytes),
                    });

                    return Some((Some(event), (wallet, active, false)));
                }
            }

            Some((None, (wallet, active, false)))
        })
        .filter_map(|event| async move { event });

        Ok(Box::pin(stream))
    }

    async fn check_incoming_payment_status(
        &self,
        payment_identifier: &PaymentIdentifier,
    ) -> Result<Vec<WaitPaymentResponse>, Self::Err> {
        debug!("Checking incoming payment status");

        // Extract payment hash from identifier
        let payment_hash = match payment_identifier {
            PaymentIdentifier::PaymentHash(hash) => PaymentHash::from(*hash),
            _ => {
                return Err(cdk_common::payment::Error::Custom(
                    "Unsupported payment identifier type".to_string(),
                ));
            }
        };

        // Get the receive status
        let receive = self
            .wallet
            .lightning_receive_status(payment_hash)
            .await
            .map_err(|e| {
                cdk_common::payment::Error::Custom(format!("Failed to check receive status: {}", e))
            })?;

        if let Some(receive) = receive {
            // If finished, return the response
            if receive.finished_at.is_some() {
                let amount = receive
                    .invoice
                    .amount_milli_satoshis()
                    .map(|msat| self.btc_amount_to_cdk(bitcoin::Amount::from_sat(msat / 1000)))
                    .unwrap_or(Self::cdk_amount_zero());

                // Convert PaymentHash to bytes for hex encoding
                let payment_hash_bytes: [u8; 32] = payment_hash.into();
                return Ok(vec![WaitPaymentResponse {
                    payment_identifier: payment_identifier.clone(),
                    payment_amount: amount,
                    payment_id: hex::encode(payment_hash_bytes),
                }]);
            }
        }

        Ok(vec![])
    }

    async fn check_outgoing_payment(
        &self,
        payment_identifier: &PaymentIdentifier,
    ) -> Result<MakePaymentResponse, Self::Err> {
        debug!("Checking outgoing payment");

        // Extract payment hash from identifier
        let payment_hash = match payment_identifier {
            PaymentIdentifier::PaymentHash(hash) => PaymentHash::from(*hash),
            _ => {
                return Err(cdk_common::payment::Error::Custom(
                    "Unsupported payment identifier type".to_string(),
                ));
            }
        };

        // Check the payment status
        let preimage = self
            .wallet
            .check_lightning_payment(payment_hash, false)
            .await
            .map_err(|e| {
                cdk_common::payment::Error::Custom(format!("Failed to check payment: {}", e))
            })?;

        // Get the pending sends to find the amount
        let pending_sends = self.wallet.pending_lightning_sends().await.map_err(|e| {
            cdk_common::payment::Error::Custom(format!("Failed to get pending sends: {}", e))
        })?;

        let send_info = pending_sends.iter().find(|s| {
            if let Invoice::Bolt11(ref inv) = s.invoice {
                inv.payment_hash().to_byte_array() == payment_hash.as_ref()
            } else {
                false
            }
        });

        let (amount, status, payment_proof) = if let Some(lightning_send) = preimage {
            // Payment succeeded
            let amount = self.btc_amount_to_cdk(lightning_send.amount);
            let payment_proof = lightning_send
                .preimage
                .map(|preimage| hex::encode(preimage.as_ref()));
            (amount, MeltQuoteState::Paid, payment_proof)
        } else if send_info.is_some() {
            // Still pending
            let amount = send_info
                .map(|s| self.btc_amount_to_cdk(s.amount))
                .unwrap_or(Self::cdk_amount_zero());
            (amount, MeltQuoteState::Pending, None)
        } else {
            // Not found - might be completed already or failed
            (Self::cdk_amount_zero(), MeltQuoteState::Unpaid, None)
        };

        let amount_val = amount.value();
        let total_spent = Amount::new(
            amount_val + std::cmp::max(1, amount_val / 1000),
            CurrencyUnit::Sat,
        );

        Ok(MakePaymentResponse {
            payment_lookup_id: payment_identifier.clone(),
            payment_proof,
            status,
            total_spent,
        })
    }

    fn is_payment_event_stream_active(&self) -> bool {
        self.wait_invoice_active.load(Ordering::SeqCst)
    }

    fn cancel_payment_event_stream(&self) {
        self.wait_invoice_active.store(false, Ordering::SeqCst);
    }
}
