//! Database module for storing quote-to-payment mappings
//!
//! Uses redb to store mappings between mint/melt quotes and Spark payment IDs

use anyhow::Result;
use redb::{Database, ReadableDatabase, TableDefinition};
use std::path::Path;
use std::sync::Arc;

/// Table for storing mint quote ID to payment request mappings
/// Key: 32-byte payment hash, Value: payment request string
const MINT_QUOTES_TABLE: TableDefinition<&[u8; 32], &str> = TableDefinition::new("mint_quotes");

/// Table for storing melt quote ID to payment request mappings
/// Key: 32-byte payment hash, Value: payment request string
const MELT_QUOTES_TABLE: TableDefinition<&[u8; 32], &str> = TableDefinition::new("melt_quotes");

/// Spark SSP receive request IDs keyed by BOLT11 payment hash.
const MINT_PAYMENT_IDS_TABLE: TableDefinition<&[u8; 32], &str> =
    TableDefinition::new("mint_payment_ids");

/// Spark SSP send request IDs keyed by BOLT11 payment hash.
const MELT_PAYMENT_IDS_TABLE: TableDefinition<&[u8; 32], &str> =
    TableDefinition::new("melt_payment_ids");

/// Spark transfer IDs used as idempotency keys for outgoing payments.
const MELT_TRANSFER_IDS_TABLE: TableDefinition<&[u8; 32], &str> =
    TableDefinition::new("melt_transfer_ids");

/// Database wrapper for quote-to-payment mappings
#[derive(Clone)]
pub struct QuoteDatabase {
    db: Arc<Database>,
}

impl QuoteDatabase {
    /// Create a new database instance or open an existing one
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = Database::create(path)?;

        // Create tables if they don't exist
        let write_txn = db.begin_write()?;
        {
            let _mint_table = write_txn.open_table(MINT_QUOTES_TABLE)?;
            let _melt_table = write_txn.open_table(MELT_QUOTES_TABLE)?;
            let _mint_payment_ids = write_txn.open_table(MINT_PAYMENT_IDS_TABLE)?;
            let _melt_payment_ids = write_txn.open_table(MELT_PAYMENT_IDS_TABLE)?;
            let _melt_transfer_ids = write_txn.open_table(MELT_TRANSFER_IDS_TABLE)?;
        }
        write_txn.commit()?;

        tracing::info!("Quote database initialized");

        Ok(Self { db: Arc::new(db) })
    }

    /// Store a mint quote ID to Spark payment ID mapping
    pub fn insert_mint_quote(&self, payment_hash: &[u8; 32], payment_request: &str) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(MINT_QUOTES_TABLE)?;
            table.insert(payment_hash, payment_request)?;
        }
        write_txn.commit()?;
        tracing::debug!(
            "Inserted mint quote mapping: {} -> {}",
            hex::encode(payment_hash),
            payment_request
        );
        Ok(())
    }

    /// Store a melt quote ID to Spark payment ID mapping
    pub fn insert_melt_quote(&self, payment_hash: &[u8; 32], payment_request: &str) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(MELT_QUOTES_TABLE)?;
            table.insert(payment_hash, payment_request)?;
        }
        write_txn.commit()?;
        tracing::debug!(
            "Inserted melt quote mapping: {} -> {}",
            hex::encode(payment_hash),
            payment_request
        );
        Ok(())
    }

    /// Get the Spark payment request for a mint quote
    pub fn get_mint_quote(&self, payment_hash: &[u8; 32]) -> Result<Option<String>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(MINT_QUOTES_TABLE)?;

        let result = table.get(payment_hash)?;
        Ok(result.map(|v| v.value().to_string()))
    }

    /// Get the Spark payment request for a melt quote
    pub fn get_melt_quote(&self, payment_hash: &[u8; 32]) -> Result<Option<String>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(MELT_QUOTES_TABLE)?;

        let result = table.get(payment_hash)?;
        Ok(result.map(|v| v.value().to_string()))
    }

    /// Store the SSP receive request ID for an incoming invoice.
    pub fn insert_mint_payment_id(&self, payment_hash: &[u8; 32], payment_id: &str) -> Result<()> {
        self.insert_mapping(MINT_PAYMENT_IDS_TABLE, payment_hash, payment_id)
    }

    /// Get the SSP receive request ID for an incoming invoice.
    pub fn get_mint_payment_id(&self, payment_hash: &[u8; 32]) -> Result<Option<String>> {
        self.get_mapping(MINT_PAYMENT_IDS_TABLE, payment_hash)
    }

    /// Store the SSP send request ID for an outgoing invoice.
    pub fn insert_melt_payment_id(&self, payment_hash: &[u8; 32], payment_id: &str) -> Result<()> {
        self.insert_mapping(MELT_PAYMENT_IDS_TABLE, payment_hash, payment_id)
    }

    /// Get the SSP send request ID for an outgoing invoice.
    pub fn get_melt_payment_id(&self, payment_hash: &[u8; 32]) -> Result<Option<String>> {
        self.get_mapping(MELT_PAYMENT_IDS_TABLE, payment_hash)
    }

    /// Store the transfer ID used to make an outgoing payment idempotent.
    pub fn insert_melt_transfer_id(
        &self,
        payment_hash: &[u8; 32],
        transfer_id: &str,
    ) -> Result<()> {
        self.insert_mapping(MELT_TRANSFER_IDS_TABLE, payment_hash, transfer_id)
    }

    /// Get the transfer ID for an outgoing payment.
    pub fn get_melt_transfer_id(&self, payment_hash: &[u8; 32]) -> Result<Option<String>> {
        self.get_mapping(MELT_TRANSFER_IDS_TABLE, payment_hash)
    }

    fn insert_mapping(
        &self,
        definition: TableDefinition<&[u8; 32], &str>,
        payment_hash: &[u8; 32],
        value: &str,
    ) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(definition)?;
            table.insert(payment_hash, value)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn get_mapping(
        &self,
        definition: TableDefinition<&[u8; 32], &str>,
        payment_hash: &[u8; 32],
    ) -> Result<Option<String>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(definition)?;
        let result = table.get(payment_hash)?;
        Ok(result.map(|value| value.value().to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::QuoteDatabase;

    fn test_db_path() -> std::path::PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "cdk-spark-quotes-{}-{unique}.redb",
            std::process::id()
        ))
    }

    #[test]
    fn persists_quote_and_spark_ids() {
        let path = test_db_path();
        let hash = [42_u8; 32];

        {
            let db = QuoteDatabase::new(&path).expect("create quote database");
            db.insert_mint_quote(&hash, "incoming-invoice")
                .expect("insert incoming invoice");
            db.insert_mint_payment_id(&hash, "receive-request")
                .expect("insert receive request");
            db.insert_melt_quote(&hash, "outgoing-invoice")
                .expect("insert outgoing invoice");
            db.insert_melt_payment_id(&hash, "send-request")
                .expect("insert send request");
            db.insert_melt_transfer_id(&hash, "transfer-id")
                .expect("insert transfer ID");
        }

        let db = QuoteDatabase::new(&path).expect("reopen quote database");
        assert_eq!(
            db.get_mint_quote(&hash).expect("get incoming invoice"),
            Some("incoming-invoice".to_string())
        );
        assert_eq!(
            db.get_mint_payment_id(&hash).expect("get receive request"),
            Some("receive-request".to_string())
        );
        assert_eq!(
            db.get_melt_quote(&hash).expect("get outgoing invoice"),
            Some("outgoing-invoice".to_string())
        );
        assert_eq!(
            db.get_melt_payment_id(&hash).expect("get send request"),
            Some("send-request".to_string())
        );
        assert_eq!(
            db.get_melt_transfer_id(&hash).expect("get transfer ID"),
            Some("transfer-id".to_string())
        );

        drop(db);
        std::fs::remove_file(path).expect("remove quote database");
    }
}
