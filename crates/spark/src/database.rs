//! Database module for storing quote-to-payment mappings
//!
//! Uses redb to store mappings between mint/melt quotes and Spark payment IDs

use anyhow::Result;
use redb::{Database, ReadableDatabase, TableDefinition};
use std::path::Path;
use std::sync::Arc;

/// Table for storing mint quote ID to Spark payment ID mappings
/// Key: 32-byte payment hash, Value: payment request string
const MINT_QUOTES_TABLE: TableDefinition<&[u8; 32], &str> = TableDefinition::new("mint_quotes");

/// Table for storing melt quote ID to Spark payment ID mappings
/// Key: 32-byte payment hash, Value: payment request string
const MELT_QUOTES_TABLE: TableDefinition<&[u8; 32], &str> = TableDefinition::new("melt_quotes");

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
        }
        write_txn.commit()?;

        tracing::info!("Database initialized with mint_quotes and melt_quotes tables");

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
}
