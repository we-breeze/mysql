//! Application-facing MySQL contract.

use std::{collections::BTreeSet, fmt, sync::Arc, time::Duration};

use futures_core::Stream;
use thiserror::Error;

use crate::{FromMysqlRow, MysqlArgs, MysqlSelectorValue};

/// Result metadata for a write statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MysqlExecution {
    pub rows_affected: u64,
    pub last_insert_id: u64,
}

/// Current state of the service-owned connection pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolStats {
    pub size: u32,
    pub idle: usize,
    pub max_connections: u32,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MysqlError {
    #[error("invalid MySQL service configuration: {reason}")]
    InvalidConfig { reason: String },
    #[error("invalid MySQL connection URL")]
    InvalidUrl,
    #[error("invalid MySQL query: {reason}")]
    InvalidQuery { reason: String },
    #[error("cannot encode MySQL argument {position}: {message}")]
    EncodeArgument { position: usize, message: String },
    #[error("MySQL connection acquisition timed out")]
    PoolTimedOut,
    #[error("MySQL row was not found")]
    RowNotFound,
    #[error("MySQL column {0:?} was not found")]
    ColumnNotFound(String),
    #[error("MySQL column {column:?} unexpectedly contained NULL")]
    UnexpectedNull { column: String },
    #[error("cannot decode MySQL column {column:?} as {expected}")]
    Decode {
        column: String,
        expected: &'static str,
    },
    #[error("MySQL request failed (code {code:?}, SQLSTATE {sql_state:?}): {message}")]
    Database {
        code: Option<u16>,
        sql_state: Option<String>,
        message: String,
    },
    #[error("MySQL transaction failed: {operation}; rollback also failed: {rollback}")]
    TransactionRollback {
        operation: Box<MysqlError>,
        rollback: Box<MysqlError>,
    },
}

pub type MysqlResult<T> = Result<T, MysqlError>;

impl MysqlError {
    pub fn database_code(&self) -> Option<u16> {
        match self {
            Self::Database { code, .. } => *code,
            Self::TransactionRollback { operation, .. } => operation.database_code(),
            _ => None,
        }
    }

    pub fn database_sql_state(&self) -> Option<&str> {
        match self {
            Self::Database { sql_state, .. } => sql_state.as_deref(),
            Self::TransactionRollback { operation, .. } => operation.database_sql_state(),
            _ => None,
        }
    }

    pub fn is_duplicate_key(&self) -> bool {
        matches!(self.database_code(), Some(1022 | 1062 | 1169 | 1586 | 1859))
    }
}

/// Physical table decision returned by a configured selector.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MysqlTableSelection {
    Base,
    Shard(u32),
}

/// Chooses a physical table from the first SQL argument.
pub trait MysqlTableSelector: Send + Sync {
    fn select(&self, first: MysqlSelectorValue<'_>) -> MysqlResult<MysqlTableSelection>;
}

impl<F> MysqlTableSelector for F
where
    F: for<'value> Fn(MysqlSelectorValue<'value>) -> MysqlResult<MysqlTableSelection> + Send + Sync,
{
    fn select(&self, first: MysqlSelectorValue<'_>) -> MysqlResult<MysqlTableSelection> {
        self(first)
    }
}

#[derive(Clone)]
pub struct MysqlTableSharding {
    pub(crate) shard_count: u32,
    pub(crate) tables: Vec<MysqlLogicalTable>,
    pub(crate) selector: Arc<dyn MysqlTableSelector>,
}

#[derive(Clone, Debug)]
pub(crate) struct MysqlLogicalTable {
    pub(crate) logical: String,
    pub(crate) token: String,
    pub(crate) base: String,
    pub(crate) shards: Vec<String>,
}

impl MysqlTableSharding {
    pub fn new<I, N, S>(shard_count: u32, tables: I, selector: S) -> MysqlResult<Self>
    where
        I: IntoIterator<Item = N>,
        N: Into<String>,
        S: MysqlTableSelector + 'static,
    {
        if shard_count == 0 {
            return Err(MysqlError::InvalidConfig {
                reason: "table shard count must be greater than zero".to_string(),
            });
        }

        let mut seen = BTreeSet::new();
        let mut logical_tables = Vec::new();
        for table in tables {
            let table = table.into();
            if !valid_identifier(&table) {
                return Err(MysqlError::InvalidConfig {
                    reason: format!("invalid logical table identifier {table:?}"),
                });
            }
            if !seen.insert(table.clone()) {
                return Err(MysqlError::InvalidConfig {
                    reason: format!("duplicate logical table {table:?}"),
                });
            }
            logical_tables.push(MysqlLogicalTable {
                token: format!("{{{{{table}}}}}"),
                base: table.clone(),
                shards: (0..shard_count)
                    .map(|shard| format!("{table}_{shard:04}"))
                    .collect(),
                logical: table,
            });
        }
        if logical_tables.is_empty() {
            return Err(MysqlError::InvalidConfig {
                reason: "at least one logical table is required".to_string(),
            });
        }

        Ok(Self {
            shard_count,
            tables: logical_tables,
            selector: Arc::new(selector),
        })
    }

    pub fn shard_count(&self) -> u32 {
        self.shard_count
    }

    pub fn tables(&self) -> impl ExactSizeIterator<Item = &str> {
        self.tables.iter().map(|table| table.logical.as_str())
    }
}

impl fmt::Debug for MysqlTableSharding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MysqlTableSharding")
            .field("shard_count", &self.shard_count)
            .field("tables", &self.tables)
            .finish_non_exhaustive()
    }
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        && !value.as_bytes()[0].is_ascii_digit()
}

/// Connection-pool, session, and optional table-selection configuration.
#[derive(Clone, Debug)]
pub struct MysqlServiceOptions {
    pub max_connections: u32,
    pub min_connections: u32,
    pub acquire_timeout: Duration,
    pub idle_timeout: Option<Duration>,
    pub max_lifetime: Option<Duration>,
    pub slow_acquire_threshold: Duration,
    pub test_before_acquire: bool,
    pub charset: String,
    pub timezone: Option<String>,
    pub table_sharding: Option<MysqlTableSharding>,
}

impl Default for MysqlServiceOptions {
    fn default() -> Self {
        Self {
            max_connections: 30,
            min_connections: 0,
            acquire_timeout: Duration::from_secs(30),
            idle_timeout: Some(Duration::from_secs(10 * 60)),
            max_lifetime: Some(Duration::from_secs(60 * 60)),
            slow_acquire_threshold: Duration::from_secs(2),
            test_before_acquire: true,
            charset: "utf8mb4".to_string(),
            timezone: None,
            table_sharding: None,
        }
    }
}

impl MysqlServiceOptions {
    #[must_use]
    pub fn with_max_connections(mut self, value: u32) -> Self {
        self.max_connections = value;
        self
    }

    #[must_use]
    pub fn with_min_connections(mut self, value: u32) -> Self {
        self.min_connections = value;
        self
    }

    #[must_use]
    pub fn with_acquire_timeout(mut self, value: Duration) -> Self {
        self.acquire_timeout = value;
        self
    }

    #[must_use]
    pub fn with_idle_timeout(mut self, value: Option<Duration>) -> Self {
        self.idle_timeout = value;
        self
    }

    #[must_use]
    pub fn with_max_lifetime(mut self, value: Option<Duration>) -> Self {
        self.max_lifetime = value;
        self
    }

    #[must_use]
    pub fn with_test_before_acquire(mut self, value: bool) -> Self {
        self.test_before_acquire = value;
        self
    }

    #[must_use]
    pub fn with_charset(mut self, value: impl Into<String>) -> Self {
        self.charset = value.into();
        self
    }

    #[must_use]
    pub fn with_timezone(mut self, value: impl Into<String>) -> Self {
        self.timezone = Some(value.into());
        self
    }

    #[must_use]
    pub fn with_table_sharding(mut self, value: MysqlTableSharding) -> Self {
        self.table_sharding = Some(value);
        self
    }
}

/// Data-access contract consumed by repositories and application services.
#[allow(async_fn_in_trait)]
pub trait Mysql: Send + Sync {
    type Transaction: MysqlTransaction;

    async fn execute<S, A>(&self, sql: S, arguments: A) -> MysqlResult<MysqlExecution>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send;

    async fn fetch_optional<S, A, T>(&self, sql: S, arguments: A) -> MysqlResult<Option<T>>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send;

    async fn fetch_one<S, A, T>(&self, sql: S, arguments: A) -> MysqlResult<T>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send;

    fn fetch<'service, S, A, T>(
        &'service self,
        sql: S,
        arguments: A,
    ) -> impl Stream<Item = MysqlResult<T>> + Send + 'service
    where
        S: AsRef<str> + Send + 'service,
        A: MysqlArgs + Send + 'service,
        T: FromMysqlRow + Send + 'service;

    async fn fetch_all<S, A, T>(&self, sql: S, arguments: A) -> MysqlResult<Vec<T>>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send;

    async fn with_transaction<T, F>(&self, operation: F) -> MysqlResult<T>
    where
        T: Send,
        F: for<'transaction> AsyncFnOnce(&'transaction mut Self::Transaction) -> MysqlResult<T>
            + Send;
}

/// Scoped query operations available inside Mysql::with_transaction.
#[allow(async_fn_in_trait)]
pub trait MysqlTransaction: Send {
    async fn execute<S, A>(&mut self, sql: S, arguments: A) -> MysqlResult<MysqlExecution>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send;

    async fn fetch_optional<S, A, T>(&mut self, sql: S, arguments: A) -> MysqlResult<Option<T>>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send;

    async fn fetch_one<S, A, T>(&mut self, sql: S, arguments: A) -> MysqlResult<T>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send;

    fn fetch<'transaction, S, A, T>(
        &'transaction mut self,
        sql: S,
        arguments: A,
    ) -> impl Stream<Item = MysqlResult<T>> + Send + 'transaction
    where
        S: AsRef<str> + Send + 'transaction,
        A: MysqlArgs + Send + 'transaction,
        T: FromMysqlRow + Send + 'transaction;

    async fn fetch_all<S, A, T>(&mut self, sql: S, arguments: A) -> MysqlResult<Vec<T>>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_config_precomputes_physical_names() {
        let sharding =
            MysqlTableSharding::new(2, ["tasks", "subtasks"], |first: MysqlSelectorValue<'_>| {
                Ok(MysqlTableSelection::Shard((first.as_u64()? % 2) as u32))
            })
            .unwrap();

        assert_eq!(sharding.tables().collect::<Vec<_>>(), ["tasks", "subtasks"]);
        assert_eq!(sharding.tables[0].shards, ["tasks_0000", "tasks_0001"]);
    }

    #[test]
    fn rejects_unsafe_logical_table_identifiers() {
        let error = MysqlTableSharding::new(
            2,
            ["tasks; DROP TABLE users"],
            |_: MysqlSelectorValue<'_>| Ok(MysqlTableSelection::Base),
        )
        .unwrap_err();
        assert!(matches!(error, MysqlError::InvalidConfig { .. }));
    }
}
