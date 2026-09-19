//! Application-facing MySQL contract.

use std::time::Duration;

use futures_core::Stream;
use thiserror::Error;

use crate::{FromMysqlRow, MysqlArgs, MysqlRouteKey, MysqlRouting, MysqlService};

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
    #[error("MySQL query timed out")]
    QueryTimedOut,
    #[error("MySQL row was not found")]
    RowNotFound,
    #[error("MySQL column {0:?} was not found")]
    ColumnNotFound(String),
    #[error("MySQL column index {index} is outside a row with {len} columns")]
    ColumnIndexOutOfBounds { index: usize, len: usize },
    #[error("MySQL result has {actual} columns; expected {expected}")]
    ColumnCount { expected: usize, actual: usize },
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

/// Connection-pool and session configuration.
#[derive(Clone, Debug)]
pub struct MysqlServiceOptions {
    pub max_connections: u32,
    pub min_connections: u32,
    pub acquire_timeout: Duration,
    pub query_timeout: Duration,
    pub idle_timeout: Option<Duration>,
    pub max_lifetime: Option<Duration>,
    pub slow_acquire_threshold: Duration,
    pub test_before_acquire: bool,
    pub charset: String,
    pub timezone: Option<String>,
}

impl Default for MysqlServiceOptions {
    fn default() -> Self {
        Self {
            max_connections: 32,
            min_connections: 0,
            acquire_timeout: Duration::from_secs(2),
            query_timeout: Duration::from_secs(3),
            idle_timeout: Some(Duration::from_secs(10 * 60)),
            max_lifetime: Some(Duration::from_secs(60 * 60)),
            slow_acquire_threshold: Duration::from_secs(2),
            test_before_acquire: true,
            charset: "utf8mb4".to_string(),
            timezone: None,
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
    pub fn with_query_timeout(mut self, value: Duration) -> Self {
        self.query_timeout = value;
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
}

/// Data-access contract consumed by repositories and application services.
#[allow(async_fn_in_trait)]
pub trait Mysql: Send + Sync {
    type Transaction: MysqlTransaction;

    /// Bind an application routing policy to an owned handle sharing this
    /// service's connection pool. SQL arguments remain independent of routing.
    fn with_route<R>(&self, routing: R) -> MysqlService
    where
        R: MysqlRouting + 'static;

    /// Bind an explicit key on an independent handle sharing this service's
    /// policy and pool, preserving the implementation type. Without a policy
    /// this is a no-op, as on `MysqlService`.
    fn route<K: MysqlRouteKey>(&self, key: K) -> Self;

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
