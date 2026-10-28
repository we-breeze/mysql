//! Typed characteristics for one root transaction, never a pooled session default.

/// Isolation applied to one transaction before its BEGIN on the same connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MysqlIsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl MysqlIsolationLevel {
    pub(crate) fn statement(self) -> &'static str {
        match self {
            Self::ReadUncommitted => "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
            Self::ReadCommitted => "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
            Self::RepeatableRead => "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            Self::Serializable => "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        }
    }
}

/// Options for a root transaction. Defaults preserve the session's isolation.
/// Nested savepoints keep their enclosing transaction's characteristics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MysqlTransactionOptions {
    pub isolation_level: Option<MysqlIsolationLevel>,
}

impl MysqlTransactionOptions {
    #[must_use]
    pub fn with_isolation_level(mut self, level: MysqlIsolationLevel) -> Self {
        self.isolation_level = Some(level);
        self
    }
}
