//! Lend the owned connection to a callback and restore it on every exit path.
use super::MysqlTransactionService;
use crate::MysqlResult;
use crate::mysql_service::map_sqlx_error;
use crate::query_routing::QueryRouting;
use sqlx::{MySqlConnection, TransactionManager, mysql::MySqlTransactionManager};

pub(super) struct Savepoint<'parent> {
    parent: &'parent mut MysqlTransactionService,
    transaction: MysqlTransactionService,
    parent_depth: usize,
}

impl<'parent> Savepoint<'parent> {
    pub(super) fn new(
        parent: &'parent mut MysqlTransactionService,
        route: Option<QueryRouting>,
    ) -> MysqlResult<Self> {
        let parent_depth = MySqlTransactionManager::get_transaction_depth(parent.inner()?);
        let inner = parent.inner.take();
        let query_timeout = parent.query_timeout;
        Ok(Self {
            parent,
            transaction: MysqlTransactionService {
                inner,
                route,
                query_timeout,
            },
            parent_depth,
        })
    }

    fn connection(&mut self) -> MysqlResult<&mut MySqlConnection> {
        Ok(self.transaction.inner()?)
    }

    pub(super) fn transaction(&mut self) -> &mut MysqlTransactionService {
        &mut self.transaction
    }

    pub(super) async fn begin(&mut self) -> MysqlResult<()> {
        MySqlTransactionManager::begin(self.connection()?, None)
            .await
            .map_err(map_sqlx_error)
    }

    pub(super) async fn commit(&mut self) -> MysqlResult<()> {
        MySqlTransactionManager::commit(self.connection()?)
            .await
            .map_err(map_sqlx_error)
    }

    pub(super) async fn rollback(&mut self) -> MysqlResult<()> {
        MySqlTransactionManager::rollback(self.connection()?)
            .await
            .map_err(map_sqlx_error)
    }
}

impl Drop for Savepoint<'_> {
    fn drop(&mut self) {
        if let Some(mut inner) = self.transaction.inner.take() {
            // SQLx queues rollback synchronously. The next connection operation
            // drains it before executing user SQL, including after cancellation.
            while MySqlTransactionManager::get_transaction_depth(&inner) > self.parent_depth {
                MySqlTransactionManager::start_rollback(&mut inner);
            }
            self.parent.inner = Some(inner);
        }
    }
}
