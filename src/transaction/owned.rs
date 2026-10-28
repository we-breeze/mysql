//! Own the checkout across SET/BEGIN, including cancellation before BEGIN confirms.
use crate::mysql_service::map_sqlx_error;
use crate::{MysqlIsolationLevel, MysqlResult};
use sqlx::{
    Executor, MySql, MySqlConnection, MySqlPool, TransactionManager,
    mysql::MySqlTransactionManager, pool::PoolConnection,
};

#[derive(Clone, Copy)]
enum Phase {
    Pending,
    Active,
    Complete,
}

pub(crate) struct ConfiguredTransaction {
    connection: Option<PoolConnection<MySql>>,
    phase: Phase,
}

impl ConfiguredTransaction {
    pub(crate) async fn begin(
        pool: &MySqlPool,
        isolation: MysqlIsolationLevel,
    ) -> MysqlResult<Self> {
        let mut connection = pool.acquire().await.map_err(map_sqlx_error)?;
        // SET affects the next transaction. Until BEGIN is confirmed, dropping
        // the future must discard the connection instead of leaking that setting.
        connection.close_on_drop();
        let mut transaction = Self {
            connection: Some(connection),
            phase: Phase::Pending,
        };
        transaction
            .connection_mut()
            .execute(isolation.statement())
            .await
            .map_err(map_sqlx_error)?;
        MySqlTransactionManager::begin(transaction.connection_mut(), None)
            .await
            .map_err(map_sqlx_error)?;
        transaction.phase = Phase::Active;
        Ok(transaction)
    }

    pub(crate) fn connection(&self) -> &MySqlConnection {
        self.connection
            .as_deref()
            .expect("transaction owns its connection")
    }

    pub(crate) fn connection_mut(&mut self) -> &mut MySqlConnection {
        self.connection
            .as_deref_mut()
            .expect("transaction owns its connection")
    }

    pub(crate) async fn commit(mut self) -> MysqlResult<()> {
        MySqlTransactionManager::commit(self.connection_mut())
            .await
            .map_err(map_sqlx_error)?;
        self.phase = Phase::Complete;
        Ok(())
    }

    pub(crate) async fn rollback(mut self) -> MysqlResult<()> {
        MySqlTransactionManager::rollback(self.connection_mut())
            .await
            .map_err(map_sqlx_error)?;
        self.phase = Phase::Complete;
        Ok(())
    }
}

impl Drop for ConfiguredTransaction {
    fn drop(&mut self) {
        let Some(mut connection) = self.connection.take() else {
            return;
        };
        if matches!(self.phase, Phase::Pending) {
            // close_on_drop remains armed for setup errors or an interrupted BEGIN.
            return;
        }
        if matches!(self.phase, Phase::Active) {
            // Same cancellation behavior as SQLx Transaction: queue rollback
            // before the pool tests and lends this connection again.
            MySqlTransactionManager::start_rollback(&mut connection);
        }
        // BEGIN consumed the one-transaction setting. Eagerly float the checkout
        // before scheduling its normal SQLx pool-return checks. If the runtime
        // has gone away, dropping this future closes its floating connection.
        let returned = connection.return_to_pool();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(returned);
        }
    }
}
