//! Root transaction construction keeps setup and BEGIN on one checked-out connection.
use super::*;
use crate::MysqlTransactionOptions;
use crate::transaction::{inner::InnerTransaction, owned::ConfiguredTransaction};

impl MysqlService {
    /// Runs one transaction, committing on Ok and rolling back on Err.
    pub async fn with_transaction<T, F>(&self, operation: F) -> MysqlResult<T>
    where
        T: Send,
        F: for<'transaction> AsyncFnOnce(
                &'transaction mut MysqlTransactionService,
            ) -> MysqlResult<T>
            + Send,
    {
        self.with_transaction_options(MysqlTransactionOptions::default(), operation)
            .await
    }

    /// Apply typed characteristics before BEGIN. Successful transactions return
    /// the connection without changing its session defaults. Setup cancellation
    /// discards it; cancellation inside the callback queues ordinary rollback.
    /// Nested savepoints retain this transaction's isolation.
    pub async fn with_transaction_options<T, F>(
        &self,
        options: MysqlTransactionOptions,
        operation: F,
    ) -> MysqlResult<T>
    where
        T: Send,
        F: for<'transaction> AsyncFnOnce(
                &'transaction mut MysqlTransactionService,
            ) -> MysqlResult<T>
            + Send,
    {
        let observation = Observation::transaction(self.metrics.transaction);
        let result = run_with_timeout(self.query_timeout, async move {
            let inner = match options.isolation_level {
                Some(level) => InnerTransaction::Configured(
                    ConfiguredTransaction::begin(&self.master_pool, level).await?,
                ),
                None => InnerTransaction::Ordinary(
                    self.master_pool.begin().await.map_err(map_sqlx_error)?,
                ),
            };
            let mut transaction = MysqlTransactionService {
                inner: Some(inner),
                route: self.routing.clone(),
                query_timeout: self.query_timeout,
            };
            match operation(&mut transaction).await {
                Ok(value) => {
                    transaction.commit().await?;
                    Ok(value)
                }
                Err(operation) => match transaction.rollback().await {
                    Ok(()) => Err(operation),
                    Err(rollback) => Err(MysqlError::TransactionRollback {
                        operation: Box::new(operation),
                        rollback: Box::new(rollback),
                    }),
                },
            }
        })
        .await;
        observation.finish(result.is_ok());
        result
    }
}
