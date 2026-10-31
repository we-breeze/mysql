//! Connection-bound transactions and nested SQLx SAVEPOINT scopes.
pub(crate) mod inner;
pub(crate) mod owned;
mod savepoint;
use crate::metrics::Observation;
#[cfg(feature = "ai-strict")]
use crate::mysql_service::check_transaction_execute;
use crate::mysql_service::{
    decode_row, execute_query, map_sqlx_error, prepare_query, run_with_timeout,
};
use crate::query_routing::QueryRouting;
use crate::{
    FromMysqlRow, MysqlArgs, MysqlError, MysqlExecution, MysqlNestedTransaction, MysqlResult,
    MysqlTransaction,
};
use async_stream::stream;
use futures_core::Stream;
use futures_util::{StreamExt, pin_mut};
use sqlx::MySql;

/// Scoped transaction value created only by MysqlService::with_transaction.
pub struct MysqlTransactionService {
    pub(crate) inner: Option<inner::InnerTransaction>,
    pub(crate) route: Option<QueryRouting>,
    pub(crate) query_timeout: std::time::Duration,
}

/// Explicit name for a scoped callback handle on the same owned connection.
pub type MysqlTransactionScope<'connection> = MysqlTransactionService;

impl std::fmt::Debug for MysqlTransactionService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MysqlTransactionService")
            .finish_non_exhaustive()
    }
}

impl MysqlTransactionService {
    /// Bind a key for queries on this same transaction connection. The
    /// transaction's default routing key is unchanged after this view is dropped.
    pub fn route<K: crate::MysqlRouteKey>(&mut self, key: K) -> crate::RoutedMysqlTransaction<'_> {
        let routing = self.route.as_ref().map(|routing| routing.with_key(key));
        crate::RoutedMysqlTransaction::new(self, routing)
    }

    pub async fn with_transaction<T, F>(&mut self, operation: F) -> MysqlResult<T>
    where
        T: Send,
        F: for<'operation> AsyncFnOnce(&'operation mut MysqlTransactionService) -> MysqlResult<T>
            + Send,
    {
        let route = self.route.clone();
        self.with_nested_route(route, operation).await
    }

    /// Run a SAVEPOINT on this same connection. Success releases only the
    /// savepoint; failure rolls back only its changes. SQLx also rolls back
    /// the savepoint if this future is cancelled before completion.
    pub(crate) async fn with_nested_route<T, F>(
        &mut self,
        route: Option<QueryRouting>,
        operation: F,
    ) -> MysqlResult<T>
    where
        T: Send,
        F: for<'operation> AsyncFnOnce(&'operation mut MysqlTransactionService) -> MysqlResult<T>
            + Send,
    {
        let timeout = self.query_timeout;
        run_with_timeout(timeout, async move {
            let mut scope = savepoint::Savepoint::new(self, route)?;
            scope.begin().await?;
            match operation(scope.transaction()).await {
                Ok(value) => {
                    scope.commit().await?;
                    Ok(value)
                }
                Err(operation) => match scope.rollback().await {
                    Ok(()) => Err(operation),
                    Err(rollback) => Err(MysqlError::TransactionRollback {
                        operation: Box::new(operation),
                        rollback: Box::new(rollback),
                    }),
                },
            }
        })
        .await
    }

    fn inner(&mut self) -> MysqlResult<&mut inner::InnerTransaction> {
        self.inner.as_mut().ok_or_else(|| MysqlError::Database {
            code: None,
            sql_state: None,
            message: "transaction is already complete".to_string(),
        })
    }

    pub(crate) async fn commit(mut self) -> MysqlResult<()> {
        self.inner
            .take()
            .expect("transaction is present until it is consumed")
            .commit()
            .await
    }

    pub(crate) async fn rollback(mut self) -> MysqlResult<()> {
        self.inner
            .take()
            .expect("transaction is present until it is consumed")
            .rollback()
            .await
    }
}

impl MysqlNestedTransaction for MysqlTransactionService {
    type Transaction<'nested>
        = MysqlTransactionScope<'nested>
    where
        Self: 'nested;

    async fn with_transaction<'nested, T, F>(&'nested mut self, operation: F) -> MysqlResult<T>
    where
        Self: 'nested,
        T: Send,
        F: for<'operation> AsyncFnOnce(
                &'operation mut Self::Transaction<'nested>,
            ) -> MysqlResult<T>
            + Send,
    {
        let route = self.route.clone();
        self.with_nested_route(route, operation).await
    }
}

impl MysqlTransaction for MysqlTransactionService {
    async fn execute<S, A>(&mut self, sql: S, arguments: A) -> MysqlResult<MysqlExecution>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
    {
        let observation = Observation::query(None, sql.as_ref());
        let result = run_with_timeout(self.query_timeout, async {
            #[cfg(feature = "ai-strict")]
            check_transaction_execute(sql.as_ref())?;
            let (sql, arguments) = prepare_query(sql.as_ref(), arguments, self.route.as_ref())?;
            execute_query(&mut **self.inner()?, sql.as_ref(), arguments).await
        })
        .await;
        observation.finish(result.is_ok());
        result
    }

    async fn fetch_optional<S, A, T>(&mut self, sql: S, arguments: A) -> MysqlResult<Option<T>>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send,
    {
        let observation = Observation::query(None, sql.as_ref());
        let result = run_with_timeout(self.query_timeout, async {
            let (sql, arguments) = prepare_query(sql.as_ref(), arguments, self.route.as_ref())?;
            sqlx::query_with::<MySql, _>(sql.as_ref(), arguments)
                .fetch_optional(&mut **self.inner()?)
                .await
                .map_err(map_sqlx_error)?
                .map(decode_row)
                .transpose()
        })
        .await;
        observation.finish(result.is_ok());
        result
    }

    async fn fetch_one<S, A, T>(&mut self, sql: S, arguments: A) -> MysqlResult<T>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send,
    {
        let observation = Observation::query(None, sql.as_ref());
        let result = run_with_timeout(self.query_timeout, async {
            let (sql, arguments) = prepare_query(sql.as_ref(), arguments, self.route.as_ref())?;
            sqlx::query_with::<MySql, _>(sql.as_ref(), arguments)
                .fetch_optional(&mut **self.inner()?)
                .await
                .map_err(map_sqlx_error)?
                .map(decode_row)
                .transpose()?
                .ok_or(MysqlError::RowNotFound)
        })
        .await;
        observation.finish(result.is_ok());
        result
    }

    fn fetch<'transaction, S, A, T>(
        &'transaction mut self,
        sql: S,
        arguments: A,
    ) -> impl Stream<Item = MysqlResult<T>> + Send + 'transaction
    where
        S: AsRef<str> + Send + 'transaction,
        A: MysqlArgs + Send + 'transaction,
        T: FromMysqlRow + Send + 'transaction,
    {
        stream! {
            let observation = Observation::query(None, sql.as_ref());
            let deadline = tokio::time::Instant::now() + self.query_timeout;
            let mut success = true;
            let (sql, arguments) = match prepare_query(
                sql.as_ref(),
                arguments,
                self.route.as_ref(),
            ) {
                Ok(prepared) => prepared,
                Err(error) => {
                    yield Err(error);
                    return;
                }
            };
            let transaction = match self.inner() {
                Ok(transaction) => transaction,
                Err(error) => {
                    yield Err(error);
                    return;
                }
            };
            let rows =
                sqlx::query_with::<MySql, _>(sql.as_ref(), arguments).fetch(&mut **transaction);
            pin_mut!(rows);
            loop {
                match tokio::time::timeout_at(deadline, rows.next()).await {
                    Ok(Some(row)) => {
                        let result = row.map_err(map_sqlx_error).and_then(decode_row);
                        success &= result.is_ok();
                        yield result;
                    }
                    Ok(None) => break,
                    Err(_) => {
                        observation.finish(false);
                        yield Err(MysqlError::QueryTimedOut);
                        return;
                    }
                }
            }
            observation.finish(success);
        }
    }

    async fn fetch_all<S, A, T>(&mut self, sql: S, arguments: A) -> MysqlResult<Vec<T>>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send,
    {
        let rows = self.fetch(sql, arguments);
        pin_mut!(rows);
        let mut values = Vec::new();
        while let Some(row) = rows.next().await {
            values.push(row?);
        }
        Ok(values)
    }
}
