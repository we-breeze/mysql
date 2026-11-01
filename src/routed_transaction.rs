//! Explicit per-statement routing on an existing transaction connection.

use futures_core::Stream;
use futures_util::{StreamExt, pin_mut};

use crate::{
    FromMysqlRow, MysqlArgs, MysqlExecution, MysqlNestedTransaction, MysqlResult, MysqlTransaction,
    MysqlTransactionService, query_routing::QueryRouting,
};

/// A borrowed query view; it never starts, commits, or moves the transaction.
pub struct RoutedMysqlTransaction<'transaction> {
    transaction: &'transaction mut MysqlTransactionService,
    routing: Option<QueryRouting>,
}

/// Explicit name for a routed view inside any transaction scope.
pub type RoutedMysqlTransactionScope<'transaction, 'connection> =
    RoutedMysqlTransaction<'transaction>;

impl<'transaction> RoutedMysqlTransaction<'transaction> {
    pub async fn with_transaction<T, F>(&mut self, operation: F) -> MysqlResult<T>
    where
        T: Send,
        F: for<'operation> AsyncFnOnce(&'operation mut MysqlTransactionService) -> MysqlResult<T>
            + Send,
    {
        self.transaction
            .with_nested_route(self.routing.clone(), operation)
            .await
    }

    pub(crate) fn new(
        transaction: &'transaction mut MysqlTransactionService,
        routing: Option<QueryRouting>,
    ) -> Self {
        Self {
            transaction,
            routing,
        }
    }
}

impl MysqlNestedTransaction for RoutedMysqlTransaction<'_> {
    type Transaction<'nested>
        = MysqlTransactionService
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
        self.transaction
            .with_nested_route(self.routing.clone(), operation)
            .await
    }
}

impl MysqlTransaction for RoutedMysqlTransaction<'_> {
    async fn execute<S, A>(&mut self, sql: S, arguments: A) -> MysqlResult<MysqlExecution>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
    {
        self.transaction
            .execute_with_route(sql, arguments, self.routing.as_ref())
            .await
    }

    async fn fetch_optional<S, A, T>(&mut self, sql: S, arguments: A) -> MysqlResult<Option<T>>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send,
    {
        self.transaction
            .fetch_optional_with_route(sql, arguments, self.routing.as_ref())
            .await
    }

    async fn fetch_one<S, A, T>(&mut self, sql: S, arguments: A) -> MysqlResult<T>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send,
    {
        self.transaction
            .fetch_one_with_route(sql, arguments, self.routing.as_ref())
            .await
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
        self.transaction
            .fetch_with_route(sql, arguments, self.routing.clone(), "fetch")
    }

    async fn fetch_all<S, A, T>(&mut self, sql: S, arguments: A) -> MysqlResult<Vec<T>>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send,
    {
        let rows =
            self.transaction
                .fetch_with_route(sql, arguments, self.routing.clone(), "fetch_all");
        pin_mut!(rows);
        let mut values = Vec::new();
        while let Some(row) = rows.next().await {
            values.push(row?);
        }
        Ok(values)
    }
}
