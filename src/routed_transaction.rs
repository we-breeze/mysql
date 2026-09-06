//! Explicit per-statement routing on an existing transaction connection.

use async_stream::stream;
use futures_core::Stream;
use futures_util::{StreamExt, pin_mut};

use crate::{
    FromMysqlRow, MysqlArgs, MysqlExecution, MysqlResult, MysqlTransaction,
    MysqlTransactionService, mysql_service::render_query, sharded_service::QueryRouting,
};

/// A borrowed query view; it never starts, commits, or moves the transaction.
pub struct RoutedMysqlTransaction<'transaction> {
    transaction: &'transaction mut MysqlTransactionService,
    routing: Option<QueryRouting>,
}

impl<'transaction> RoutedMysqlTransaction<'transaction> {
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

impl MysqlTransaction for RoutedMysqlTransaction<'_> {
    async fn execute<S, A>(&mut self, sql: S, arguments: A) -> MysqlResult<MysqlExecution>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
    {
        let sql = render_query(sql.as_ref(), &arguments, self.routing.as_ref())?;
        self.transaction.execute(sql, arguments).await
    }

    async fn fetch_optional<S, A, T>(&mut self, sql: S, arguments: A) -> MysqlResult<Option<T>>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send,
    {
        let sql = render_query(sql.as_ref(), &arguments, self.routing.as_ref())?;
        self.transaction.fetch_optional(sql, arguments).await
    }

    async fn fetch_one<S, A, T>(&mut self, sql: S, arguments: A) -> MysqlResult<T>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send,
    {
        let sql = render_query(sql.as_ref(), &arguments, self.routing.as_ref())?;
        self.transaction.fetch_one(sql, arguments).await
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
            let sql = match render_query(sql.as_ref(), &arguments, self.routing.as_ref()) {
                Ok(sql) => sql,
                Err(error) => { yield Err(error); return; }
            };
            let rows = self.transaction.fetch(sql, arguments);
            pin_mut!(rows);
            while let Some(row) = rows.next().await { yield row; }
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
