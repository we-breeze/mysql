//! SQLx-backed implementation of the public MySQL contract.

use std::{borrow::Cow, str::FromStr};

use async_stream::stream;
use futures_core::Stream;
use futures_util::{StreamExt, pin_mut};
use sqlx::{
    MySql, MySqlPool,
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
};

use crate::metrics::{MysqlMetrics, Observation};

use crate::{
    FromMysqlRow, Mysql, MysqlArgs, MysqlError, MysqlExecution, MysqlResult, MysqlRouteKey,
    MysqlRouting, MysqlRow, MysqlServiceOptions, MysqlTransaction, arguments::encode_arguments,
    query_routing::QueryRouting, routing::render_sql,
};

/// SQLx-backed MySQL service with optional per-handle table routing.
/// Clones and handles created by `with_route`/`route` share one connection pool.
/// Plain SQL bypasses routing; templated SQL uses the handle's policy and key.
#[derive(Clone, Debug)]
pub struct MysqlService {
    pool: MySqlPool,
    metrics: MysqlMetrics,
    routing: Option<QueryRouting>,
}

impl MysqlService {
    pub async fn connect(url: &str) -> MysqlResult<Self> {
        Self::connect_with_options(url, MysqlServiceOptions::default()).await
    }

    pub async fn connect_with_options(
        url: &str,
        options: MysqlServiceOptions,
    ) -> MysqlResult<Self> {
        let (connect_options, pool_options) = build_options(url, &options)?;
        let metrics = MysqlMetrics::new(connect_options.get_host());
        let pool = pool_options
            .connect_with(connect_options)
            .await
            .map_err(map_sqlx_error)?;
        Ok(Self {
            pool,
            metrics,
            routing: None,
        })
    }

    pub fn connect_lazy(url: &str) -> MysqlResult<Self> {
        Self::connect_lazy_with_options(url, MysqlServiceOptions::default())
    }

    pub fn connect_lazy_with_options(url: &str, options: MysqlServiceOptions) -> MysqlResult<Self> {
        let (connect_options, pool_options) = build_options(url, &options)?;
        Ok(Self {
            metrics: MysqlMetrics::new(connect_options.get_host()),
            pool: pool_options.connect_lazy_with(connect_options),
            routing: None,
        })
    }

    /// Bind a policy on an independent service sharing this connection pool.
    /// Rebinding replaces the policy and clears any explicit key.
    pub fn with_route<R: MysqlRouting + 'static>(&self, routing: R) -> Self {
        Self {
            pool: self.pool.clone(),
            metrics: self.metrics,
            routing: Some(QueryRouting::new(routing)),
        }
    }

    /// Bind a key on an independent service without changing the original.
    /// The key is never encoded as a SQL argument. Without a policy this is a
    /// no-op; templated SQL still requires `with_route`, and plain SQL is unchanged.
    pub fn route<K: MysqlRouteKey>(&self, key: K) -> Self {
        Self {
            pool: self.pool.clone(),
            metrics: self.metrics,
            routing: self.routing.as_ref().map(|routing| routing.with_key(key)),
        }
    }

    pub async fn execute<S, A>(&self, sql: S, arguments: A) -> MysqlResult<MysqlExecution>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
    {
        let observation = Observation::new(self.metrics.update);
        let result = async {
            let (sql, arguments) = prepare_query(sql.as_ref(), arguments, self.routing.as_ref())?;
            sqlx::query_with::<MySql, _>(sql.as_ref(), arguments)
                .execute(&self.pool)
                .await
                .map(execution)
                .map_err(map_sqlx_error)
        }
        .await;
        observation.finish(result.is_ok());
        result
    }

    pub async fn fetch_optional<S, A, T>(&self, sql: S, arguments: A) -> MysqlResult<Option<T>>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send,
    {
        let observation = Observation::new(self.metrics.get);
        let result = async {
            let (sql, arguments) = prepare_query(sql.as_ref(), arguments, self.routing.as_ref())?;
            sqlx::query_with::<MySql, _>(sql.as_ref(), arguments)
                .fetch_optional(&self.pool)
                .await
                .map_err(map_sqlx_error)?
                .map(decode_row)
                .transpose()
        }
        .await;
        observation.finish(result.is_ok());
        result
    }

    pub async fn fetch_one<S, A, T>(&self, sql: S, arguments: A) -> MysqlResult<T>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send,
    {
        let observation = Observation::new(self.metrics.get);
        let result = async {
            let (sql, arguments) = prepare_query(sql.as_ref(), arguments, self.routing.as_ref())?;
            sqlx::query_with::<MySql, _>(sql.as_ref(), arguments)
                .fetch_optional(&self.pool)
                .await
                .map_err(map_sqlx_error)?
                .map(decode_row)
                .transpose()?
                .ok_or(MysqlError::RowNotFound)
        }
        .await;
        observation.finish(result.is_ok());
        result
    }

    /// Lazily fetches and decodes rows with driver-level backpressure.
    pub fn fetch<'service, S, A, T>(
        &'service self,
        sql: S,
        arguments: A,
    ) -> impl Stream<Item = MysqlResult<T>> + Send + 'service
    where
        S: AsRef<str> + Send + 'service,
        A: MysqlArgs + Send + 'service,
        T: FromMysqlRow + Send + 'service,
    {
        stream! {
            let observation = Observation::new(self.metrics.list);
            let mut success = true;
            let (sql, arguments) = match prepare_query(
                sql.as_ref(),
                arguments,
                self.routing.as_ref(),
            ) {
                Ok(prepared) => prepared,
                Err(error) => {
                    yield Err(error);
                    return;
                }
            };
            let rows = sqlx::query_with::<MySql, _>(sql.as_ref(), arguments).fetch(&self.pool);
            pin_mut!(rows);
            while let Some(row) = rows.next().await {
                let result = row.map_err(map_sqlx_error).and_then(decode_row);
                success &= result.is_ok();
                yield result;
            }
            observation.finish(success);
        }
    }

    pub async fn fetch_all<S, A, T>(&self, sql: S, arguments: A) -> MysqlResult<Vec<T>>
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

    /// Runs one scoped transaction. The closure cannot commit or roll back.
    pub async fn with_transaction<T, F>(&self, operation: F) -> MysqlResult<T>
    where
        T: Send,
        F: for<'transaction> AsyncFnOnce(
                &'transaction mut MysqlTransactionService,
            ) -> MysqlResult<T>
            + Send,
    {
        let observation = Observation::new(self.metrics.transaction);
        let result = async {
            let mut transaction = self.begin(self.routing.clone()).await?;
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
        }
        .await;
        observation.finish(result.is_ok());
        result
    }

    pub async fn ping(&self) -> MysqlResult<()> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(map_sqlx_error)
    }

    pub fn pool_stats(&self) -> crate::PoolStats {
        crate::PoolStats {
            size: self.pool.size(),
            idle: self.pool.num_idle(),
            max_connections: self.pool.options().get_max_connections(),
        }
    }

    /// Close the pool shared by every clone and routed handle.
    pub async fn close(&self) {
        self.pool.close().await;
    }

    async fn begin(&self, route: Option<QueryRouting>) -> MysqlResult<MysqlTransactionService> {
        self.pool
            .begin()
            .await
            .map(|inner| MysqlTransactionService {
                inner: Some(inner),
                metrics: self.metrics,
                route,
            })
            .map_err(map_sqlx_error)
    }
}

impl Mysql for MysqlService {
    type Transaction = MysqlTransactionService;

    fn with_route<R: MysqlRouting + 'static>(&self, routing: R) -> MysqlService {
        MysqlService::with_route(self, routing)
    }

    async fn execute<S, A>(&self, sql: S, arguments: A) -> MysqlResult<MysqlExecution>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
    {
        MysqlService::execute(self, sql, arguments).await
    }

    async fn fetch_optional<S, A, T>(&self, sql: S, arguments: A) -> MysqlResult<Option<T>>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send,
    {
        MysqlService::fetch_optional(self, sql, arguments).await
    }

    async fn fetch_one<S, A, T>(&self, sql: S, arguments: A) -> MysqlResult<T>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send,
    {
        MysqlService::fetch_one(self, sql, arguments).await
    }

    fn fetch<'service, S, A, T>(
        &'service self,
        sql: S,
        arguments: A,
    ) -> impl Stream<Item = MysqlResult<T>> + Send + 'service
    where
        S: AsRef<str> + Send + 'service,
        A: MysqlArgs + Send + 'service,
        T: FromMysqlRow + Send + 'service,
    {
        MysqlService::fetch(self, sql, arguments)
    }

    async fn fetch_all<S, A, T>(&self, sql: S, arguments: A) -> MysqlResult<Vec<T>>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send,
    {
        MysqlService::fetch_all(self, sql, arguments).await
    }

    async fn with_transaction<T, F>(&self, operation: F) -> MysqlResult<T>
    where
        T: Send,
        F: for<'transaction> AsyncFnOnce(&'transaction mut Self::Transaction) -> MysqlResult<T>
            + Send,
    {
        MysqlService::with_transaction(self, operation).await
    }
}

/// Scoped transaction value created only by MysqlService::with_transaction.
pub struct MysqlTransactionService {
    inner: Option<sqlx::Transaction<'static, MySql>>,
    metrics: MysqlMetrics,
    route: Option<QueryRouting>,
}

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

    fn inner(&mut self) -> MysqlResult<&mut sqlx::Transaction<'static, MySql>> {
        self.inner.as_mut().ok_or_else(|| MysqlError::Database {
            code: None,
            sql_state: None,
            message: "transaction is already complete".to_string(),
        })
    }

    async fn commit(mut self) -> MysqlResult<()> {
        self.inner
            .take()
            .expect("transaction is present until it is consumed")
            .commit()
            .await
            .map_err(map_sqlx_error)
    }

    async fn rollback(mut self) -> MysqlResult<()> {
        self.inner
            .take()
            .expect("transaction is present until it is consumed")
            .rollback()
            .await
            .map_err(map_sqlx_error)
    }
}

impl MysqlTransaction for MysqlTransactionService {
    async fn execute<S, A>(&mut self, sql: S, arguments: A) -> MysqlResult<MysqlExecution>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
    {
        let observation = Observation::new(self.metrics.update);
        let result = async {
            let (sql, arguments) = prepare_query(sql.as_ref(), arguments, self.route.as_ref())?;
            sqlx::query_with::<MySql, _>(sql.as_ref(), arguments)
                .execute(&mut **self.inner()?)
                .await
                .map(execution)
                .map_err(map_sqlx_error)
        }
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
        let observation = Observation::new(self.metrics.get);
        let result = async {
            let (sql, arguments) = prepare_query(sql.as_ref(), arguments, self.route.as_ref())?;
            sqlx::query_with::<MySql, _>(sql.as_ref(), arguments)
                .fetch_optional(&mut **self.inner()?)
                .await
                .map_err(map_sqlx_error)?
                .map(decode_row)
                .transpose()
        }
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
        let observation = Observation::new(self.metrics.get);
        let result = async {
            let (sql, arguments) = prepare_query(sql.as_ref(), arguments, self.route.as_ref())?;
            sqlx::query_with::<MySql, _>(sql.as_ref(), arguments)
                .fetch_optional(&mut **self.inner()?)
                .await
                .map_err(map_sqlx_error)?
                .map(decode_row)
                .transpose()?
                .ok_or(MysqlError::RowNotFound)
        }
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
            let observation = Observation::new(self.metrics.list);
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
            while let Some(row) = rows.next().await {
                let result = row.map_err(map_sqlx_error).and_then(decode_row);
                success &= result.is_ok();
                yield result;
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

pub(crate) fn prepare_query<'sql, A>(
    sql: &'sql str,
    arguments: A,
    route: Option<&QueryRouting>,
) -> MysqlResult<(Cow<'sql, str>, sqlx::mysql::MySqlArguments)>
where
    A: MysqlArgs,
{
    let rendered = render_query(sql, &arguments, route)?;
    let encoded = encode_arguments(arguments)?;
    Ok((rendered, encoded))
}

/// Plain statements bypass table routing, including inside a sharded transaction.
pub(crate) fn render_query<'sql, A: MysqlArgs>(
    sql: &'sql str,
    arguments: &A,
    routing: Option<&QueryRouting>,
) -> MysqlResult<Cow<'sql, str>> {
    let mut implicit_key = None;
    render_sql(sql, |template, out| {
        let routing =
            routing.ok_or_else(|| crate::routing::invalid("table templates require with_route"))?;
        routing.render_template(template, arguments, &mut implicit_key, out)
    })
}

fn build_options(
    url: &str,
    options: &MysqlServiceOptions,
) -> MysqlResult<(MySqlConnectOptions, MySqlPoolOptions)> {
    validate_options(options)?;
    let connect_options = MySqlConnectOptions::from_str(url)
        .map_err(|_| MysqlError::InvalidUrl)?
        .charset(&options.charset)
        .timezone(options.timezone.clone());
    let pool_options = MySqlPoolOptions::new()
        .max_connections(options.max_connections)
        .min_connections(options.min_connections)
        .acquire_timeout(options.acquire_timeout)
        .idle_timeout(options.idle_timeout)
        .max_lifetime(options.max_lifetime)
        .acquire_slow_threshold(options.slow_acquire_threshold)
        .test_before_acquire(options.test_before_acquire);
    Ok((connect_options, pool_options))
}

fn validate_options(options: &MysqlServiceOptions) -> MysqlResult<()> {
    let invalid = |reason: &str| MysqlError::InvalidConfig {
        reason: reason.to_string(),
    };
    if options.max_connections == 0 {
        return Err(invalid("max_connections must be greater than zero"));
    }
    if options.min_connections > options.max_connections {
        return Err(invalid("min_connections must not exceed max_connections"));
    }
    if options.acquire_timeout.is_zero() {
        return Err(invalid("acquire_timeout must be greater than zero"));
    }
    if options.idle_timeout.is_some_and(|value| value.is_zero()) {
        return Err(invalid("idle_timeout must be greater than zero when set"));
    }
    if options.max_lifetime.is_some_and(|value| value.is_zero()) {
        return Err(invalid("max_lifetime must be greater than zero when set"));
    }
    if options.slow_acquire_threshold.is_zero() {
        return Err(invalid("slow_acquire_threshold must be greater than zero"));
    }
    if options.slow_acquire_threshold > options.acquire_timeout {
        return Err(invalid(
            "slow_acquire_threshold must not exceed acquire_timeout",
        ));
    }
    if options.charset.is_empty() {
        return Err(invalid("charset must not be empty"));
    }
    Ok(())
}

fn execution(result: sqlx::mysql::MySqlQueryResult) -> MysqlExecution {
    MysqlExecution {
        rows_affected: result.rows_affected(),
        last_insert_id: result.last_insert_id(),
    }
}

fn decode_row<T: FromMysqlRow>(row: sqlx::mysql::MySqlRow) -> MysqlResult<T> {
    T::from_mysql_row(MysqlRow::new(row))
}

fn map_sqlx_error(error: sqlx::Error) -> MysqlError {
    match error {
        sqlx::Error::PoolTimedOut => MysqlError::PoolTimedOut,
        sqlx::Error::RowNotFound => MysqlError::RowNotFound,
        sqlx::Error::Database(error) => {
            let mysql = error.try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>();
            MysqlError::Database {
                code: mysql.map(sqlx::mysql::MySqlDatabaseError::number),
                sql_state: mysql
                    .and_then(sqlx::mysql::MySqlDatabaseError::code)
                    .map(str::to_string),
                message: error.message().to_string(),
            }
        }
        other => MysqlError::Database {
            code: None,
            sql_state: None,
            message: other.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn options() -> MysqlServiceOptions {
        MysqlServiceOptions {
            max_connections: 4,
            min_connections: 0,
            acquire_timeout: Duration::from_secs(2),
            idle_timeout: Some(Duration::from_secs(60)),
            max_lifetime: Some(Duration::from_secs(300)),
            slow_acquire_threshold: Duration::from_millis(500),
            test_before_acquire: true,
            charset: "utf8mb4".to_string(),
            timezone: Some("+08:00".to_string()),
        }
    }

    #[tokio::test]
    async fn service_builds_one_lazy_pool() {
        let service = MysqlService::connect_lazy_with_options(
            "mysql://user:secret@127.0.0.1:3306/wegent",
            options(),
        )
        .unwrap();
        assert_eq!(service.pool_stats().size, 0);
        assert_eq!(service.pool_stats().max_connections, 4);
        service.close().await;
    }

    #[test]
    fn invalid_url_error_is_credential_safe() {
        let error = MysqlService::connect_lazy_with_options(
            "mysql://user:top-secret@[broken/db",
            options(),
        )
        .unwrap_err();
        assert!(!error.to_string().contains("top-secret"));
        assert!(!format!("{error:?}").contains("top-secret"));
    }
}

#[cfg(test)]
mod routing_tests;
