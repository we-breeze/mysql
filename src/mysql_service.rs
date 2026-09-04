//! SQLx-backed implementation of the public MySQL contract.

use std::{borrow::Cow, str::FromStr, sync::Arc};

use async_stream::stream;
use futures_core::Stream;
use futures_util::{StreamExt, pin_mut};
use sqlx::{
    MySql, MySqlPool,
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
};

use crate::{
    FromMysqlRow, Mysql, MysqlArgs, MysqlError, MysqlExecution, MysqlResult, MysqlRow,
    MysqlSelectorValue, MysqlServiceOptions, MysqlTableSelection, MysqlTableSharding,
    MysqlTransaction, arguments::encode_arguments,
};

/// SQLx-backed MySQL service.
#[derive(Clone, Debug)]
pub struct MysqlService {
    pool: MySqlPool,
    table_sharding: Option<Arc<MysqlTableSharding>>,
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
        let pool = pool_options
            .connect_with(connect_options)
            .await
            .map_err(map_sqlx_error)?;
        Ok(Self {
            pool,
            table_sharding: options.table_sharding.map(Arc::new),
        })
    }

    pub fn connect_lazy(url: &str) -> MysqlResult<Self> {
        Self::connect_lazy_with_options(url, MysqlServiceOptions::default())
    }

    pub fn connect_lazy_with_options(url: &str, options: MysqlServiceOptions) -> MysqlResult<Self> {
        let (connect_options, pool_options) = build_options(url, &options)?;
        Ok(Self {
            pool: pool_options.connect_lazy_with(connect_options),
            table_sharding: options.table_sharding.map(Arc::new),
        })
    }

    pub async fn execute<S, A>(&self, sql: S, arguments: A) -> MysqlResult<MysqlExecution>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
    {
        let (sql, arguments) =
            prepare_query(sql.as_ref(), arguments, self.table_sharding.as_deref())?;
        sqlx::query_with::<MySql, _>(sql.as_ref(), arguments)
            .execute(&self.pool)
            .await
            .map(execution)
            .map_err(map_sqlx_error)
    }

    pub async fn fetch_optional<S, A, T>(&self, sql: S, arguments: A) -> MysqlResult<Option<T>>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send,
    {
        let (sql, arguments) =
            prepare_query(sql.as_ref(), arguments, self.table_sharding.as_deref())?;
        sqlx::query_with::<MySql, _>(sql.as_ref(), arguments)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?
            .map(decode_row)
            .transpose()
    }

    pub async fn fetch_one<S, A, T>(&self, sql: S, arguments: A) -> MysqlResult<T>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send,
    {
        self.fetch_optional(sql, arguments)
            .await?
            .ok_or(MysqlError::RowNotFound)
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
            let (sql, arguments) = match prepare_query(
                sql.as_ref(),
                arguments,
                self.table_sharding.as_deref(),
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
                yield row.map_err(map_sqlx_error).and_then(decode_row);
            }
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
        let mut transaction = self.begin().await?;
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

    pub async fn close(&self) {
        self.pool.close().await;
    }

    async fn begin(&self) -> MysqlResult<MysqlTransactionService> {
        self.pool
            .begin()
            .await
            .map(|inner| MysqlTransactionService {
                inner: Some(inner),
                table_sharding: self.table_sharding.clone(),
            })
            .map_err(map_sqlx_error)
    }
}

impl Mysql for MysqlService {
    type Transaction = MysqlTransactionService;

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
    table_sharding: Option<Arc<MysqlTableSharding>>,
}

impl std::fmt::Debug for MysqlTransactionService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MysqlTransactionService")
            .finish_non_exhaustive()
    }
}

impl MysqlTransactionService {
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
        let (sql, arguments) =
            prepare_query(sql.as_ref(), arguments, self.table_sharding.as_deref())?;
        sqlx::query_with::<MySql, _>(sql.as_ref(), arguments)
            .execute(&mut **self.inner()?)
            .await
            .map(execution)
            .map_err(map_sqlx_error)
    }

    async fn fetch_optional<S, A, T>(&mut self, sql: S, arguments: A) -> MysqlResult<Option<T>>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send,
    {
        let (sql, arguments) =
            prepare_query(sql.as_ref(), arguments, self.table_sharding.as_deref())?;
        sqlx::query_with::<MySql, _>(sql.as_ref(), arguments)
            .fetch_optional(&mut **self.inner()?)
            .await
            .map_err(map_sqlx_error)?
            .map(decode_row)
            .transpose()
    }

    async fn fetch_one<S, A, T>(&mut self, sql: S, arguments: A) -> MysqlResult<T>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send,
    {
        self.fetch_optional(sql, arguments)
            .await?
            .ok_or(MysqlError::RowNotFound)
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
            let (sql, arguments) = match prepare_query(
                sql.as_ref(),
                arguments,
                self.table_sharding.as_deref(),
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
                yield row.map_err(map_sqlx_error).and_then(decode_row);
            }
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

fn prepare_query<'sql, A>(
    sql: &'sql str,
    arguments: A,
    sharding: Option<&MysqlTableSharding>,
) -> MysqlResult<(Cow<'sql, str>, sqlx::mysql::MySqlArguments)>
where
    A: MysqlArgs,
{
    let rendered = render_sql(sql, arguments.first_selector_value(), sharding)?;
    let encoded = encode_arguments(arguments)?;
    Ok((rendered, encoded))
}

fn render_sql<'sql>(
    sql: &'sql str,
    first: Option<MysqlSelectorValue<'_>>,
    sharding: Option<&MysqlTableSharding>,
) -> MysqlResult<Cow<'sql, str>> {
    let Some(sharding) = sharding else {
        return Ok(Cow::Borrowed(sql));
    };
    let first = first.ok_or_else(|| MysqlError::InvalidQuery {
        reason: "table-sharded MySQL requires the first SQL argument as its table selector"
            .to_string(),
    })?;
    let selection = sharding.selector.select(first)?;
    if let MysqlTableSelection::Shard(shard) = selection
        && shard >= sharding.shard_count
    {
        return Err(MysqlError::InvalidQuery {
            reason: format!(
                "table selector returned shard {shard}, but shard_count is {}",
                sharding.shard_count
            ),
        });
    }

    let mut output = String::with_capacity(sql.len() + 16);
    let mut cursor = 0;
    let mut found = false;
    loop {
        let next = sharding
            .tables
            .iter()
            .filter_map(|table| {
                sql[cursor..]
                    .find(&table.token)
                    .map(|offset| (cursor + offset, table))
            })
            .min_by_key(|(offset, _)| *offset);
        let Some((offset, table)) = next else {
            break;
        };
        found = true;
        output.push_str(&sql[cursor..offset]);
        let physical = match selection {
            MysqlTableSelection::Base => table.base.as_str(),
            MysqlTableSelection::Shard(shard) => &table.shards[shard as usize],
        };
        output.push_str(physical);
        cursor = offset + table.token.len();
    }
    if !found {
        return Err(MysqlError::InvalidQuery {
            reason: "table-sharded SQL must contain a configured logical-table token".to_string(),
        });
    }
    output.push_str(&sql[cursor..]);
    Ok(Cow::Owned(output))
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
            table_sharding: None,
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
    fn selector_uses_the_first_argument_and_precomputed_table_name() {
        let sharding = MysqlTableSharding::new(
            16,
            ["tasks", "subtasks"],
            |first: MysqlSelectorValue<'_>| {
                Ok(MysqlTableSelection::Shard((first.as_u64()? % 16) as u32))
            },
        )
        .unwrap();
        let sql = render_sql(
            "SELECT * FROM {{tasks}} JOIN {{subtasks}} USING (id) WHERE user_id = ?",
            Some(MysqlSelectorValue::U64(17)),
            Some(&sharding),
        )
        .unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM tasks_0001 JOIN subtasks_0001 USING (id) WHERE user_id = ?"
        );
    }

    #[test]
    fn sharded_query_requires_first_argument_and_logical_table() {
        let sharding = MysqlTableSharding::new(16, ["tasks"], |_: MysqlSelectorValue<'_>| {
            Ok(MysqlTableSelection::Base)
        })
        .unwrap();
        assert!(render_sql("SELECT * FROM {{tasks}}", None, Some(&sharding)).is_err());
        assert!(
            render_sql(
                "SELECT * FROM users WHERE id = ?",
                Some(MysqlSelectorValue::U64(1)),
                Some(&sharding),
            )
            .is_err()
        );
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
