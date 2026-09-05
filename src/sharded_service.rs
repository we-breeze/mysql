//! An owned routed service handle sharing the ordinary service's pool.

use std::sync::Arc;

use async_stream::stream;
use futures_core::Stream;
use futures_util::{StreamExt, pin_mut};

use crate::{
    FromMysqlRow, Mysql, MysqlArgs, MysqlExecution, MysqlResult, MysqlRoute, MysqlRouting,
    MysqlService, MysqlTransactionService, MysqlValue, PoolStats,
    routing::{invalid, render_sql},
};

/// Fixed-type handle that a repository can retain. Cloning or rebinding it
/// shares the underlying pool; dropping it does not close the parent's pool.
///
/// The application policy is evaluated once for each query (on first poll for
/// a stream) using an explicit key or the first SQL argument. Single-database
/// transactions resolve table routing separately for each templated statement.
#[derive(Clone)]
pub struct ShardedMysqlService {
    service: MysqlService,
    routing: QueryRouting,
}

#[derive(Clone)]
pub(crate) struct QueryRouting {
    policy: Arc<dyn MysqlRouting>,
    key: Option<Arc<dyn MysqlValue + Sync>>,
}

impl QueryRouting {
    pub(crate) fn with_key<K: MysqlValue + Sync + 'static>(&self, key: K) -> Self {
        Self {
            policy: self.policy.clone(),
            key: Some(Arc::new(key)),
        }
    }

    pub(crate) fn resolve<A: MysqlArgs>(&self, arguments: &A) -> MysqlResult<MysqlRoute> {
        let key = match &self.key {
            Some(key) => key.route_value(),
            None => arguments.first_route_value().ok_or_else(|| {
                invalid("sharded queries require .route(key) or a first SQL argument")
            })?,
        };
        self.policy.resolve(key)
    }
}

impl std::fmt::Debug for ShardedMysqlService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShardedMysqlService")
            .field("service", &self.service)
            .finish_non_exhaustive()
    }
}

impl ShardedMysqlService {
    pub(crate) fn new<R: MysqlRouting + 'static>(service: MysqlService, routing: R) -> Self {
        Self {
            service,
            routing: QueryRouting {
                policy: Arc::new(routing),
                key: None,
            },
        }
    }

    pub fn with_route<R: MysqlRouting + 'static>(&self, routing: R) -> Self {
        self.service.with_route(routing)
    }

    /// Bind an explicit key on an independent handle. The original handle
    /// remains unchanged, so it can serve concurrent users. The key is never
    /// encoded as a SQL argument. Pass owned keys when retaining the handle.
    pub fn route<K: MysqlValue + Sync + 'static>(&self, key: K) -> Self {
        Self {
            service: self.service.clone(),
            routing: self.routing.with_key(key),
        }
    }

    pub fn pool_stats(&self) -> PoolStats {
        self.service.pool_stats()
    }
}

impl Mysql for ShardedMysqlService {
    type Transaction = MysqlTransactionService;

    fn with_route<R: MysqlRouting + 'static>(&self, routing: R) -> ShardedMysqlService {
        ShardedMysqlService::with_route(self, routing)
    }

    async fn execute<S, A>(&self, sql: S, arguments: A) -> MysqlResult<MysqlExecution>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
    {
        let route = self.routing.resolve(&arguments)?;
        let sql = render_sql(sql.as_ref(), Some(&route), true)?;
        self.service.execute(sql, arguments).await
    }

    async fn fetch_optional<S, A, T>(&self, sql: S, arguments: A) -> MysqlResult<Option<T>>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send,
    {
        let route = self.routing.resolve(&arguments)?;
        let sql = render_sql(sql.as_ref(), Some(&route), true)?;
        self.service.fetch_optional(sql, arguments).await
    }

    async fn fetch_one<S, A, T>(&self, sql: S, arguments: A) -> MysqlResult<T>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send,
    {
        self.fetch_optional(sql, arguments)
            .await?
            .ok_or(crate::MysqlError::RowNotFound)
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
        stream! {
            let rendered = self.routing.resolve(&arguments)
                .and_then(|route| render_sql(sql.as_ref(), Some(&route), true));
            let sql = match rendered {
                Ok(sql) => sql,
                Err(error) => {
                    yield Err(error);
                    return;
                }
            };
            let rows = self.service.fetch(sql, arguments);
            pin_mut!(rows);
            while let Some(row) = rows.next().await {
                yield row;
            }
        }
    }

    async fn fetch_all<S, A, T>(&self, sql: S, arguments: A) -> MysqlResult<Vec<T>>
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

    async fn with_transaction<T, F>(&self, operation: F) -> MysqlResult<T>
    where
        T: Send,
        F: for<'transaction> AsyncFnOnce(&'transaction mut Self::Transaction) -> MysqlResult<T>
            + Send,
    {
        self.service
            .run_transaction(Some(self.routing.clone()), operation)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MysqlError, MysqlRouteValue};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn lazy() -> MysqlService {
        MysqlService::connect_lazy("mysql://user:secret@127.0.0.1:1/test").unwrap()
    }

    #[tokio::test]
    async fn invalid_routes_fail_before_connecting_and_streams_resolve_only_when_polled() {
        let mysql = lazy();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let tasks = mysql.with_route(move |key: MysqlRouteValue<'_>| {
            count.fetch_add(1, Ordering::SeqCst);
            MysqlRoute::new().with_table("tasks", format!("tasks_{:04}", key.as_u64()? % 16))
        });

        assert!(tasks.execute("DELETE FROM {{tasks}}", ()).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            tasks
                .execute("DELETE FROM {{tasks}} WHERE id = ?", ("not a uid",))
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            tasks
                .route(17_u64)
                .execute("DELETE FROM {{unknown}} WHERE id = ?", ("not a uid",))
                .await
                .is_err()
        );
        assert!(
            tasks
                .route(17_u64)
                .execute("DELETE FROM tasks", ())
                .await
                .is_err()
        );
        assert!(
            mysql
                .execute("DELETE FROM {{tasks}} WHERE id = ?", (17_u64,))
                .await
                .is_err()
        );
        assert_eq!(mysql.pool_stats().size, 0);
        assert_eq!(tasks.pool_stats().size, 0);

        #[derive(crate::FromMysqlRow)]
        struct Row {
            #[allow(dead_code)]
            id: u64,
        }
        let before = calls.load(Ordering::SeqCst);
        let rows = tasks.fetch::<_, _, Row>("SELECT id FROM {{tasks}} WHERE id = ?", ("bad key",));
        assert_eq!(calls.load(Ordering::SeqCst), before);
        pin_mut!(rows);
        assert!(matches!(
            rows.next().await,
            Some(Err(MysqlError::InvalidQuery { .. }))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), before + 1);
        assert!(rows.next().await.is_none());
        assert_eq!(mysql.pool_stats().size, 0);
        mysql.close().await;
    }
}
