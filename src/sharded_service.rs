//! An owned routed service handle sharing the ordinary service's pool.

use std::sync::Arc;

use futures_core::Stream;
use futures_util::{StreamExt, pin_mut};

use crate::{
    FromMysqlRow, Mysql, MysqlArgs, MysqlExecution, MysqlResult, MysqlRouteKey, MysqlRouting,
    MysqlService, MysqlTransactionService, PoolStats,
    arguments::ArgumentRouteKey,
    routing::{RouteRenderer, invalid},
};

/// Fixed-type handle that a repository can retain. Cloning or rebinding it
/// shares the underlying pool; dropping it does not close the parent's pool.
///
/// Plain SQL bypasses routing, even when an explicit key is bound. For SQL
/// containing table templates, the policy is evaluated once per distinct name
/// (on first poll for a stream), with the explicit key or first SQL argument.
/// Single-database transactions follow the same per-statement routing rules.
#[derive(Clone)]
pub struct ShardedMysqlService {
    service: MysqlService,
    routing: QueryRouting,
}

#[derive(Clone)]
pub(crate) struct QueryRouting {
    policy: Arc<dyn RouteRenderer>,
    key: Option<Arc<dyn MysqlRouteKey>>,
}

impl QueryRouting {
    pub(crate) fn with_key<K: MysqlRouteKey>(&self, key: K) -> Self {
        Self {
            policy: self.policy.clone(),
            key: Some(Arc::new(key)),
        }
    }

    pub(crate) fn render_template<A: MysqlArgs>(
        &self,
        template: &str,
        arguments: &A,
        implicit_key: &mut Option<ArgumentRouteKey>,
        out: &mut dyn std::fmt::Write,
    ) -> MysqlResult<()> {
        let key = match &self.key {
            Some(key) => key.as_ref(),
            None => {
                if implicit_key.is_none() {
                    let value = arguments.first_route_value().ok_or_else(|| {
                        invalid("sharded queries require .route(key) or a first SQL argument")
                    })?;
                    *implicit_key = Some(ArgumentRouteKey::new(value)?);
                }
                implicit_key.as_ref().unwrap().as_key()
            }
        };
        self.policy.render(template, key, out)
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
    pub fn route<K: MysqlRouteKey>(&self, key: K) -> Self {
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
        self.service
            .execute_with_routing(sql, arguments, Some(&self.routing))
            .await
    }

    async fn fetch_optional<S, A, T>(&self, sql: S, arguments: A) -> MysqlResult<Option<T>>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send,
    {
        self.service
            .fetch_optional_with_routing(sql, arguments, Some(&self.routing))
            .await
    }

    async fn fetch_one<S, A, T>(&self, sql: S, arguments: A) -> MysqlResult<T>
    where
        S: AsRef<str> + Send,
        A: MysqlArgs + Send,
        T: FromMysqlRow + Send,
    {
        self.service
            .fetch_one_with_routing(sql, arguments, Some(&self.routing))
            .await
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
        self.service
            .fetch_with_routing(sql, arguments, Some(&self.routing))
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
    use crate::MysqlError;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn lazy() -> MysqlService {
        MysqlService::connect_lazy("mysql://user:secret@127.0.0.1:1/test").unwrap()
    }

    #[tokio::test]
    async fn results_borrow_policy_template_and_key_and_write_only_once() {
        use crate::MysqlRouteOutput;
        struct Key(u64);
        struct Policy {
            prefix: String,
            writes: Arc<AtomicUsize>,
        }
        struct Output<'a> {
            prefix: &'a str,
            template: &'a str,
            id: &'a u64,
            writes: &'a AtomicUsize,
        }
        impl MysqlRouteOutput for Output<'_> {
            fn write_to(&self, out: &mut dyn std::fmt::Write) -> std::fmt::Result {
                self.writes.fetch_add(1, Ordering::SeqCst);
                write!(out, "{}_{}_{:04}", self.prefix, self.template, self.id)
            }
        }
        impl MysqlRouting for Policy {
            fn resolve<'a>(
                &'a self,
                template: &'a str,
                key: &'a dyn MysqlRouteKey,
            ) -> MysqlResult<impl MysqlRouteOutput + 'a> {
                let key = key
                    .downcast_ref::<Key>()
                    .ok_or_else(|| invalid("expected Key"))?;
                Ok(Output {
                    prefix: &self.prefix,
                    template,
                    id: &key.0,
                    writes: &self.writes,
                })
            }
        }
        let mysql = lazy();
        let writes = Arc::new(AtomicUsize::new(0));
        let routed = mysql
            .with_route(Policy {
                prefix: "app".into(),
                writes: writes.clone(),
            })
            .route(Key(509));
        let sql = "SELECT * FROM {{tasks}} JOIN {{subtasks}} JOIN {{tasks}}";
        assert_eq!(
            crate::mysql_service::render_query(sql, &(), Some(&routed.routing)).unwrap(),
            "SELECT * FROM `app_tasks_0509` JOIN `app_subtasks_0509` JOIN `app_tasks_0509`"
        );
        assert_eq!(writes.load(Ordering::SeqCst), 2);

        struct BorrowKey;
        impl MysqlRouting for BorrowKey {
            fn resolve<'a>(
                &'a self,
                _: &'a str,
                key: &'a dyn MysqlRouteKey,
            ) -> MysqlResult<impl MysqlRouteOutput + 'a> {
                key.as_str()
            }
        }
        let borrowed = mysql.with_route(BorrowKey).route(String::from("tasks"));
        assert_eq!(
            crate::mysql_service::render_query(
                "SELECT * FROM {{table}}",
                &(),
                Some(&borrowed.routing)
            )
            .unwrap(),
            "SELECT * FROM `tasks`"
        );
        let number = mysql
            .with_route(|_: &str, _: &dyn MysqlRouteKey| Ok(509_u64))
            .route(());
        assert_eq!(
            crate::mysql_service::render_query(
                "SELECT * FROM tasks_{{slot}}",
                &(),
                Some(&number.routing)
            )
            .unwrap(),
            "SELECT * FROM `tasks_509`"
        );
        mysql.close().await;
    }

    #[tokio::test]
    async fn output_errors_and_ignored_writer_errors_fail_before_connecting() {
        use crate::MysqlRouteOutput;
        struct BrokenOutput {
            ignore_error: bool,
        }
        impl MysqlRouteOutput for BrokenOutput {
            fn write_to(&self, out: &mut dyn std::fmt::Write) -> std::fmt::Result {
                out.write_str("tasks")?;
                if self.ignore_error {
                    let _ = out.write_str("; DROP TABLE users");
                    Ok(())
                } else {
                    Err(std::fmt::Error)
                }
            }
        }
        let mysql = lazy();
        for ignore_error in [false, true] {
            let routed = mysql
                .with_route(move |_: &str, _: &dyn MysqlRouteKey| Ok(BrokenOutput { ignore_error }))
                .route(());
            assert!(matches!(
                routed.execute("DELETE FROM {{tasks}}", ()).await,
                Err(MysqlError::InvalidQuery { .. })
            ));
        }
        let empty = mysql
            .with_route(|_: &str, _: &dyn MysqlRouteKey| Ok(""))
            .route(());
        assert!(matches!(
            empty.execute("DELETE FROM {{tasks}}", ()).await,
            Err(MysqlError::InvalidQuery { .. })
        ));
        assert_eq!(mysql.pool_stats().size, 0);
        mysql.close().await;
    }

    #[tokio::test]
    async fn custom_key_types_keep_business_meaning_without_sql_encoding() {
        // Neither type implements MysqlValue or Clone.
        struct ByTaskId(u64);
        struct ByUserId(u64);
        let mysql = lazy();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let tasks = mysql.with_route(move |template: &str, key: &dyn MysqlRouteKey| {
            count.fetch_add(1, Ordering::SeqCst);
            if let Some(ByTaskId(id)) = key.downcast_ref::<ByTaskId>() {
                return Ok(if *id < (1 << 37) {
                    template.to_owned()
                } else {
                    format!("{template}_{:04}", (id >> 37) % 1024)
                });
            }
            let uid = key
                .downcast_ref::<ByUserId>()
                .ok_or_else(|| invalid("expected ByTaskId or ByUserId"))?
                .0;
            Ok(format!("{template}_{:04}", uid % 1024))
        });
        let sql = "SELECT * FROM {{tasks}} a JOIN {{subtasks}} b JOIN {{tasks}} c";
        for (handle, expected) in [
            (
                tasks.route(ByTaskId(509)),
                "SELECT * FROM `tasks` a JOIN `subtasks` b JOIN `tasks` c",
            ),
            (
                tasks.route(ByUserId(509)),
                "SELECT * FROM `tasks_0509` a JOIN `subtasks_0509` b JOIN `tasks_0509` c",
            ),
            (
                tasks.route(ByTaskId((509 << 37) | 123)),
                "SELECT * FROM `tasks_0509` a JOIN `subtasks_0509` b JOIN `tasks_0509` c",
            ),
        ] {
            let before = calls.load(Ordering::SeqCst);
            // Cloning a handle shares the opaque key. SQL arguments are independent.
            let cloned = handle.clone();
            drop(handle);
            let rendered =
                crate::mysql_service::render_query(sql, &("not the key",), Some(&cloned.routing))
                    .unwrap();
            assert_eq!(rendered, expected);
            assert_eq!(calls.load(Ordering::SeqCst), before + 2);
        }
        let invalid_key = tasks.route(509_u64);
        assert!(crate::mysql_service::render_query(sql, &(), Some(&invalid_key.routing)).is_err());
        assert!(crate::mysql_service::render_query(sql, &(), Some(&tasks.routing)).is_err());
        // Rebinding clears the previous explicit key and uses the new policy.
        let rebound =
            tasks
                .route(ByUserId(509))
                .with_route(|name: &str, key: &dyn MysqlRouteKey| {
                    Ok(format!("{name}_{:04}", key.as_u64()?))
                });
        assert_eq!(
            crate::mysql_service::render_query(
                "SELECT * FROM {{tasks}}",
                &(2_u8,),
                Some(&rebound.routing)
            )
            .unwrap(),
            "SELECT * FROM `tasks_0002`"
        );
        assert_eq!(mysql.pool_stats().size, 0);
        mysql.close().await;
    }

    #[tokio::test]
    async fn implicit_borrowed_keys_are_adapted_once_and_plain_sql_does_not_read_them() {
        use crate::{MysqlRouteValue, MysqlValueWriter};
        struct Args<'a> {
            value: &'a str,
            reads: &'a AtomicUsize,
        }
        impl MysqlArgs for Args<'_> {
            fn len(&self) -> usize {
                1
            }
            fn encoded_size_hint(&self) -> usize {
                self.value.len() + 9
            }
            fn first_route_value(&self) -> Option<MysqlRouteValue<'_>> {
                self.reads.fetch_add(1, Ordering::SeqCst);
                Some(MysqlRouteValue::String(self.value))
            }
            fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
                writer.push(self.value)
            }
        }
        let mysql = lazy();
        let tasks = mysql.with_route(|template: &str, key: &dyn MysqlRouteKey| {
            Ok(format!("{template}_{}", key.as_str()?))
        });
        let text = String::from("tenant");
        let reads = AtomicUsize::new(0);
        let args = Args {
            value: text.as_str(),
            reads: &reads,
        };
        let sql = "SELECT * FROM {{tasks}} JOIN {{subtasks}} JOIN {{tasks}}";
        assert_eq!(
            crate::mysql_service::render_query(sql, &args, Some(&tasks.routing)).unwrap(),
            "SELECT * FROM `tasks_tenant` JOIN `subtasks_tenant` JOIN `tasks_tenant`"
        );
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        let plain = "SELECT '{{tasks}}' /* {{subtasks}} */";
        assert_eq!(
            crate::mysql_service::render_query(plain, &args, Some(&tasks.routing)).unwrap(),
            plain
        );
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        let explicit = tasks.route(String::from("other"));
        assert!(crate::mysql_service::render_query(sql, &args, Some(&explicit.routing)).is_ok());
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        mysql.close().await;
    }

    #[tokio::test]
    async fn plain_queries_bypass_routing_for_every_query_method() {
        let mysql = lazy();
        let tasks = mysql.with_route(|_: &str, _: &dyn MysqlRouteKey| -> MysqlResult<&str> {
            panic!("plain SQL must not invoke the routing policy")
        });
        // A closed pool lets every query reach the driver without a database.
        mysql.close().await;
        for handle in [tasks.clone(), tasks.route("unused key")] {
            for sql in [
                "SELECT ? AS value",
                "SELECT ? AS value, '{{tasks}}' AS literal /* {{ignored}} */",
            ] {
                let rendered = crate::mysql_service::render_query(
                    sql,
                    &("not a routing key",),
                    Some(&handle.routing),
                )
                .unwrap();
                assert!(matches!(rendered, std::borrow::Cow::Borrowed(_)));
                assert_eq!(rendered, sql);

                let results = [
                    handle.execute(sql, ("value",)).await.map(|_| ()),
                    handle
                        .fetch_one::<_, _, String>(sql, ("value",))
                        .await
                        .map(|_| ()),
                    handle
                        .fetch_optional::<_, _, String>(sql, ("value",))
                        .await
                        .map(|_| ()),
                    handle
                        .fetch_all::<_, _, String>(sql, ("value",))
                        .await
                        .map(|_| ()),
                ];
                for result in results {
                    assert!(matches!(result, Err(MysqlError::Database { .. })));
                }
                let rows = handle.fetch::<_, _, String>(sql, ("value",));
                pin_mut!(rows);
                assert!(matches!(
                    rows.next().await,
                    Some(Err(MysqlError::Database { .. }))
                ));
                assert!(rows.next().await.is_none());
            }
            // No argument is required when there is no template.
            assert!(matches!(
                handle.execute("DELETE FROM config", ()).await,
                Err(MysqlError::Database { .. })
            ));
        }
    }

    #[tokio::test]
    async fn invalid_routes_fail_before_connecting_and_streams_resolve_only_when_polled() {
        let mysql = lazy();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let tasks = mysql.with_route(move |template: &str, key: &dyn MysqlRouteKey| {
            count.fetch_add(1, Ordering::SeqCst);
            if template != "tasks" {
                return Err(invalid("unknown template"));
            }
            Ok(format!("tasks_{:04}", key.as_u64()? % 16))
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
