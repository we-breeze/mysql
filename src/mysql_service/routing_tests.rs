use super::*;
use crate::{MysqlError, routing::invalid};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn lazy() -> MysqlService {
    MysqlService::connect_lazy("mysql://user:secret@127.0.0.1:1/test").unwrap()
}

fn bind_key<M: Mysql, K: MysqlRouteKey>(mysql: &M, key: K) -> M {
    mysql.route(key)
}

#[tokio::test]
async fn one_service_type_keeps_policy_and_key_bindings_independent() {
    let plain = lazy();
    let policy = |name: &str, key: &dyn MysqlRouteKey| Ok(format!("{name}_{:04}", key.as_u64()?));
    // Inherent and trait entry points both return the same concrete type.
    let routed: MysqlService = Mysql::with_route(&plain, policy);
    let keyed: MysqlService = bind_key(&routed, 17_u64);
    let rebound: MysqlService = keyed.with_route(|name: &str, key: &dyn MysqlRouteKey| {
        Ok(format!("{name}_{:04}", key.as_u64()? + 100))
    });
    let sql = "SELECT * FROM {{tasks}}";
    assert!(render_query(sql, &(2_u64,), plain.routing.as_ref()).is_err());
    assert_eq!(
        render_query(sql, &(2_u64,), routed.routing.as_ref()).unwrap(),
        "SELECT * FROM `tasks_0002`"
    );
    assert_eq!(
        render_query(sql, &(2_u64,), keyed.routing.as_ref()).unwrap(),
        "SELECT * FROM `tasks_0017`"
    );
    assert_eq!(
        render_query(sql, &(2_u64,), rebound.routing.as_ref()).unwrap(),
        "SELECT * FROM `tasks_0102`"
    );
    drop(keyed.clone());
    assert!(!plain.pool.is_closed());
    keyed.close().await;
    for service in [&plain, &routed, &keyed, &rebound] {
        assert!(service.pool.is_closed());
    }
}

#[tokio::test]
async fn key_without_policy_does_not_enable_routing_or_leak_through_debug() {
    struct PrivateKey(&'static str);
    let mysql = lazy();
    let plain = bind_key(&mysql, PrivateKey("private-route-key"));
    assert!(plain.routing.is_none());
    assert!(matches!(
        render_query("SELECT ?", &(1,), plain.routing.as_ref()).unwrap(),
        Cow::Borrowed(_)
    ));
    assert_eq!(
        plain
            .execute("DELETE FROM {{tasks}}", ())
            .await
            .unwrap_err(),
        invalid("table templates require with_route"),
    );
    let routed = mysql
        .with_route(|_: &str, key: &dyn MysqlRouteKey| {
            Ok(key.downcast_ref::<PrivateKey>().unwrap().0)
        })
        .route(PrivateKey("private-route-key"));
    assert!(!format!("{routed:?}").contains("private-route-key"));
    assert_eq!(mysql.pool_stats().size, 0);
    mysql.close().await;
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
        crate::mysql_service::render_query(sql, &(), routed.routing.as_ref()).unwrap(),
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
            borrowed.routing.as_ref()
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
            number.routing.as_ref()
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
            crate::mysql_service::render_query(sql, &("not the key",), cloned.routing.as_ref())
                .unwrap();
        assert_eq!(rendered, expected);
        assert_eq!(calls.load(Ordering::SeqCst), before + 2);
    }
    let invalid_key = tasks.route(509_u64);
    assert!(crate::mysql_service::render_query(sql, &(), invalid_key.routing.as_ref()).is_err());
    assert!(crate::mysql_service::render_query(sql, &(), tasks.routing.as_ref()).is_err());
    // Rebinding clears the previous explicit key and uses the new policy.
    let rebound = tasks
        .route(ByUserId(509))
        .with_route(|name: &str, key: &dyn MysqlRouteKey| {
            Ok(format!("{name}_{:04}", key.as_u64()?))
        });
    assert_eq!(
        crate::mysql_service::render_query(
            "SELECT * FROM {{tasks}}",
            &(2_u8,),
            rebound.routing.as_ref()
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
        crate::mysql_service::render_query(sql, &args, tasks.routing.as_ref()).unwrap(),
        "SELECT * FROM `tasks_tenant` JOIN `subtasks_tenant` JOIN `tasks_tenant`"
    );
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    let plain = "SELECT '{{tasks}}' /* {{subtasks}} */";
    assert_eq!(
        crate::mysql_service::render_query(plain, &args, tasks.routing.as_ref()).unwrap(),
        plain
    );
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    let explicit = tasks.route(String::from("other"));
    assert!(crate::mysql_service::render_query(sql, &args, explicit.routing.as_ref()).is_ok());
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
                handle.routing.as_ref(),
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
