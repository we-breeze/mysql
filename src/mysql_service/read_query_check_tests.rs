use super::*;

#[test]
fn literal_check_is_optional_and_does_not_parse_sql() {
    for sql in [
        "SELECT * FROM tasks for update",
        "SELECT * FROM tasks FOR UPDATE",
        "SELECT 'for update'",
        "SELECT 1 /* FOR UPDATE */",
    ] {
        let result = prepare_fetch_query(sql, (), None);
        if cfg!(feature = "ai-strict") {
            assert!(
                matches!(result, Err(MysqlError::InvalidQuery { .. })),
                "{sql}"
            );
        } else {
            assert!(result.is_ok(), "{sql}");
        }
        assert!(prepare_query(sql, (), None).is_ok());
    }
    for sql in [
        "SELECT 1",
        "SELECT 1 For Update",
        "SELECT 1 FOR\nUPDATE",
        "UPDATE tasks SET id = 1",
    ] {
        assert!(prepare_fetch_query(sql, (), None).is_ok(), "{sql}");
    }
}

#[cfg(feature = "ai-strict")]
#[tokio::test]
async fn service_fetch_rejects_but_transactions_allow_locking_queries() {
    async fn check_transaction<T: MysqlTransaction>(tx: &mut T, sql: &str) {
        assert!(matches!(
            tx.fetch_optional::<_, _, i64>(sql, ()).await,
            Err(MysqlError::Database { .. })
        ));
        assert!(matches!(
            tx.fetch_one::<_, _, i64>(sql, ()).await,
            Err(MysqlError::Database { .. })
        ));
        assert!(matches!(
            tx.fetch_all::<_, _, i64>(sql, ()).await,
            Err(MysqlError::Database { .. })
        ));
        let rows = tx.fetch::<_, _, i64>(sql, ());
        pin_mut!(rows);
        assert!(matches!(
            rows.next().await,
            Some(Err(MysqlError::Database { .. }))
        ));
        assert!(rows.next().await.is_none());
    }

    let mysql = MysqlService::connect_lazy("mysql://user:secret@127.0.0.1:1/test").unwrap();
    let routed = mysql
        .with_route(|_: &str, _: &dyn MysqlRouteKey| Ok("tasks_01"))
        .route(1);
    for (service, sql) in [
        (&mysql, "SELECT id FROM tasks for update"),
        (&routed, "SELECT id FROM {{tasks}} FOR UPDATE"),
    ] {
        assert!(matches!(
            Mysql::fetch_optional::<_, _, i64>(service, sql, ()).await,
            Err(MysqlError::InvalidQuery { .. })
        ));
        assert!(matches!(
            Mysql::fetch_one::<_, _, i64>(service, sql, ()).await,
            Err(MysqlError::InvalidQuery { .. })
        ));
        assert!(matches!(
            Mysql::fetch_all::<_, _, i64>(service, sql, ()).await,
            Err(MysqlError::InvalidQuery { .. })
        ));
        let rows = Mysql::fetch::<_, _, i64>(service, sql, ());
        pin_mut!(rows);
        assert!(matches!(
            rows.next().await,
            Some(Err(MysqlError::InvalidQuery { .. }))
        ));
        assert!(rows.next().await.is_none());
    }
    let mut tx = MysqlTransactionService {
        inner: None,
        route: routed.routing.clone(),
        query_timeout: std::time::Duration::from_secs(1),
    };
    check_transaction(&mut tx, "SELECT id FROM {{tasks}} FOR UPDATE").await;
    check_transaction(&mut tx.route(2), "SELECT id FROM {{tasks}} for update").await;
    mysql.close().await;
}
