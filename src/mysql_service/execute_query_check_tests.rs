use super::*;

#[test]
fn execute_accepts_dml_and_select_prefixes_with_whitespace_and_mixed_case() {
    for sql in [
        "INSERT INTO tasks (id) VALUES (?)",
        "  update tasks SET id = ?",
        "\t\nDeLeTe FROM tasks WHERE id = ?",
        "\r\nreplace INTO tasks (id) VALUES (?)",
        " \nSeLeCt ? AS value",
    ] {
        let (prepared, _) = prepare_execute_query(sql, (1_i64,), None).unwrap();
        assert_eq!(prepared, sql);
    }
}

#[test]
fn execute_prefix_check_is_optional() {
    for sql in [
        "",
        " \t\n",
        "BEGIN",
        " START TRANSACTION",
        "ROLLBACK",
        "CREATE TABLE tasks (id BIGINT)",
        "/* write */ INSERT INTO tasks (id) VALUES (1)",
        "WITH task_ids AS (SELECT 1) DELETE FROM tasks WHERE id IN (SELECT * FROM task_ids)",
        "更新 tasks",
    ] {
        let result = prepare_execute_query(sql, (), None);
        if cfg!(feature = "ai-strict") {
            assert_eq!(
                result.unwrap_err(),
                MysqlError::InvalidQuery {
                    reason: "execute only accepts INSERT, UPDATE, DELETE, REPLACE, or SELECT; use with_transaction for other SQL".to_string(),
                },
                "{sql}",
            );
        } else {
            assert!(result.is_ok(), "{sql}");
        }
    }
}

#[tokio::test]
async fn only_service_execute_applies_the_optional_prefix_check() {
    let mysql = MysqlService::connect_lazy("mysql://user:secret@127.0.0.1:1/test").unwrap();
    let routed = mysql
        .with_route(|_: &str, _: &dyn MysqlRouteKey| Ok("tasks_01"))
        .route(1);
    mysql.close().await;

    for service in [&mysql, &routed] {
        let commit_reason = "MySQL uses autocommit by default; a separate COMMIT is not needed. Use with_transaction for explicit transactions";
        for (result, reason) in [
            (service.execute("COMMIT", ()).await, commit_reason),
            (
                Mysql::execute(service, " \nCoMmIt WORK;", ()).await,
                commit_reason,
            ),
            (
                Mysql::execute(service, "ROLLBACK", ()).await,
                "execute only accepts INSERT, UPDATE, DELETE, REPLACE, or SELECT; use with_transaction for other SQL",
            ),
        ] {
            if cfg!(feature = "ai-strict") {
                assert_eq!(
                    result.unwrap_err(),
                    MysqlError::InvalidQuery {
                        reason: reason.to_string(),
                    },
                );
            } else {
                assert!(matches!(result, Err(MysqlError::Database { .. })));
            }
        }
    }

    // An absent connection lets us distinguish query rejection from execution
    // without sending any SQL to a database.
    let mut transaction = MysqlTransactionService {
        inner: None,
        route: routed.routing.clone(),
        query_timeout: std::time::Duration::from_secs(1),
    };
    assert!(matches!(
        transaction.execute("SET @task_id = ?", (1_i64,)).await,
        Err(MysqlError::Database { .. }),
    ));
    assert!(matches!(
        transaction
            .route(2)
            .execute("SET @task_id = ?", (2_i64,))
            .await,
        Err(MysqlError::Database { .. }),
    ));
    for sql in ["COMMIT", "ROLLBACK"] {
        assert!(matches!(
            transaction.execute(sql, ()).await,
            Err(MysqlError::Database { .. }),
        ));
        assert!(matches!(
            transaction.route(2).execute(sql, ()).await,
            Err(MysqlError::Database { .. }),
        ));
    }
}

#[tokio::test]
async fn autocommit_check_covers_service_and_transaction_execute_only_when_enabled() {
    fn check(result: MysqlResult<MysqlExecution>) {
        if cfg!(feature = "ai-strict") {
            assert_eq!(
                result.unwrap_err(),
                MysqlError::InvalidQuery {
                    reason: "autocommit is managed by MysqlService and must not be set manually"
                        .to_string(),
                },
            );
        } else {
            assert!(matches!(result, Err(MysqlError::Database { .. })));
        }
    }

    let mysql = MysqlService::connect_lazy("mysql://user:secret@127.0.0.1:1/test").unwrap();
    let routed = mysql
        .with_route(|_: &str, _: &dyn MysqlRouteKey| Ok("tasks_01"))
        .route(1);
    mysql.close().await;
    let mut transaction = MysqlTransactionService {
        inner: None,
        route: routed.routing.clone(),
        query_timeout: std::time::Duration::from_secs(1),
    };

    for sql in [
        "SET autocommit = 0",
        " \tSeT AuToCoMmIt = 1",
        "SET SESSION autocommit = OFF",
        "SET @@SESSION.autocommit = ON",
        "SET GLOBAL autocommit = 0",
    ] {
        for service in [&mysql, &routed] {
            check(service.execute(sql, ()).await);
            check(Mysql::execute(service, sql, ()).await);
        }
        check(transaction.execute(sql, ()).await);
        check(transaction.route(2).execute(sql, ()).await);
    }
}
