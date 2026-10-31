use super::*;

const STATEMENT_REASON: &str = "execute only accepts INSERT, UPDATE, DELETE, REPLACE, or SELECT";
const TRANSACTION_CONTROL_REASON: &str = "transaction control is managed by MysqlService; use tx.with_transaction(...) for nested transactions (SAVEPOINT), or return Ok/Err from the callback to commit/roll back the current scope";

fn assert_optional_rejection(result: MysqlResult<MysqlExecution>, reason: &str, sql: &str) {
    if cfg!(feature = "ai-strict") {
        assert_eq!(
            result.unwrap_err(),
            MysqlError::InvalidQuery {
                reason: reason.to_string(),
            },
            "{sql}",
        );
    } else {
        assert!(matches!(result, Err(MysqlError::Database { .. })), "{sql}");
    }
}

#[test]
fn execute_accepts_dml_and_select_prefixes_with_whitespace_and_mixed_case() {
    for sql in [
        "INSERT INTO tasks (id) VALUES (?)",
        "  update tasks SET id = ?",
        "\t\nDeLeTe FROM tasks WHERE id = ?",
        "\r\nreplace INTO tasks (id) VALUES (?)",
        " \nSeLeCt ? AS value",
        "SELECT(?)",
        "SELECT/* inline comment */?",
    ] {
        let (prepared, _) = prepare_execute_query(sql, (1_i64,), None).unwrap();
        assert_eq!(prepared, sql);
        #[cfg(feature = "ai-strict")]
        check_transaction_execute(sql).unwrap();
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
        "SELECTED 1",
        "SELECT_value",
        "SELECT$variable",
        "SELECTé",
        "INSERTED INTO tasks VALUES (1)",
    ] {
        let result = prepare_execute_query(sql, (), None);
        if cfg!(feature = "ai-strict") {
            assert_eq!(
                result.unwrap_err(),
                MysqlError::InvalidQuery {
                    reason: STATEMENT_REASON.to_string(),
                },
                "{sql}",
            );
        } else {
            assert!(result.is_ok(), "{sql}");
        }
    }
}

#[tokio::test]
async fn service_execute_applies_the_optional_prefix_check() {
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
                STATEMENT_REASON,
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
}

#[tokio::test]
async fn transaction_execute_applies_the_optional_whitelist_and_nested_transaction_guidance() {
    // An absent connection lets us distinguish query rejection from execution
    // without sending any SQL to a database.
    let mut transaction = MysqlTransactionService {
        inner: None,
        route: None,
        query_timeout: std::time::Duration::from_secs(1),
    };
    for sql in [
        "INSERT INTO tasks (id) VALUES (?)",
        "  UpDaTe tasks SET id = ?",
        "\tDELETE FROM tasks WHERE id = ?",
        "\nREPLACE INTO tasks (id) VALUES (?)",
        "SELECT ? AS value",
        "SELECT 'COMMIT; ROLLBACK; SAVEPOINT scope'",
        "SELECT(?)",
        "SELECT/* inline comment */?",
    ] {
        assert!(
            matches!(
                transaction.execute(sql, (1_i64,)).await,
                Err(MysqlError::Database { .. }),
            ),
            "{sql}"
        );
        assert!(
            matches!(
                transaction.route(2).execute(sql, (1_i64,)).await,
                Err(MysqlError::Database { .. }),
            ),
            "{sql}"
        );
    }
    for sql in [
        "COMMIT",
        " \nCoMmIt WORK AND CHAIN;",
        "ROLLBACK",
        "ROLLBACK WORK",
        "ROLLBACK TO scope",
        "\tRoLlBaCk TO SAVEPOINT scope",
        "BEGIN",
        "BEGIN WORK",
        " START TRANSACTION",
        "START\nTRANSACTION READ ONLY",
        "SAVEPOINT scope",
        " \nSaVePoInT scope;",
        "RELEASE SAVEPOINT scope",
        "release\tsavepoint scope",
    ] {
        assert_optional_rejection(
            transaction.execute(sql, ()).await,
            TRANSACTION_CONTROL_REASON,
            sql,
        );
        assert_optional_rejection(
            transaction.route(2).execute(sql, ()).await,
            TRANSACTION_CONTROL_REASON,
            sql,
        );
    }
    for sql in [
        "",
        " \t\n",
        "SET @task_id = 1",
        "CREATE TABLE tasks (id BIGINT)",
        "DROP TABLE tasks",
        "/* write */ INSERT INTO tasks (id) VALUES (1)",
        "WITH task_ids AS (SELECT 1) DELETE FROM tasks WHERE id IN (SELECT * FROM task_ids)",
        "SELECTED 1",
        "SELECT_value",
        "SELECT$variable",
        "SELECTé",
        "COMMITTED",
        "更新 tasks",
    ] {
        assert_optional_rejection(transaction.execute(sql, ()).await, STATEMENT_REASON, sql);
        assert_optional_rejection(
            transaction.route(2).execute(sql, ()).await,
            STATEMENT_REASON,
            sql,
        );
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
