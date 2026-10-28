//! Real MySQL regressions for parameterless execution and manual savepoints.

use super::*;
use sqlx::{Executor, Row};

async fn setup(table: &str) -> MysqlService {
    let url = std::env::var("BREEZE_MYSQL_TEST_URL")
        .expect("integration-tests requires BREEZE_MYSQL_TEST_URL");
    let mysql = MysqlService::connect_with_options(
        &url,
        MysqlServiceOptions::default().with_max_connections(1),
    )
    .await
    .unwrap();
    // Fixture DDL uses the raw driver so ai-strict's pooled DML contract stays intact.
    mysql.master_pool.execute(format!(
        "CREATE TEMPORARY TABLE {table} (id BIGINT AUTO_INCREMENT PRIMARY KEY, value VARCHAR(128)) ENGINE=InnoDB"
    ).as_str()).await.unwrap();
    mysql
}

async fn prepare_count(mysql: &MysqlService) -> u64 {
    mysql
        .master_pool
        .fetch_one("SHOW SESSION STATUS LIKE 'Com_stmt_prepare'")
        .await
        .unwrap()
        .get::<String, _>(1)
        .parse()
        .unwrap()
}

#[tokio::test]
async fn parameterless_execute_uses_text_protocol_for_unit_and_empty_arguments() {
    let mysql = setup("brz_execute_text").await;
    let before = prepare_count(&mysql).await;
    let first = mysql
        .execute(
            "INSERT INTO brz_execute_text (value) VALUES ('literal ?')",
            (),
        )
        .await
        .unwrap();
    let second = mysql
        .execute(
            "INSERT INTO brz_execute_text (value) VALUES ('empty arguments')",
            Vec::<i64>::new(),
        )
        .await
        .unwrap();
    assert_eq!(first.rows_affected, 1);
    assert_eq!(first.last_insert_id, 1);
    assert_eq!(second.last_insert_id, 2);
    assert_eq!(prepare_count(&mysql).await, before);
    let row = mysql
        .master_pool
        .fetch_one("SELECT value FROM brz_execute_text WHERE id=1")
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>(0), "literal ?");
    mysql.close().await;
}

#[tokio::test]
async fn parameterized_execute_keeps_prepared_binding() {
    let mysql = setup("brz_execute_bound").await;
    let before = prepare_count(&mysql).await;
    let value = "'); DROP TABLE brz_execute_bound; -- ?";
    let inserted = mysql
        .execute("INSERT INTO brz_execute_bound (value) VALUES (?)", (value,))
        .await
        .unwrap();
    assert_eq!(inserted.rows_affected, 1);
    assert_eq!(prepare_count(&mysql).await, before + 1);
    let row = mysql
        .master_pool
        .fetch_one("SELECT value FROM brz_execute_bound")
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>(0), value);
    mysql.close().await;
}

#[tokio::test]
async fn manual_savepoints_keep_one_connection_and_outer_commit_or_rollback() {
    let mysql = setup("brz_execute_savepoint").await;
    mysql
        .with_transaction(async |tx| {
            let connection: u64 = tx.fetch_one("SELECT CONNECTION_ID()", ()).await?;
            tx.execute("INSERT INTO brz_execute_savepoint (id) VALUES (1)", ())
                .await?;
            tx.execute("SAVEPOINT manual_scope", ()).await?;
            tx.execute("INSERT INTO brz_execute_savepoint (id) VALUES (2)", ())
                .await?;
            tx.execute("ROLLBACK TO SAVEPOINT manual_scope", ()).await?;
            tx.execute("RELEASE SAVEPOINT manual_scope", ()).await?;
            tx.execute(
                "INSERT INTO brz_execute_savepoint (id) VALUES (?)",
                (3_i64,),
            )
            .await?;
            let after: u64 = tx.fetch_one("SELECT CONNECTION_ID()", ()).await?;
            assert_eq!(connection, after);
            Ok(())
        })
        .await
        .unwrap();
    let result: MysqlResult<()> = mysql
        .with_transaction(async |tx| {
            tx.execute("SAVEPOINT manual_scope", ()).await?;
            tx.execute("INSERT INTO brz_execute_savepoint (id) VALUES (4)", ())
                .await?;
            tx.execute("RELEASE SAVEPOINT manual_scope", ()).await?;
            Err(MysqlError::RowNotFound)
        })
        .await;
    assert_eq!(result, Err(MysqlError::RowNotFound));
    let rows = mysql
        .master_pool
        .fetch_all("SELECT id FROM brz_execute_savepoint ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.get::<i64, _>(0))
            .collect::<Vec<_>>(),
        [1, 3]
    );
    mysql.close().await;
}

#[tokio::test]
async fn parameterless_execute_preserves_explicit_pool_and_transaction_routes() {
    let mysql = setup("brz_execute_route_7").await;
    let routed = mysql
        .clone()
        .with_route(|table: &str, key: &dyn MysqlRouteKey| Ok(format!("{table}_{}", key.as_u64()?)))
        .route(7_u64);
    routed
        .execute("INSERT INTO {{brz_execute_route}} (id) VALUES (1)", ())
        .await
        .unwrap();
    routed
        .with_transaction(async |tx| {
            let mut scoped = tx.route(7_u64);
            scoped.execute("SAVEPOINT routed_scope", ()).await?;
            scoped
                .execute("INSERT INTO {{brz_execute_route}} (id) VALUES (2)", ())
                .await?;
            scoped.execute("RELEASE SAVEPOINT routed_scope", ()).await?;
            Ok(())
        })
        .await
        .unwrap();
    let row = mysql
        .master_pool
        .fetch_one("SELECT COUNT(*) FROM brz_execute_route_7")
        .await
        .unwrap();
    assert_eq!(row.get::<i64, _>(0), 2);
    mysql.close().await;
}
