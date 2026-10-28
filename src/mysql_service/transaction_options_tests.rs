//! Actual snapshot visibility, rollback, cancellation and clean connection reuse.
use super::*;
use crate::{MysqlIsolationLevel, MysqlTransactionOptions};
use sqlx::Executor;
use std::time::Duration;

mod setup_cancellation;

fn read_committed() -> MysqlTransactionOptions {
    MysqlTransactionOptions::default().with_isolation_level(MysqlIsolationLevel::ReadCommitted)
}

async fn setup(table: &str) -> (MysqlService, MysqlService) {
    let url =
        std::env::var("BREEZE_MYSQL_TEST_URL").expect("integration-tests requires a MySQL URL");
    let options = MysqlServiceOptions::default()
        .with_max_connections(1)
        .with_query_timeout(Duration::from_secs(15));
    let mysql = MysqlService::connect_with_options(&url, options.clone())
        .await
        .unwrap();
    let writer = MysqlService::connect_with_options(&url, options)
        .await
        .unwrap();
    mysql
        .master_pool
        .execute(format!("DROP TABLE IF EXISTS {table}").as_str())
        .await
        .unwrap();
    mysql
        .master_pool
        .execute(
            format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, value BIGINT NOT NULL) ENGINE=InnoDB"
            )
            .as_str(),
        )
        .await
        .unwrap();
    mysql
        .execute(format!("INSERT INTO {table} VALUES (1,0)"), ())
        .await
        .unwrap();
    (mysql, writer)
}

#[tokio::test]
async fn read_committed_is_effective_before_begin_and_default_next_transaction_is_unchanged() {
    let (mysql, writer) = setup("brz_tx_isolation").await;
    let before: u64 = mysql.fetch_one("SELECT CONNECTION_ID()", ()).await.unwrap();
    let defaults: String = mysql
        .fetch_one("SELECT @@session.transaction_isolation", ())
        .await
        .unwrap();
    assert_eq!(defaults, "REPEATABLE-READ");
    Mysql::with_transaction_options(&mysql, read_committed(), async |tx| {
        let first: i64 = tx
            .fetch_one("SELECT value FROM brz_tx_isolation WHERE id=1", ())
            .await?;
        assert_eq!(first, 0);
        writer
            .execute("UPDATE brz_tx_isolation SET value=1 WHERE id=1", ())
            .await?;
        let next: i64 = tx
            .fetch_one("SELECT value FROM brz_tx_isolation WHERE id=1", ())
            .await?;
        assert_eq!(next, 1, "the current transaction must use READ COMMITTED");
        let same: u64 = tx.fetch_one("SELECT CONNECTION_ID()", ()).await?;
        assert_eq!(before, same);
        Ok(())
    })
    .await
    .unwrap();
    let after: u64 = mysql.fetch_one("SELECT CONNECTION_ID()", ()).await.unwrap();
    assert_eq!(
        before, after,
        "successful options scopes return a reusable connection"
    );
    mysql
        .with_transaction(async |tx| {
            let first: i64 = tx
                .fetch_one("SELECT value FROM brz_tx_isolation WHERE id=1", ())
                .await?;
            writer
                .execute("UPDATE brz_tx_isolation SET value=2 WHERE id=1", ())
                .await?;
            let next: i64 = tx
                .fetch_one("SELECT value FROM brz_tx_isolation WHERE id=1", ())
                .await?;
            assert_eq!(
                first, next,
                "the next ordinary transaction must retain its original snapshot"
            );
            Ok(())
        })
        .await
        .unwrap();
    let defaults_after: String = mysql
        .fetch_one("SELECT @@session.transaction_isolation", ())
        .await
        .unwrap();
    assert_eq!(defaults_after, defaults);
    mysql.close().await;
    writer.close().await;
}

#[tokio::test]
async fn configured_nested_savepoints_keep_outer_isolation_and_rollback_only_the_inner_write() {
    let (mysql, writer) = setup("brz_tx_nested_isolation").await;
    mysql
        .with_transaction_options(read_committed(), async |outer| {
            let id: u64 = outer.fetch_one("SELECT CONNECTION_ID()", ()).await?;
            let inner: MysqlResult<()> = outer
                .with_transaction(async |inner| {
                    let first: i64 = inner
                        .fetch_one("SELECT value FROM brz_tx_nested_isolation WHERE id=1", ())
                        .await?;
                    assert_eq!(first, 0);
                    writer
                        .execute("UPDATE brz_tx_nested_isolation SET value=1 WHERE id=1", ())
                        .await?;
                    let next: i64 = inner
                        .fetch_one("SELECT value FROM brz_tx_nested_isolation WHERE id=1", ())
                        .await?;
                    assert_eq!(next, 1);
                    inner
                        .execute("INSERT INTO brz_tx_nested_isolation VALUES (2,2)", ())
                        .await?;
                    Err(MysqlError::InvalidQuery {
                        reason: "inner rollback".into(),
                    })
                })
                .await;
            assert!(inner.is_err());
            let same: u64 = outer.fetch_one("SELECT CONNECTION_ID()", ()).await?;
            assert_eq!(same, id);
            let count: i64 = outer
                .fetch_one(
                    "SELECT COUNT(*) FROM brz_tx_nested_isolation WHERE id=2",
                    (),
                )
                .await?;
            assert_eq!(count, 0);
            outer
                .execute("INSERT INTO brz_tx_nested_isolation VALUES (3,3)", ())
                .await?;
            Ok(())
        })
        .await
        .unwrap();
    let count: i64 = mysql
        .fetch_one(
            "SELECT COUNT(*) FROM brz_tx_nested_isolation WHERE id=3",
            (),
        )
        .await
        .unwrap();
    assert_eq!(count, 1);
    mysql.close().await;
    writer.close().await;
}

#[tokio::test]
async fn configured_error_and_active_cancellation_rollback_without_leaking_or_discarding_clean_pool_slots()
 {
    let (mysql, writer) = setup("brz_tx_options_rollback").await;
    let before: u64 = mysql.fetch_one("SELECT CONNECTION_ID()", ()).await.unwrap();
    let result: MysqlResult<()> = mysql
        .with_transaction_options(read_committed(), async |tx| {
            tx.execute("UPDATE brz_tx_options_rollback SET value=10 WHERE id=1", ())
                .await?;
            Err(MysqlError::InvalidQuery {
                reason: "rollback scope".into(),
            })
        })
        .await;
    assert!(result.is_err());
    let value: i64 = mysql
        .fetch_one("SELECT value FROM brz_tx_options_rollback WHERE id=1", ())
        .await
        .unwrap();
    assert_eq!(value, 0);
    let (sent, received) = tokio::sync::oneshot::channel();
    let service = mysql.clone();
    let operation = tokio::spawn(async move {
        service
            .with_transaction_options(read_committed(), async |tx| {
                tx.execute("UPDATE brz_tx_options_rollback SET value=20 WHERE id=1", ())
                    .await?;
                let _ = sent.send(());
                std::future::pending::<MysqlResult<()>>().await
            })
            .await
    });
    received.await.unwrap();
    operation.abort();
    assert!(operation.await.unwrap_err().is_cancelled());
    let value: i64 = mysql
        .fetch_one("SELECT value FROM brz_tx_options_rollback WHERE id=1", ())
        .await
        .unwrap();
    assert_eq!(value, 0);
    let after: u64 = mysql.fetch_one("SELECT CONNECTION_ID()", ()).await.unwrap();
    assert_eq!(before, after);
    mysql.close().await;
    writer.close().await;
}
