#![cfg(feature = "integration-tests")]

use brz_mysql::{
    FromMysqlRow, MysqlError, MysqlNestedTransaction, MysqlResult, MysqlService,
    MysqlServiceOptions, MysqlTransaction,
};
use std::time::Duration;

#[derive(Debug, FromMysqlRow)]
struct Number {
    value: u64,
}

async fn setup(table: &str) -> Option<MysqlService> {
    let url = std::env::var("BREEZE_MYSQL_TEST_URL").ok()?;
    let db = MysqlService::connect_with_options(
        &url,
        MysqlServiceOptions::default().with_max_connections(1),
    )
    .await
    .unwrap();
    db.with_transaction(async |tx| {
        tx.execute(
            format!("CREATE TABLE IF NOT EXISTS {table} (id BIGINT PRIMARY KEY) ENGINE=InnoDB"),
            (),
        )
        .await?;
        tx.execute(format!("DELETE FROM {table}"), ()).await?;
        Ok(())
    })
    .await
    .unwrap();
    Some(db)
}

async fn insert<T: MysqlTransaction>(tx: &mut T, table: &str, id: i64) -> MysqlResult<()> {
    tx.execute(format!("INSERT INTO {table} (id) VALUES (?)"), (id,))
        .await?;
    Ok(())
}

async fn connection<T: MysqlTransaction>(tx: &mut T) -> MysqlResult<u64> {
    let row: Number = tx.fetch_one("SELECT CONNECTION_ID() AS value", ()).await?;
    Ok(row.value)
}

async fn generic_savepoint<T: MysqlNestedTransaction>(
    tx: &mut T,
    table: &str,
    id: i64,
) -> MysqlResult<()> {
    tx.with_transaction(async |inner| insert(inner, table, id).await)
        .await
}

#[tokio::test]
async fn nested_success_stays_on_one_connection_and_outer_rollback_undoes_every_level() {
    let table = "brz_nested_outer_rollback";
    let Some(db) = setup(table).await else {
        return;
    };
    let result: MysqlResult<()> = db
        .with_transaction(async |tx| {
            let id = connection(tx).await?;
            insert(tx, table, 1).await?;
            tx.with_transaction(async |inner| {
                assert_eq!(connection(inner).await?, id);
                insert(inner, table, 2).await?;
                inner
                    .with_transaction(async |deep| {
                        assert_eq!(connection(deep).await?, id);
                        generic_savepoint(deep, table, 3).await
                    })
                    .await
            })
            .await?;
            Err(MysqlError::InvalidQuery {
                reason: "outer failed".into(),
            })
        })
        .await;
    assert_eq!(
        result,
        Err(MysqlError::InvalidQuery {
            reason: "outer failed".into()
        })
    );
    let count: Number = db
        .fetch_one(
            format!("SELECT CAST(COUNT(*) AS UNSIGNED) AS value FROM {table}"),
            (),
        )
        .await
        .unwrap();
    assert_eq!(count.value, 0);
    db.close().await;
}

#[tokio::test]
async fn caught_inner_duplicate_rolls_back_only_its_work_and_allows_outer_commit() {
    let table = "brz_nested_inner_rollback";
    let Some(db) = setup(table).await else {
        return;
    };
    db.with_transaction(async |tx| {
        insert(tx, table, 1).await?;
        let result = tx
            .with_transaction(async |inner| {
                insert(inner, table, 2).await?;
                insert(inner, table, 1).await
            })
            .await;
        assert!(result.unwrap_err().is_duplicate_key());
        insert(tx, table, 3).await
    })
    .await
    .unwrap();
    let row: Number = db
        .fetch_one(
            format!("SELECT CAST(SUM(id) AS UNSIGNED) AS value FROM {table}"),
            (),
        )
        .await
        .unwrap();
    assert_eq!(row.value, 4);
    db.close().await;
}

#[tokio::test]
async fn cancelled_inner_scope_rolls_back_before_outer_connection_is_reused() {
    let table = "brz_nested_cancel";
    let Some(db) = setup(table).await else {
        return;
    };
    db.with_transaction(async |tx| {
        insert(tx, table, 1).await?;
        let result = tokio::time::timeout(
            Duration::from_millis(30),
            tx.with_transaction(async |inner| {
                insert(inner, table, 2).await?;
                std::future::pending::<MysqlResult<()>>().await
            }),
        )
        .await;
        assert!(result.is_err());
        insert(tx, table, 3).await
    })
    .await
    .unwrap();
    let count: Number = db
        .fetch_one(
            format!("SELECT CAST(COUNT(*) AS UNSIGNED) AS value FROM {table}"),
            (),
        )
        .await
        .unwrap();
    assert_eq!(count.value, 2);
    db.close().await;
}

#[tokio::test]
async fn routed_nested_scopes_inherit_and_restore_routing_on_the_same_socket() {
    let table = "brz_nested_route";
    let Some(db) = setup(table).await else {
        return;
    };
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let routed_seen = seen.clone();
    let routed = db.with_route(move |_: &str, key: &dyn brz_mysql::MysqlRouteKey| {
        routed_seen.lock().unwrap().push(key.as_u64()?);
        Ok(table)
    });
    routed
        .route(1_u64)
        .with_transaction(async |tx| {
            let id = connection(tx).await?;
            tx.route(2_u64)
                .with_transaction(async |inner| {
                    assert_eq!(connection(inner).await?, id);
                    inner
                        .execute("INSERT INTO {{items}} (id) VALUES (?)", (1_i64,))
                        .await?;
                    inner
                        .with_transaction(async |deep| {
                            assert_eq!(connection(deep).await?, id);
                            deep.execute("INSERT INTO {{items}} (id) VALUES (?)", (2_i64,))
                                .await?;
                            Ok(())
                        })
                        .await
                })
                .await?;
            assert_eq!(connection(tx).await?, id);
            tx.execute("INSERT INTO {{items}} (id) VALUES (?)", (3_i64,))
                .await?;
            Ok(())
        })
        .await
        .unwrap();
    let count: Number = db
        .fetch_one(
            format!("SELECT CAST(COUNT(*) AS UNSIGNED) AS value FROM {table}"),
            (),
        )
        .await
        .unwrap();
    assert_eq!(count.value, 3);
    assert_eq!(*seen.lock().unwrap(), [2, 2, 1]);
    db.close().await;
}
