#![cfg(feature = "integration-tests")]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use brz_mysql::{
    FromMysqlRow, Mysql, MysqlError, MysqlResult, MysqlRoute, MysqlRouteValue, MysqlRouting,
    MysqlService, MysqlServiceOptions, MysqlTransaction, ShardedMysqlService,
};
use futures_util::{StreamExt, pin_mut};

struct TaskRouting {
    prefix: &'static str,
    calls: Arc<AtomicUsize>,
}

impl MysqlRouting for TaskRouting {
    fn resolve(&self, key: MysqlRouteValue<'_>) -> MysqlResult<MysqlRoute> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let suffix = format!("{:04}", key.as_u64()? % 16);
        MysqlRoute::new()
            .with_table_suffix(&suffix)?
            .with_table("tasks", format!("{}_{suffix}", self.prefix))?
            .with_table("unused", "unused_table")
    }
}

#[derive(Debug, PartialEq, Eq, FromMysqlRow)]
struct ValueRow {
    value: String,
}

#[derive(Debug, FromMysqlRow)]
struct IdRow {
    id: u64,
}

async fn setup(
    prefix: &'static str,
) -> Option<(MysqlService, ShardedMysqlService, Arc<AtomicUsize>)> {
    let Some(url) = std::env::var("BREEZE_MYSQL_TEST_URL")
        .ok()
        .filter(|url| !url.is_empty())
    else {
        eprintln!("skipping: BREEZE_MYSQL_TEST_URL not set");
        return None;
    };
    let mysql = MysqlService::connect_with_options(
        &url,
        MysqlServiceOptions::default().with_max_connections(1),
    )
    .await
    .unwrap();
    for shard in [1, 2] {
        mysql.execute(format!("CREATE TABLE IF NOT EXISTS {prefix}_{shard:04} (id BIGINT UNSIGNED PRIMARY KEY, uid BIGINT UNSIGNED NOT NULL, value VARCHAR(64) NOT NULL)"), ()).await.unwrap();
        mysql
            .execute(
                format!("REPLACE INTO {prefix}_{shard:04} (id, uid, value) VALUES (?, ?, ?)"),
                (101_u64, 16_u64 + shard, format!("original-{shard}")),
            )
            .await
            .unwrap();
    }
    let calls = Arc::new(AtomicUsize::new(0));
    // Exercise the trait entry point, as used by generic repositories.
    fn attach<M: Mysql>(mysql: &M, routing: TaskRouting) -> ShardedMysqlService {
        mysql.with_route(routing)
    }
    let tasks = attach(
        &mysql,
        TaskRouting {
            prefix,
            calls: calls.clone(),
        },
    );
    Some((mysql, tasks, calls))
}

#[tokio::test]
async fn owned_service_shares_one_pool_and_isolates_concurrent_query_keys() {
    let Some((mysql, tasks, calls)) = setup("brz_query_route_it").await else {
        return;
    };

    // A repository can own the concrete routed service without a lifetime or
    // generic policy parameter. Both services continue to share the same pool.
    struct Tasks {
        mysql: ShardedMysqlService,
    }
    let repository = Tasks {
        mysql: tasks.clone(),
    };
    let normal: ValueRow = mysql
        .fetch_one("SELECT ? AS value", ("username",))
        .await
        .unwrap();
    assert_eq!(normal.value, "username");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let first: ValueRow = repository
        .mysql
        .fetch_one(
            "SELECT value FROM {{tasks}} WHERE uid = ? AND id = ?",
            (17_u64, 101_u64),
        )
        .await
        .unwrap();
    assert_eq!(first.value, "original-1");
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let base_connection: IdRow = mysql
        .fetch_one("SELECT CONNECTION_ID() AS id", ())
        .await
        .unwrap();
    let routed_connection: IdRow = tasks
        .route(17_u64)
        .fetch_one("SELECT CONNECTION_ID() AS id FROM {{tasks}} LIMIT 1", ())
        .await
        .unwrap();
    assert_eq!(base_connection.id, routed_connection.id);
    assert_eq!(tasks.pool_stats().size, 1);
    assert_eq!(mysql.pool_stats().size, 1);

    // Routing uid differs from both the first SQL argument and the task id.
    tasks
        .route(17_u64)
        .execute(
            "UPDATE {{tasks}} SET value = ? WHERE id = ?",
            ("explicit-1", 101_u64),
        )
        .await
        .unwrap();
    let error = tasks
        .execute(
            "UPDATE {{tasks}} SET value = ? WHERE id = ?",
            ("wrong-key", 101_u64),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, MysqlError::InvalidQuery { .. }));

    let user_one = tasks.route(17_u64);
    let user_two = tasks.route(18_u64);
    let (one, two) = tokio::join!(
        user_one
            .fetch_one::<_, _, ValueRow>("SELECT value FROM {{tasks}} WHERE id = ?", (101_u64,)),
        user_two.fetch_one::<_, _, ValueRow>(
            "SELECT value FROM brz_query_route_it_{{table_suffix}} WHERE id = ?",
            (101_u64,)
        ),
    );
    assert_eq!(one.unwrap().value, "explicit-1");
    assert_eq!(two.unwrap().value, "original-2");

    // Neither explicit handle changes the repository's first-argument fallback.
    let default: ValueRow = tasks
        .fetch_one("SELECT value FROM {{tasks}} WHERE uid = ?", (18_u64,))
        .await
        .unwrap();
    assert_eq!(default.value, "original-2");
    let old_count = calls.load(Ordering::SeqCst);
    let stream =
        tasks.fetch::<_, _, ValueRow>("SELECT value FROM {{tasks}} WHERE uid = ?", (17_u64,));
    assert_eq!(calls.load(Ordering::SeqCst), old_count);
    pin_mut!(stream);
    assert_eq!(stream.next().await.unwrap().unwrap().value, "explicit-1");
    assert!(stream.next().await.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), old_count + 1);

    let missing = tasks
        .route(17_u64)
        .fetch_optional::<_, _, ValueRow>("SELECT value FROM {{tasks}} WHERE id = ?", (999_u64,))
        .await
        .unwrap();
    assert!(missing.is_none());
    let all: Vec<ValueRow> = tasks
        .route(18_u64)
        .fetch_all("SELECT value FROM {{tasks}}", ())
        .await
        .unwrap();
    assert_eq!(
        all,
        [ValueRow {
            value: "original-2".into()
        }]
    );
    drop(repository);
    drop(user_one);
    drop(user_two);
    assert_eq!(
        mysql
            .fetch_one::<_, _, ValueRow>("SELECT ? AS value", ("still open",))
            .await
            .unwrap()
            .value,
        "still open"
    );
    mysql.close().await;
}

#[tokio::test]
async fn single_database_transaction_routes_each_statement_and_rolls_back_all_shards() {
    let Some((mysql, tasks, calls)) = setup("brz_tx_route_it").await else {
        return;
    };
    let initial_calls = calls.load(Ordering::SeqCst);
    // No .route() is needed to start a single-database transaction.
    let result: MysqlResult<()> = tasks
        .with_transaction(async |tx| {
            let plain: ValueRow = tx.fetch_one("SELECT ? AS value", ("ordinary",)).await?;
            assert_eq!(plain.value, "ordinary");
            assert_eq!(calls.load(Ordering::SeqCst), initial_calls);
            let conn: IdRow = tx.fetch_one("SELECT CONNECTION_ID() AS id", ()).await?;
            for uid in [17_u64, 18] {
                tx.execute(
                    "UPDATE {{tasks}} SET value = 'temporary' WHERE uid = ?",
                    (uid,),
                )
                .await?;
            }
            tx.route(17_u64)
                .execute(
                    "UPDATE {{tasks}} SET value = ? WHERE id = ?",
                    ("explicit-tx", 101_u64),
                )
                .await?;
            let one: ValueRow = tx
                .route(17_u64)
                .fetch_one("SELECT value FROM {{tasks}} WHERE id = ?", (101_u64,))
                .await?;
            assert_eq!(one.value, "explicit-tx");
            // Dropping the explicit view restores the ordinary fallback behavior.
            let other: ValueRow = tx
                .fetch_one("SELECT value FROM {{tasks}} WHERE uid = ?", (18_u64,))
                .await?;
            assert_eq!(other.value, "temporary");
            let routed_conn: IdRow = tx
                .route(18_u64)
                .fetch_one("SELECT CONNECTION_ID() AS id FROM {{tasks}} LIMIT 1", ())
                .await?;
            assert_eq!(conn.id, routed_conn.id);
            Err(MysqlError::InvalidQuery {
                reason: "rollback test".into(),
            })
        })
        .await;
    assert_eq!(
        result.unwrap_err(),
        MysqlError::InvalidQuery {
            reason: "rollback test".into()
        }
    );
    for uid in [17_u64, 18] {
        let row: ValueRow = tasks
            .fetch_one("SELECT value FROM {{tasks}} WHERE uid = ?", (uid,))
            .await
            .unwrap();
        assert_eq!(row.value, format!("original-{}", uid - 16));
    }

    tasks
        .with_transaction(async |tx| {
            tx.execute(
                "UPDATE {{tasks}} SET value = 'committed-1' WHERE uid = ?",
                (17_u64,),
            )
            .await?;
            tx.route(18_u64)
                .execute(
                    "UPDATE {{tasks}} SET value = ? WHERE id = ?",
                    ("committed-2", 101_u64),
                )
                .await?;
            let rows: Vec<ValueRow> = tx
                .route(18_u64)
                .fetch_all("SELECT value FROM {{tasks}}", ())
                .await?;
            assert_eq!(rows[0].value, "committed-2");
            Ok(())
        })
        .await
        .unwrap();
    for uid in [17_u64, 18] {
        let row: ValueRow = tasks
            .fetch_one("SELECT value FROM {{tasks}} WHERE uid = ?", (uid,))
            .await
            .unwrap();
        assert_eq!(row.value, format!("committed-{}", uid - 16));
    }
    mysql.close().await;
}
