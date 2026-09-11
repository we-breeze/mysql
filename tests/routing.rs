#![cfg(feature = "integration-tests")]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use brz_mysql::{
    FromMysqlRow, Mysql, MysqlError, MysqlResult, MysqlRouteKey, MysqlRouteOutput, MysqlRouting,
    MysqlService, MysqlServiceOptions, MysqlTransaction,
};
use futures_util::{StreamExt, pin_mut};

struct TaskRouting {
    prefix: &'static str,
    calls: Arc<AtomicUsize>,
}

impl MysqlRouting for TaskRouting {
    fn resolve<'a>(
        &'a self,
        template: &'a str,
        key: &'a dyn MysqlRouteKey,
    ) -> MysqlResult<impl MysqlRouteOutput + 'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let suffix = format!("{:04}", key.as_u64()? % 16);
        match template {
            "table_suffix" => Ok(suffix),
            "tasks" => Ok(format!("{}_{suffix}", self.prefix)),
            _ => Err(brz_mysql::MysqlError::InvalidQuery {
                reason: format!("unknown template: {template}"),
            }),
        }
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

async fn setup(prefix: &'static str) -> Option<(MysqlService, MysqlService, Arc<AtomicUsize>)> {
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
    fn attach<M: Mysql>(mysql: &M, routing: TaskRouting) -> MysqlService {
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
        mysql: MysqlService,
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
    // Lifecycle methods are available on routed services too, and close the
    // same pool used by the plain service.
    tasks.close().await;
    assert!(mysql.ping().await.is_err());
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

#[tokio::test]
async fn one_sharded_handle_can_mix_plain_tables_and_table_templates() {
    let Some((mysql, tasks, calls)) = setup("brz_mixed_route_it").await else {
        return;
    };
    tasks.execute(
        "CREATE TABLE IF NOT EXISTS brz_mixed_route_plain (id BIGINT UNSIGNED PRIMARY KEY, value VARCHAR(64) NOT NULL)",
        (),
    ).await.unwrap();
    tasks
        .execute("DELETE FROM brz_mixed_route_plain", ())
        .await
        .unwrap();
    tasks
        .execute(
            "INSERT INTO brz_mixed_route_plain (id, value) VALUES (101, ?)",
            ("ordinary",),
        )
        .await
        .unwrap();

    // Both implicit and explicit keys are ignored for plain SQL, including
    // strings/comments that look like templates. Exercise the generic API too.
    async fn read_plain<M: Mysql>(mysql: &M) {
        let row: ValueRow = mysql
            .fetch_one(
                "SELECT value FROM brz_mixed_route_plain WHERE value = ? /* {{tasks}} */",
                ("ordinary",),
            )
            .await
            .unwrap();
        assert_eq!(row.value, "ordinary");
        let literal: ValueRow = mysql
            .fetch_one("SELECT '{{tasks}}' AS value", ())
            .await
            .unwrap();
        assert_eq!(literal.value, "{{tasks}}");
        let missing: Option<ValueRow> = mysql
            .fetch_optional(
                "SELECT value FROM brz_mixed_route_plain WHERE value = ?",
                ("missing",),
            )
            .await
            .unwrap();
        assert!(missing.is_none());
        let all: Vec<ValueRow> = mysql
            .fetch_all("SELECT value FROM brz_mixed_route_plain", ())
            .await
            .unwrap();
        assert_eq!(
            all,
            [ValueRow {
                value: "ordinary".into()
            }]
        );
        let rows = mysql.fetch::<_, _, ValueRow>("SELECT value FROM brz_mixed_route_plain", ());
        pin_mut!(rows);
        assert_eq!(rows.next().await.unwrap().unwrap().value, "ordinary");
        assert!(rows.next().await.is_none());
    }
    read_plain(&tasks).await;
    read_plain(&tasks.route("not a numeric routing key")).await;
    tasks
        .route("unused")
        .execute(
            "UPDATE brz_mixed_route_plain SET value = ? WHERE id = 101",
            ("ordinary",),
        )
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let joined: ValueRow = tasks.fetch_one(
        "SELECT p.value FROM {{tasks}} t JOIN brz_mixed_route_plain p ON p.id = t.id WHERE t.uid = ?",
        (17_u64,),
    ).await.unwrap();
    assert_eq!(joined.value, "ordinary");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let explicit: ValueRow = tasks
        .route(18_u64)
        .fetch_one("SELECT value FROM {{tasks}} WHERE id = ?", (101_u64,))
        .await
        .unwrap();
    assert_eq!(explicit.value, "original-2");
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    tasks
        .execute("DROP TABLE brz_mixed_route_plain", ())
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    mysql.close().await;
}

// Business key types do not implement SQL argument encoding or Clone.
struct ByTaskId(u64);
struct ByUserId(u64);

struct TypedRouting {
    prefix: String,
    writes: Arc<AtomicUsize>,
}

struct TableOutput<'a> {
    prefix: &'a str,
    slot: Option<u64>,
    writes: &'a AtomicUsize,
}

impl MysqlRouteOutput for TableOutput<'_> {
    fn write_to(&self, out: &mut dyn std::fmt::Write) -> std::fmt::Result {
        self.writes.fetch_add(1, Ordering::SeqCst);
        out.write_str(self.prefix)?;
        if let Some(slot) = self.slot {
            if !self.prefix.is_empty() {
                out.write_str("_")?;
            }
            write!(out, "{slot:04}")?;
        }
        Ok(())
    }
}

impl MysqlRouting for TypedRouting {
    fn resolve<'a>(
        &'a self,
        template: &'a str,
        key: &'a dyn MysqlRouteKey,
    ) -> MysqlResult<impl MysqlRouteOutput + 'a> {
        let slot = if let Some(ByTaskId(id)) = key.downcast_ref::<ByTaskId>() {
            if *id < (1 << 37) {
                None
            } else {
                Some(((id >> 37) & 0xffff) % 16)
            }
        } else if let Some(ByUserId(uid)) = key.downcast_ref::<ByUserId>() {
            Some(uid % 16)
        } else {
            return Err(MysqlError::InvalidQuery {
                reason: "expected ByTaskId or ByUserId".into(),
            });
        };
        let prefix = match template {
            "tasks" => self.prefix.as_str(),
            "aux" => "brz_custom_route_aux",
            "slot" => "",
            _ => {
                return Err(MysqlError::InvalidQuery {
                    reason: "unknown template".into(),
                });
            }
        };
        Ok(TableOutput {
            prefix,
            slot,
            writes: &self.writes,
        })
    }
}

#[tokio::test]
async fn custom_keys_and_borrowed_outputs_work_for_queries_joins_streams_and_transactions() {
    let Some((mysql, _, _)) = setup("brz_custom_route_it").await else {
        return;
    };
    mysql
        .execute(
            "CREATE TABLE IF NOT EXISTS brz_custom_route_it LIKE brz_custom_route_it_0001",
            (),
        )
        .await
        .unwrap();
    mysql
        .execute(
            "REPLACE INTO brz_custom_route_it (id, uid, value) VALUES (101, 17, 'legacy')",
            (),
        )
        .await
        .unwrap();
    mysql
        .execute(
            "CREATE TABLE IF NOT EXISTS brz_custom_route_aux_0001 (id BIGINT UNSIGNED PRIMARY KEY)",
            (),
        )
        .await
        .unwrap();
    mysql
        .execute("REPLACE INTO brz_custom_route_aux_0001 VALUES (101)", ())
        .await
        .unwrap();
    let writes = Arc::new(AtomicUsize::new(0));
    let tasks = mysql.with_route(TypedRouting {
        prefix: "brz_custom_route_it".into(),
        writes: writes.clone(),
    });
    let legacy = tasks.route(ByTaskId(17));
    let user = tasks.route(ByUserId(17));
    let new_id = tasks.route(ByTaskId((17 << 37) | 101));
    async fn read_by_key<M: Mysql, K: MysqlRouteKey>(mysql: &M, key: K) -> MysqlResult<ValueRow> {
        mysql
            .route(key)
            .fetch_one("SELECT value FROM {{tasks}} WHERE id = ?", (101_u64,))
            .await
    }
    let old = read_by_key(&tasks, ByTaskId(17)).await.unwrap();
    assert_eq!(old.value, "legacy");
    let current: Option<ValueRow> = user
        .fetch_optional("SELECT value FROM {{tasks}} WHERE id = ?", (101_u64,))
        .await
        .unwrap();
    assert_eq!(current.unwrap().value, "original-1");
    let current: Vec<ValueRow> = new_id
        .fetch_all("SELECT value FROM {{tasks}}", ())
        .await
        .unwrap();
    assert_eq!(current[0].value, "original-1");
    assert_eq!(writes.load(Ordering::SeqCst), 3);

    let rows = user.fetch::<_, _, ValueRow>("SELECT value FROM {{tasks}}", ());
    assert_eq!(writes.load(Ordering::SeqCst), 3);
    pin_mut!(rows);
    assert_eq!(rows.next().await.unwrap().unwrap().value, "original-1");
    assert!(rows.next().await.is_none());
    assert_eq!(writes.load(Ordering::SeqCst), 4);

    let joined: ValueRow = user.fetch_one(
        "SELECT a.value FROM {{tasks}} a JOIN {{tasks}} b ON a.id=b.id JOIN {{aux}} c ON c.id=a.id",
        (),
    ).await.unwrap();
    assert_eq!(joined.value, "original-1");
    assert_eq!(writes.load(Ordering::SeqCst), 6); // two distinct names, not three occurrences
    let fragment: ValueRow = user
        .fetch_one("SELECT value FROM brz_custom_route_it_{{slot}}", ())
        .await
        .unwrap();
    assert_eq!(fragment.value, "original-1");

    let before_plain = writes.load(Ordering::SeqCst);
    let plain: String = user.fetch_one("SELECT '{{tasks}}'", ()).await.unwrap();
    assert_eq!(plain, "{{tasks}}");
    assert_eq!(writes.load(Ordering::SeqCst), before_plain);
    assert!(matches!(
        tasks
            .route(17_u64)
            .execute("DELETE FROM {{tasks}}", ())
            .await,
        Err(MysqlError::InvalidQuery { .. })
    ));

    // A typed service key is inherited by transactions; a per-statement typed
    // key can select the legacy table without changing subsequent statements.
    let result: MysqlResult<()> = user
        .with_transaction(async |tx| {
            tx.route(ByTaskId(17))
                .execute("UPDATE {{tasks}} SET value = ?", ("legacy-update",))
                .await?;
            tx.execute("UPDATE {{tasks}} SET value = ?", ("new-update",))
                .await?;
            let old: ValueRow = tx
                .route(ByTaskId(17))
                .fetch_one("SELECT value FROM {{tasks}}", ())
                .await?;
            assert_eq!(old.value, "legacy-update");
            let current: ValueRow = tx.fetch_one("SELECT value FROM {{tasks}}", ()).await?;
            assert_eq!(current.value, "new-update");
            let streamed = tx
                .route(ByTaskId((17 << 37) | 101))
                .fetch_all::<_, _, ValueRow>("SELECT value FROM {{tasks}}", ())
                .await?;
            assert_eq!(streamed[0].value, "new-update");
            Err(MysqlError::InvalidQuery {
                reason: "rollback typed routes".into(),
            })
        })
        .await;
    assert_eq!(
        result.unwrap_err(),
        MysqlError::InvalidQuery {
            reason: "rollback typed routes".into()
        }
    );
    assert_eq!(
        legacy
            .fetch_one::<_, _, ValueRow>("SELECT value FROM {{tasks}}", ())
            .await
            .unwrap()
            .value,
        "legacy"
    );
    assert_eq!(
        user.fetch_one::<_, _, ValueRow>("SELECT value FROM {{tasks}}", ())
            .await
            .unwrap()
            .value,
        "original-1"
    );
    mysql
        .execute(
            "DROP TABLE brz_custom_route_it, brz_custom_route_aux_0001",
            (),
        )
        .await
        .unwrap();
    mysql.close().await;
}
