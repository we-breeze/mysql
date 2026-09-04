#![cfg(feature = "integration-tests")]

use std::time::Duration;

use brz_mysql::{
    FromMysqlRow, Json, Mysql, MysqlError, MysqlResult, MysqlRow, MysqlSelectorValue, MysqlService,
    MysqlServiceOptions, MysqlTableSelection, MysqlTableSharding, MysqlTransaction,
};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use futures_util::{StreamExt, pin_mut};
use serde::{Deserialize, Serialize};

fn options() -> MysqlServiceOptions {
    MysqlServiceOptions {
        max_connections: 4,
        min_connections: 0,
        acquire_timeout: Duration::from_secs(5),
        idle_timeout: Some(Duration::from_secs(60)),
        max_lifetime: Some(Duration::from_secs(300)),
        slow_acquire_threshold: Duration::from_millis(500),
        test_before_acquire: true,
        charset: "utf8mb4".to_string(),
        timezone: Some("+08:00".to_string()),
        table_sharding: None,
    }
}

fn test_url() -> Option<String> {
    std::env::var("BREEZE_MYSQL_TEST_URL")
        .ok()
        .filter(|value| !value.is_empty())
}

#[derive(Debug, FromMysqlRow, PartialEq, Eq)]
struct TextRow {
    value: String,
}

#[tokio::test]
async fn typed_read_write_roundtrip_against_mysql_57() {
    let Some(url) = test_url() else {
        eprintln!("skipping: BREEZE_MYSQL_TEST_URL not set");
        return;
    };
    let service = MysqlService::connect_with_options(&url, options())
        .await
        .unwrap();

    service
        .execute(
            "CREATE TABLE IF NOT EXISTS brz_mysql_it (id BIGINT PRIMARY KEY, value VARCHAR(64) NULL)",
            (),
        )
        .await
        .unwrap();
    service
        .execute(
            "REPLACE INTO brz_mysql_it (id, value) VALUES (?, ?)",
            (1_i64, "roundtrip"),
        )
        .await
        .unwrap();

    let row: TextRow = service
        .fetch_one("SELECT value FROM brz_mysql_it WHERE id = ?", (1_i64,))
        .await
        .unwrap();
    assert_eq!(
        row,
        TextRow {
            value: "roundtrip".to_string()
        }
    );

    let through_trait: TextRow = Mysql::fetch_one(
        &service,
        "SELECT value FROM brz_mysql_it WHERE id = ?",
        (1_i64,),
    )
    .await
    .unwrap();
    assert_eq!(through_trait, row);
    service.close().await;
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
struct Payload {
    kind: String,
    enabled: bool,
}

#[derive(Debug, FromMysqlRow)]
struct WegentTypes {
    id: u64,
    signed_value: i32,
    flag: bool,
    ratio: f64,
    amount: String,
    text_value: String,
    bytes_value: Vec<u8>,
    date_value: NaiveDate,
    datetime_value: NaiveDateTime,
    time_value: NaiveTime,
    #[mysql(rename = "json_value")]
    payload: Json<Payload>,
    nullable_value: Option<i32>,
}

#[tokio::test]
async fn derive_and_json_roundtrip_against_mysql_57() {
    let Some(url) = test_url() else {
        eprintln!("skipping: BREEZE_MYSQL_TEST_URL not set");
        return;
    };
    let service = MysqlService::connect_with_options(&url, options())
        .await
        .unwrap();
    let date = NaiveDate::from_ymd_opt(2026, 9, 4).unwrap();
    let datetime = date.and_hms_micro_opt(13, 14, 15, 123_456).unwrap();
    let time = NaiveTime::from_hms_micro_opt(13, 14, 15, 123_456).unwrap();
    let payload = Payload {
        kind: "task".to_string(),
        enabled: true,
    };

    service
        .execute(
            "CREATE TABLE IF NOT EXISTS brz_mysql_types_it (             id BIGINT UNSIGNED PRIMARY KEY, signed_value INT NOT NULL, flag BOOLEAN NOT NULL,              ratio FLOAT NOT NULL, amount DECIMAL(10,2) NOT NULL, text_value VARCHAR(64) NOT NULL,              bytes_value VARBINARY(64) NOT NULL, date_value DATE NOT NULL,              datetime_value DATETIME(6) NOT NULL, time_value TIME(6) NOT NULL,              json_value JSON NOT NULL, nullable_value INT NULL)",
            (),
        )
        .await
        .unwrap();
    service
        .execute(
            "REPLACE INTO brz_mysql_types_it              (id, signed_value, flag, ratio, amount, text_value, bytes_value, date_value,               datetime_value, time_value, json_value, nullable_value)              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                7_u64,
                -42_i32,
                true,
                1.5_f32,
                "12.34",
                "wegent",
                vec![0_u8, 1, 255],
                date,
                datetime,
                time,
                Json(payload.clone()),
                Option::<i32>::None,
            ),
        )
        .await
        .unwrap();

    let row: WegentTypes = service
        .fetch_one("SELECT * FROM brz_mysql_types_it WHERE id = ?", (7_u64,))
        .await
        .unwrap();
    assert_eq!(row.id, 7);
    assert_eq!(row.signed_value, -42);
    assert!(row.flag);
    assert_eq!(row.ratio, 1.5);
    assert_eq!(row.amount, "12.34");
    assert_eq!(row.text_value, "wegent");
    assert_eq!(row.bytes_value, [0, 1, 255]);
    assert_eq!(row.date_value, date);
    assert_eq!(row.datetime_value, datetime);
    assert_eq!(row.time_value, time);
    assert_eq!(row.payload.into_inner(), payload);
    assert_eq!(row.nullable_value, None);

    service
        .execute(
            "CREATE TABLE IF NOT EXISTS brz_mysql_duplicate_it (id BIGINT PRIMARY KEY)",
            (),
        )
        .await
        .unwrap();
    service
        .execute(
            "REPLACE INTO brz_mysql_duplicate_it (id) VALUES (?)",
            (7_u64,),
        )
        .await
        .unwrap();
    let duplicate = service
        .execute(
            "INSERT INTO brz_mysql_duplicate_it (id) VALUES (?)",
            (7_u64,),
        )
        .await
        .unwrap_err();
    assert!(duplicate.is_duplicate_key());
    assert_eq!(duplicate.database_code(), Some(1062));
    assert_eq!(duplicate.database_sql_state(), Some("23000"));
    service.close().await;
}

#[derive(Debug, FromMysqlRow)]
struct IdRow {
    id: i64,
}

#[tokio::test]
async fn scoped_transaction_and_lazy_stream_are_bounded() {
    let Some(url) = test_url() else {
        eprintln!("skipping: BREEZE_MYSQL_TEST_URL not set");
        return;
    };
    let mut service_options = options();
    service_options.max_connections = 1;
    service_options.acquire_timeout = Duration::from_millis(100);
    service_options.slow_acquire_threshold = Duration::from_millis(50);
    let service = MysqlService::connect_with_options(&url, service_options)
        .await
        .unwrap();

    service
        .execute(
            "CREATE TABLE IF NOT EXISTS brz_mysql_tx_it (id BIGINT PRIMARY KEY, value VARCHAR(64) NULL)",
            (),
        )
        .await
        .unwrap();
    service
        .execute(
            "DELETE FROM brz_mysql_tx_it WHERE id BETWEEN ? AND ?",
            (2_i64, 10_i64),
        )
        .await
        .unwrap();

    let rollback_error = service
        .with_transaction(async |transaction| {
            transaction
                .execute(
                    "INSERT INTO brz_mysql_tx_it (id, value) VALUES (?, ?)",
                    (2_i64, "rollback"),
                )
                .await?;

            let started = std::time::Instant::now();
            let pool_error = service.execute("SELECT 1", ()).await.unwrap_err();
            assert_eq!(pool_error, MysqlError::PoolTimedOut);
            assert!(started.elapsed() < Duration::from_secs(1));

            Err::<(), _>(MysqlError::InvalidQuery {
                reason: "force rollback".to_string(),
            })
        })
        .await
        .unwrap_err();
    assert!(matches!(rollback_error, MysqlError::InvalidQuery { .. }));
    let rolled_back: Option<IdRow> = service
        .fetch_optional("SELECT id FROM brz_mysql_tx_it WHERE id = ?", (2_i64,))
        .await
        .unwrap();
    assert!(rolled_back.is_none());

    let committed = service
        .with_transaction(async |transaction| {
            for id in 3_i64..=7 {
                transaction
                    .execute(
                        "INSERT INTO brz_mysql_tx_it (id, value) VALUES (?, ?)",
                        (id, "commit"),
                    )
                    .await?;
            }
            let row: TextRow = transaction
                .fetch_one("SELECT value FROM brz_mysql_tx_it WHERE id = ?", (3_i64,))
                .await?;
            MysqlResult::Ok(row.value)
        })
        .await
        .unwrap();
    assert_eq!(committed, "commit");

    let lazy = service.fetch::<_, _, IdRow>(
        "SELECT id FROM brz_mysql_tx_it WHERE id >= ? ORDER BY id",
        (3_i64,),
    );
    service.execute("SELECT 1", ()).await.unwrap();
    drop(lazy);

    {
        let active = service.fetch::<_, _, IdRow>(
            "SELECT id FROM brz_mysql_tx_it WHERE id >= ? ORDER BY id",
            (3_i64,),
        );
        pin_mut!(active);
        assert_eq!(active.next().await.unwrap().unwrap().id, 3);
        assert_eq!(
            service.execute("SELECT 1", ()).await.unwrap_err(),
            MysqlError::PoolTimedOut
        );
    }

    let rows = service.fetch::<_, _, IdRow>(
        "SELECT id FROM brz_mysql_tx_it WHERE id >= ? ORDER BY id",
        (3_i64,),
    );
    pin_mut!(rows);
    let mut ids = Vec::new();
    while let Some(row) = rows.next().await {
        ids.push(row.unwrap().id);
    }
    assert_eq!(ids, [3, 4, 5, 6, 7]);

    let collected: Vec<IdRow> = service
        .fetch_all(
            "SELECT id FROM brz_mysql_tx_it WHERE id >= ? ORDER BY id",
            (6_i64,),
        )
        .await
        .unwrap();
    assert_eq!(
        collected.into_iter().map(|row| row.id).collect::<Vec<_>>(),
        [6, 7]
    );
    service.close().await;
}

#[derive(Debug, FromMysqlRow)]
struct RoutedRow {
    owner_id: u64,
    value: String,
}

#[tokio::test]
async fn table_selector_uses_the_first_sql_argument() {
    let Some(url) = test_url() else {
        eprintln!("skipping: BREEZE_MYSQL_TEST_URL not set");
        return;
    };
    let plain = MysqlService::connect_with_options(&url, options())
        .await
        .unwrap();
    for shard in [1_u32, 2] {
        plain
            .execute(
                format!(
                    "CREATE TABLE IF NOT EXISTS brz_mysql_route_it_{shard:04}                      (owner_id BIGINT UNSIGNED NOT NULL, id BIGINT PRIMARY KEY, value VARCHAR(64) NOT NULL)"
                ),
                (),
            )
            .await
            .unwrap();
    }

    let sharding = MysqlTableSharding::new(
        16,
        ["brz_mysql_route_it"],
        |first: MysqlSelectorValue<'_>| {
            Ok(MysqlTableSelection::Shard((first.as_u64()? % 16) as u32))
        },
    )
    .unwrap();
    let routed = MysqlService::connect_with_options(&url, options().with_table_sharding(sharding))
        .await
        .unwrap();

    routed
        .execute(
            "REPLACE INTO {{brz_mysql_route_it}} (owner_id, id, value) VALUES (?, ?, ?)",
            (17_u64, 1_i64, "shard-0001"),
        )
        .await
        .unwrap();
    let row: RoutedRow = routed
        .fetch_one(
            "SELECT owner_id, value FROM {{brz_mysql_route_it}} WHERE owner_id = ? AND id = ?",
            (17_u64, 1_i64),
        )
        .await
        .unwrap();
    assert_eq!(row.owner_id, 17);
    assert_eq!(row.value, "shard-0001");

    let physical: TextRow = plain
        .fetch_one(
            "SELECT value FROM brz_mysql_route_it_0001 WHERE owner_id = ? AND id = ?",
            (17_u64, 1_i64),
        )
        .await
        .unwrap();
    assert_eq!(physical.value, "shard-0001");

    let missing = routed
        .fetch_optional::<_, _, RoutedRow>("SELECT owner_id, value FROM {{brz_mysql_route_it}}", ())
        .await
        .unwrap_err();
    assert!(matches!(missing, MysqlError::InvalidQuery { .. }));

    routed.close().await;
    plain.close().await;
}

#[allow(dead_code)]
fn manual_mapping_example(row: MysqlRow) -> MysqlResult<TextRow> {
    Ok(TextRow {
        value: row.get_required("value")?,
    })
}
