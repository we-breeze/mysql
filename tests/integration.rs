#![cfg(feature = "integration-tests")]

use brz_mysql::{Executor as _, MySqlPoolConfig, MySqlResource, MySqlResourceConfig, Row as _};
use std::time::Duration;

fn config(url: String) -> MySqlPoolConfig {
    MySqlPoolConfig {
        url,
        max_connections: 4,
        acquire_timeout: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(60),
        max_lifetime: Duration::from_secs(300),
        slow_acquire_threshold: Duration::from_millis(500),
    }
}

fn url_for_database(url: &str, database: &str) -> String {
    let (base, query) = url.split_once('?').unwrap_or((url, ""));
    let authority = base.rsplit_once('/').unwrap().0;
    format!("{authority}/{database}?{query}")
}

fn test_url() -> Option<String> {
    std::env::var("BREEZE_MYSQL_TEST_URL")
        .ok()
        .filter(|value| !value.is_empty())
}

#[tokio::test]
async fn read_write_roundtrip_against_mysql_57() {
    let Some(url) = test_url() else {
        eprintln!("skipping: BREEZE_MYSQL_TEST_URL not set");
        return;
    };
    let resource = MySqlResource::connect_lazy(&MySqlResourceConfig {
        reader: config(url.clone()),
        writer: config(url),
    })
    .unwrap();

    resource
        .writer()
        .execute("CREATE TABLE IF NOT EXISTS brz_mysql_it (id BIGINT PRIMARY KEY, value VARCHAR(64) NULL)")
        .await
        .unwrap();
    brz_mysql::query("REPLACE INTO brz_mysql_it (id, value) VALUES (?, ?)")
        .bind(1_i64)
        .bind("roundtrip")
        .execute(resource.writer())
        .await
        .unwrap();
    let row = brz_mysql::query("SELECT value FROM brz_mysql_it WHERE id = ?")
        .bind(1_i64)
        .fetch_one(resource.reader())
        .await
        .unwrap();
    assert_eq!(row.try_get::<String, _>("value").unwrap(), "roundtrip");
    resource.close().await;
    assert!(resource.reader().acquire().await.is_err());
}

#[tokio::test]
async fn roles_route_to_distinct_databases_and_pool_timeout_is_bounded() {
    let Some(url) = test_url() else {
        eprintln!("skipping: BREEZE_MYSQL_TEST_URL not set");
        return;
    };
    let bootstrap = MySqlResource::connect_lazy(&MySqlResourceConfig {
        reader: config(url.clone()),
        writer: config(url.clone()),
    })
    .unwrap();
    bootstrap
        .writer()
        .execute("CREATE DATABASE IF NOT EXISTS brz_mysql_reader")
        .await
        .unwrap();
    bootstrap
        .writer()
        .execute("CREATE DATABASE IF NOT EXISTS brz_mysql_writer")
        .await
        .unwrap();

    let mut reader = config(url_for_database(&url, "brz_mysql_reader"));
    reader.max_connections = 1;
    reader.acquire_timeout = Duration::from_millis(100);
    reader.slow_acquire_threshold = Duration::from_millis(50);
    let resource = MySqlResource::connect_lazy(&MySqlResourceConfig {
        reader,
        writer: config(url_for_database(&url, "brz_mysql_writer")),
    })
    .unwrap();
    let reader_database: String = brz_mysql::query_scalar("SELECT DATABASE()")
        .fetch_one(resource.reader())
        .await
        .unwrap();
    let writer_database: String = brz_mysql::query_scalar("SELECT DATABASE()")
        .fetch_one(resource.writer())
        .await
        .unwrap();
    assert_eq!(reader_database, "brz_mysql_reader");
    assert_eq!(writer_database, "brz_mysql_writer");

    let held = resource.reader().acquire().await.unwrap();
    let started = std::time::Instant::now();
    let error = resource.reader().acquire().await.unwrap_err();
    assert!(matches!(error, brz_mysql::Error::PoolTimedOut));
    assert!(started.elapsed() < Duration::from_secs(1));
    drop(held);
    resource.close().await;
    bootstrap.close().await;
}
