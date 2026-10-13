//! Server-side statement lifetime checks run by the MySQL integration suite.

use super::*;
use sqlx::{Connection, Row};

fn test_url() -> String {
    std::env::var("BREEZE_MYSQL_TEST_URL")
        .expect("integration-tests requires BREEZE_MYSQL_TEST_URL")
}

fn options() -> MysqlServiceOptions {
    MysqlServiceOptions::default()
        .with_max_connections(1)
        .with_query_timeout(std::time::Duration::from_secs(10))
}

async fn assert_statements_closed(pool: &MySqlPool) {
    let mut connection = pool.acquire().await.unwrap();
    assert_eq!(connection.cached_statements_size(), 0);
    // Observe this connection via the text protocol so the check adds no prepare.
    let rows = sqlx::Executor::fetch_all(
        &mut *connection,
        "SHOW SESSION STATUS WHERE Variable_name IN \
         ('Com_stmt_prepare', 'Com_stmt_execute', 'Com_stmt_close')",
    )
    .await
    .unwrap();
    let counters: std::collections::HashMap<String, u64> = rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            let value: String = row.get(1);
            (name, value.parse().unwrap())
        })
        .collect();
    assert!(counters["Com_stmt_prepare"] > 0);
    assert_eq!(counters["Com_stmt_prepare"], counters["Com_stmt_execute"]);
    assert_eq!(counters["Com_stmt_prepare"], counters["Com_stmt_close"]);
}

async fn exercise_queries(mysql: &MysqlService) {
    mysql
        .execute("SET @brz_mysql_cache_test = ?", (7_u64,))
        .await
        .unwrap();
    for value in 1..=5_u64 {
        let row: u64 = mysql.fetch_one("SELECT ?", (value,)).await.unwrap();
        assert_eq!(row, value);
        let row: Option<u64> = mysql
            .fetch_optional("SELECT ? FROM DUAL WHERE FALSE", (value,))
            .await
            .unwrap();
        assert_eq!(row, None);
        let literal: u64 = mysql
            .fetch_one(format!("SELECT CAST({value} AS UNSIGNED)"), ())
            .await
            .unwrap();
        assert_eq!(literal, value);
        let placeholders = vec!["?"; value as usize].join(", ");
        let rows: Vec<u64> = mysql
            .fetch_all(
                format!("SELECT CAST(1 AS UNSIGNED) FROM DUAL WHERE 1 IN ({placeholders})"),
                (1..=value).collect::<Vec<_>>(),
            )
            .await
            .unwrap();
        assert_eq!(rows, [1]);
    }
    let rows = mysql.fetch::<_, _, u64>("SELECT ? UNION ALL SELECT ?", (8_u64, 9_u64));
    pin_mut!(rows);
    let mut values = Vec::new();
    while let Some(row) = rows.next().await {
        values.push(row.unwrap());
    }
    assert_eq!(values, [8, 9]);
}

#[tokio::test]
async fn eager_and_lazy_read_write_pools_release_every_statement() {
    let url = test_url();
    let services = [
        MysqlService::connect_lazy_with_options(&url, options()).unwrap(),
        MysqlService::connect_with_options(&url, options())
            .await
            .unwrap(),
        MysqlService::connect_lazy_read_write_with_options(&url, &url, options()).unwrap(),
        MysqlService::connect_read_write_with_options(&url, &url, options())
            .await
            .unwrap(),
    ];
    for service in services {
        exercise_queries(&service).await;
        assert_statements_closed(&service.master_pool).await;
        if let Some(slave) = &service.slave_pool {
            assert_statements_closed(slave).await;
        }
        service.close().await;
    }
}

#[tokio::test]
async fn connection_url_cannot_reenable_statement_caching() {
    let url = test_url();
    let separator = if url.contains('?') { '&' } else { '?' };
    let url = format!("{url}{separator}statement-cache-capacity=100");
    let service = MysqlService::connect_lazy_with_options(&url, options()).unwrap();
    exercise_queries(&service).await;
    assert_statements_closed(&service.master_pool).await;
    service.close().await;
}

#[tokio::test]
async fn transactions_release_statements_on_commit_and_rollback() {
    let url = test_url();
    let service =
        MysqlService::connect_lazy_read_write_with_options(&url, &url, options()).unwrap();
    service
        .with_transaction(async |transaction| {
            transaction
                .execute("SET @brz_mysql_cache_test = ?", (7_u64,))
                .await?;
            for value in 1..=5_u64 {
                let row: u64 = transaction.fetch_one("SELECT ?", (value,)).await?;
                assert_eq!(row, value);
                let row: Option<u64> = transaction
                    .fetch_optional("SELECT ? FROM DUAL WHERE FALSE", (value,))
                    .await?;
                assert_eq!(row, None);
                let rows: Vec<u64> = transaction.fetch_all("SELECT ?", (value,)).await?;
                assert_eq!(rows, [value]);
            }
            let rows =
                transaction.fetch::<_, _, u64>("SELECT ? UNION ALL SELECT ?", (8_u64, 9_u64));
            pin_mut!(rows);
            let mut values = Vec::new();
            while let Some(row) = rows.next().await {
                values.push(row?);
            }
            assert_eq!(values, [8, 9]);
            Ok(())
        })
        .await
        .unwrap();
    assert_statements_closed(&service.master_pool).await;
    let outcome: MysqlResult<()> = service
        .with_transaction(async |transaction| {
            let row: u64 = transaction.fetch_one("SELECT ?", (10_u64,)).await?;
            assert_eq!(row, 10);
            Err(MysqlError::InvalidQuery {
                reason: "force rollback".into(),
            })
        })
        .await;
    assert!(matches!(outcome, Err(MysqlError::InvalidQuery { .. })));
    assert_statements_closed(&service.master_pool).await;
    service.close().await;
}
