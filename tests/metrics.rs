#![cfg(all(feature = "integration-tests", feature = "metrics"))]

use brz_mysql::{MysqlError, MysqlService, MysqlTransaction};
use futures_util::StreamExt;

fn counts(name: &str) -> [(u64, u64); 3] {
    let names = ["r", "w", "t"].map(|suffix| format!("{name}_{suffix}"));
    let mut counts = [(0, 0); 3];
    let mut found = 0;
    brz_metrics::visit(|name, kind, snapshot| {
        if let Some(index) = names.iter().position(|expected| expected == name) {
            assert_eq!(kind, "MYSQL");
            counts[index] = (snapshot.total, snapshot.failure);
            found += 1;
        }
    });
    assert_eq!(found, 3);
    counts
}

#[tokio::test]
async fn operations_share_host_handles_and_record_once() {
    let Ok(url) = std::env::var("BREEZE_MYSQL_TEST_URL") else {
        return;
    };
    let options: sqlx::mysql::MySqlConnectOptions = url.parse().unwrap();
    let port = options.get_port().to_string();
    let host = options.get_host();
    let mysql = MysqlService::connect(&url).await.unwrap();
    let other =
        MysqlService::connect_lazy(&format!("mysql://different:secret@{host}/other_db")).unwrap();
    assert_eq!(counts(&port), [(0, 0); 3]);
    let _: i64 = mysql.clone().fetch_one("SELECT 1", ()).await.unwrap();
    let missing: Option<i64> = mysql
        .fetch_optional("SELECT 1 FROM DUAL WHERE FALSE", ())
        .await
        .unwrap();
    assert_eq!(missing, None);
    assert_eq!(
        mysql
            .fetch_one::<_, _, i64>("SELECT 1 FROM DUAL WHERE FALSE", ())
            .await,
        Err(MysqlError::RowNotFound)
    );
    assert!(
        mysql
            .fetch_one::<_, _, i64>("SELECT 'wrong type'", ())
            .await
            .is_err()
    );
    assert_eq!(counts(&port)[0], (4, 2));

    drop(mysql.fetch::<_, _, i64>("SELECT 1", ()));
    assert_eq!(counts(&port)[0], (4, 2));
    let rows: Vec<i64> = mysql
        .fetch_all("SELECT 1 UNION ALL SELECT 2", ())
        .await
        .unwrap();
    assert_eq!(rows, [1, 2]);
    {
        let mut rows = Box::pin(mysql.fetch::<_, _, i64>("SELECT 1 UNION ALL SELECT 2", ()));
        assert_eq!(rows.next().await.unwrap().unwrap(), 1);
    }
    assert!(
        mysql
            .fetch_all::<_, _, i64>("SELECT 'wrong type'", ())
            .await
            .is_err()
    );
    assert_eq!(counts(&port)[0], (7, 4));
    mysql.execute("SET @metric_test = 1", ()).await.unwrap();
    assert!(mysql.execute("INVALID STATEMENT", ()).await.is_err());
    assert_eq!(counts(&port)[1], (2, 1));

    mysql
        .with_transaction(async |tx| {
            let _: i64 = tx.fetch_one("SELECT 1", ()).await?;
            let _: Vec<i64> = tx.fetch_all("SELECT 1", ()).await?;
            tx.execute("SET @metric_test = 2", ()).await?;
            Ok(())
        })
        .await
        .unwrap();
    let failed: Result<(), _> = mysql
        .with_transaction(async |tx| {
            let _: i64 = tx
                .route(1_i64)
                .fetch_one("SELECT 1 FROM DUAL WHERE FALSE", ())
                .await?;
            Ok(())
        })
        .await;
    assert_eq!(failed, Err(MysqlError::RowNotFound));
    // Start then cancel a transaction after it has acquired its connection.
    {
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let operation = mysql.with_transaction(async |_tx| {
            ready_tx.send(()).unwrap();
            std::future::pending::<Result<(), MysqlError>>().await
        });
        tokio::pin!(operation);
        tokio::select! {
            _ = ready_rx => {}
            result = &mut operation => panic!("unexpected completion: {result:?}"),
        }
    }
    assert_eq!(counts(&port), [(7, 4), (2, 1), (3, 2)]);
    other.close().await;
    mysql.close().await;
}
