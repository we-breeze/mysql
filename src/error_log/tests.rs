use super::*;
use crate::{
    Json, MysqlResult, MysqlService, MysqlTransaction, MysqlTransactionService, MysqlValue,
};
use futures_util::{StreamExt, pin_mut};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tracing::{
    Event, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<BTreeMap<String, String>>>>);

struct Fields(BTreeMap<String, String>);

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
}

impl Subscriber for Capture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target() == "breeze.mysql" && *metadata.level() == tracing::Level::ERROR
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        if self.enabled(event.metadata()) {
            let mut fields = Fields(BTreeMap::new());
            event.record(&mut fields);
            self.0.lock().unwrap().push(fields.0);
        }
    }
}

fn transaction() -> MysqlTransactionService {
    MysqlTransactionService {
        inner: None,
        route: None,
        query_timeout: Duration::from_secs(1),
    }
}

fn formatted_log<A: MysqlArgs>(operation: &'static str, sql: &str, arguments: &A) -> QueryLog {
    let mut log = QueryLog::new(operation, sql).unwrap();
    log.capture_parameters(arguments);
    log
}

#[tokio::test]
async fn forwarded_custom_values_and_argument_containers_record_bound_values() {
    struct Forward(u64);
    impl MysqlValue for Forward {
        fn write(self, writer: &mut crate::MysqlValueWriter) -> MysqlResult<()> {
            writer.push(self.0)
        }
    }
    struct CustomArgs(Vec<Forward>);
    impl MysqlArgs for CustomArgs {
        fn len(&self) -> usize {
            self.0.len()
        }
        fn encoded_size_hint(&self) -> usize {
            0
        }
        fn write(self, writer: &mut crate::MysqlValueWriter) -> MysqlResult<()> {
            for value in self.0 {
                writer.push(value)?;
            }
            Ok(())
        }
    }
    let capture = Capture::default();
    let dispatch = tracing::Dispatch::new(capture.clone());
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let mut tx = transaction();
    assert!(
        tx.execute("SELECT ?, ?", (Forward(17), "text"))
            .await
            .is_err()
    );
    assert!(
        tx.execute("SELECT ?", CustomArgs(vec![Forward(23)]))
            .await
            .is_err()
    );
    let events = capture.0.lock().unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["parameters"], "[17, \"text\"]");
    assert_eq!(events[1]["parameters"], "[23]");
}

#[tokio::test]
async fn statements_emit_one_error_before_returning_or_yielding() {
    let capture = Capture::default();
    let dispatch = tracing::Dispatch::new(capture.clone());
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let mysql = MysqlService::connect_lazy("mysql://user:secret@127.0.0.1:1/test").unwrap();
    mysql.close().await;
    let sql = "SELECT ?";
    assert!(mysql.execute(sql, (17,)).await.is_err());
    assert!(mysql.fetch_optional::<_, _, i64>(sql, (17,)).await.is_err());
    assert!(mysql.fetch_one::<_, _, i64>(sql, (17,)).await.is_err());
    assert!(mysql.fetch_all::<_, _, i64>(sql, (17,)).await.is_err());
    let rows = mysql.fetch::<_, _, i64>(sql, (17,));
    pin_mut!(rows);
    assert!(rows.next().await.unwrap().is_err());
    // Consumers may stop at the first error; it must already have been logged.
    let events = capture.0.lock().unwrap();
    assert_eq!(events.len(), 5);
    for (event, operation) in events.iter().zip([
        "execute",
        "fetch_optional",
        "fetch_one",
        "fetch_all",
        "fetch",
    ]) {
        assert_eq!(event["operation"], operation);
        assert_eq!(event["sql"], sql);
        assert_eq!(event["parameters"], "[17]");
        assert!(!event["error"].is_empty());
        assert!(event["elapsed_ms"].parse::<f64>().unwrap() >= 0.0);
    }
}

#[tokio::test]
async fn transaction_views_keep_original_sql_and_log_errors_once() {
    let capture = Capture::default();
    let dispatch = tracing::Dispatch::new(capture.clone());
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let mut tx = transaction();
    assert!(tx.execute("UPDATE items SET id=?", (23,)).await.is_err());
    assert!(
        tx.fetch_optional::<_, _, i64>("SELECT ?", (23,))
            .await
            .is_err()
    );
    assert!(tx.fetch_one::<_, _, i64>("SELECT ?", (23,)).await.is_err());
    assert!(tx.fetch_all::<_, _, i64>("SELECT ?", (23,)).await.is_err());
    {
        let rows = tx.fetch::<_, _, i64>("SELECT ?", (23,));
        pin_mut!(rows);
        assert!(rows.next().await.unwrap().is_err());
    }
    let sql = "SELECT id FROM {{items}} WHERE id=?";
    let mut routed = tx.route(7_u64);
    assert!(routed.execute(sql, (23,)).await.is_err());
    assert!(
        routed
            .fetch_optional::<_, _, i64>(sql, (23,))
            .await
            .is_err()
    );
    assert!(routed.fetch_one::<_, _, i64>(sql, (23,)).await.is_err());
    assert!(routed.fetch_all::<_, _, i64>(sql, (23,)).await.is_err());
    let rows = routed.fetch::<_, _, i64>(sql, (23,));
    pin_mut!(rows);
    assert!(rows.next().await.unwrap().is_err());
    let events = capture.0.lock().unwrap();
    assert_eq!(events.len(), 10);
    for (index, event) in events.iter().enumerate() {
        assert_eq!(event["parameters"], "[23]");
        if index >= 5 {
            assert_eq!(event["sql"], sql);
        }
    }
}

#[tokio::test]
async fn outer_transaction_failures_do_not_add_error_events() {
    let capture = Capture::default();
    let dispatch = tracing::Dispatch::new(capture.clone());
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let mysql = MysqlService::connect_lazy("mysql://user:secret@127.0.0.1:1/test").unwrap();
    mysql.close().await;
    assert!(mysql.with_transaction(async |_| Ok(())).await.is_err());
    assert!(capture.0.lock().unwrap().is_empty());
}

#[test]
fn sql_limit_preserves_utf8_and_parameters_include_values_without_debug_bounds() {
    #[derive(serde::Serialize)]
    struct Payload {
        name: &'static str,
    }
    let capture = Capture::default();
    tracing::subscriber::with_default(capture.clone(), || {
        let sql = format!("SELECT '{}' WHERE id=?", "汉".repeat(2000));
        let args = (
            17_i64,
            "hello\n\"世界",
            true,
            Option::<i64>::None,
            &[0x00_u8, 0xff][..],
            Json(Payload { name: "test" }),
            "12.50".parse::<sqlx::types::BigDecimal>().unwrap(),
            Some(Json(serde_json::json!({"enabled": true}))),
        );
        let log = formatted_log("execute", &sql, &args);
        assert!(log.sql.len() <= 4096);
        assert!(sql.starts_with(log.sql.as_ref()));
        assert!(log.sql_truncated);
        assert!(
            log.parameters
                .contains("17, \"hello\\n\\\"世界\", true, null, 0x00ff")
        );
        assert!(log.parameters.contains(r#"{"name":"test"}"#));
        assert!(log.parameters.contains("12.50"));
        assert!(log.parameters.contains(r#"{"enabled":true}"#));
        log.error(
            Duration::from_micros(1250),
            &MysqlError::Database {
                code: Some(1064),
                sql_state: Some("42000".into()),
                message: "syntax error".into(),
            },
        );
        assert_eq!(formatted_log("execute", "SELECT 1", &()).parameters, "[]");
        assert_eq!(
            formatted_log("execute", "SELECT ?", &vec![1, 2]).parameters,
            "[1, 2]"
        );
        assert_eq!(
            formatted_log("execute", "SELECT ?", &[3, 4]).parameters,
            "[3, 4]"
        );
    });
    let events = capture.0.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["database_code"], "1064");
    assert_eq!(events[0]["sql_state"], "42000");
    assert_eq!(events[0]["elapsed_ms"], "1.25");
}

struct BadFormat;

impl MysqlValue for BadFormat {
    fn write(self, writer: &mut crate::MysqlValueWriter) -> MysqlResult<()> {
        writer.push(1_i64)
    }
    fn fmt_log_value(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        Err(fmt::Error)
    }
}

#[test]
fn disabled_error_target_skips_parameter_snapshots() {
    struct MustNotFormat;
    impl MysqlValue for MustNotFormat {
        fn write(self, writer: &mut crate::MysqlValueWriter) -> MysqlResult<()> {
            writer.push(1_i64)
        }
        fn fmt_log_value(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("disabled logging must not format parameters")
        }
    }
    tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), || {
        assert!(QueryLog::new("execute", "SELECT ?").is_none());
        let mut observation = crate::metrics::Observation::query(None, "execute", "SELECT ?");
        observation.capture_parameters(&(MustNotFormat,));
        observation.encode_arguments((MustNotFormat,)).unwrap();
    });
}

#[tokio::test]
async fn formatting_failure_does_not_change_the_query_result() {
    let capture = Capture::default();
    let dispatch = tracing::Dispatch::new(capture.clone());
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let mut tx = transaction();
    assert!(matches!(
        tx.execute("SELECT ? FROM {{missing}}", (BadFormat,)).await,
        Err(MysqlError::InvalidQuery { .. })
    ));
    let events = capture.0.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert!(events[0]["parameters"].contains("unable to format parameters"));
}

#[cfg(feature = "integration-tests")]
#[tokio::test]
async fn database_decode_and_timeout_errors_are_logged_but_success_and_missing_optional_rows_are_not()
 {
    let url =
        std::env::var("BREEZE_MYSQL_TEST_URL").expect("integration-tests requires a MySQL URL");
    let mysql = MysqlService::connect_with_options(
        &url,
        crate::MysqlServiceOptions::default().with_query_timeout(Duration::from_millis(200)),
    )
    .await
    .unwrap();
    let capture = Capture::default();
    let dispatch = tracing::Dispatch::new(capture.clone());
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let row: Option<i64> = mysql
        .fetch_optional("SELECT ? FROM DUAL WHERE FALSE", (7_i64,))
        .await
        .unwrap();
    assert_eq!(row, None);
    assert!(capture.0.lock().unwrap().is_empty());
    let result: MysqlResult<()> = mysql
        .with_transaction(async |tx| {
            tx.fetch_one::<_, _, i64>("SELECT ? FROM", (71_i64,))
                .await?;
            Ok(())
        })
        .await;
    assert_eq!(result.unwrap_err().database_code(), Some(1064));
    assert_eq!(
        capture.0.lock().unwrap().len(),
        1,
        "the outer transaction must not duplicate the error"
    );
    assert!(matches!(
        mysql.fetch_one::<_, _, i64>("SELECT 'text'", ()).await,
        Err(MysqlError::Decode { .. })
    ));
    assert_eq!(
        mysql
            .fetch_one::<_, _, i64>("SELECT ? FROM DUAL WHERE FALSE", (8_i64,))
            .await,
        Err(MysqlError::RowNotFound)
    );
    assert_eq!(
        mysql
            .fetch_all::<_, _, i64>("SELECT SLEEP(?)", (1_i64,))
            .await,
        Err(MysqlError::QueryTimedOut)
    );
    {
        let events = capture.0.lock().unwrap();
        assert_eq!(events.len(), 4);
        assert_eq!(events[0]["parameters"], "[71]");
        assert_eq!(events[0]["database_code"], "1064");
        assert_eq!(events[1]["parameters"], "[]");
        assert_eq!(events[2]["parameters"], "[8]");
        assert_eq!(events[3]["operation"], "fetch_all");
        assert_eq!(events[3]["parameters"], "[1]");
    }
    mysql.close().await;
}
