#![cfg(feature = "integration-tests")]

use brz_mysql::{
    BinaryColumn, FromMysqlCol, FromMysqlRow, FromMysqlValue, Json, Mysql, MysqlError, MysqlResult,
    MysqlRoute, MysqlRow, MysqlService, MysqlTransaction,
};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use futures_util::{StreamExt, pin_mut};
use serde::Deserialize;

async fn service() -> Option<MysqlService> {
    let url = std::env::var("BREEZE_MYSQL_TEST_URL").ok()?;
    Some(MysqlService::connect(&url).await.unwrap())
}

async fn scalar<T: FromMysqlCol + Send + std::fmt::Debug + PartialEq>(
    mysql: &impl Mysql,
    sql: &str,
    expected: T,
) {
    let actual: Option<T> = mysql.fetch_optional(sql, ()).await.unwrap();
    assert_eq!(actual, Some(expected), "{sql}");
}

#[tokio::test]
async fn common_columns_are_direct_scalar_results() {
    let Some(mysql) = service().await else { return };
    macro_rules! number {
        ($($type:ty => $sql:expr => $value:expr),+ $(,)?) => {$(
            scalar::<$type>(&mysql, $sql, $value).await;
        )+};
    }
    number!(
        i8 => "SELECT CAST(-12 AS SIGNED)" => -12,
        i16 => "SELECT CAST(-1234 AS SIGNED)" => -1234,
        i32 => "SELECT CAST(-123456 AS SIGNED)" => -123456,
        i64 => "SELECT CAST(-9223372036854775808 AS SIGNED)" => i64::MIN,
        isize => "SELECT CAST(-42 AS SIGNED)" => -42,
        u8 => "SELECT CAST(255 AS UNSIGNED)" => u8::MAX,
        u16 => "SELECT CAST(65535 AS UNSIGNED)" => u16::MAX,
        u32 => "SELECT CAST(4294967295 AS UNSIGNED)" => u32::MAX,
        u64 => "SELECT CAST(18446744073709551615 AS UNSIGNED)" => u64::MAX,
        usize => "SELECT CAST(42 AS UNSIGNED)" => 42,
    );
    scalar(&mysql, "SELECT 'AbC 中文'", "AbC 中文".to_string()).await;
    scalar(&mysql, "SELECT X'00ff'", vec![0_u8, 255]).await;
    scalar(
        &mysql,
        "SELECT CAST('2026-09-06' AS DATE)",
        NaiveDate::from_ymd_opt(2026, 9, 6).unwrap(),
    )
    .await;
    scalar(
        &mysql,
        "SELECT CAST('2026-09-06 12:34:56.123456' AS DATETIME(6))",
        NaiveDate::from_ymd_opt(2026, 9, 6)
            .unwrap()
            .and_hms_micro_opt(12, 34, 56, 123456)
            .unwrap(),
    )
    .await;
    scalar(
        &mysql,
        "SELECT CAST('12:34:56.123456' AS TIME(6))",
        NaiveTime::from_hms_micro_opt(12, 34, 56, 123456).unwrap(),
    )
    .await;
    scalar(
        &mysql,
        "SELECT CAST('12.3400' AS DECIMAL(10,4))",
        "12.3400".to_string(),
    )
    .await;
    scalar(
        &mysql,
        "SELECT CAST('12.3400' AS DECIMAL(10,4))",
        "12.3400".parse::<sqlx::types::BigDecimal>().unwrap(),
    )
    .await;
    mysql.execute("CREATE TABLE IF NOT EXISTS brz_mysql_scalar_types (id INT PRIMARY KEY, flag BOOLEAN, single_value FLOAT, double_value DOUBLE)", ()).await.unwrap();
    mysql
        .execute(
            "REPLACE INTO brz_mysql_scalar_types VALUES (1, TRUE, 1.25, 2.5)",
            (),
        )
        .await
        .unwrap();
    scalar(
        &mysql,
        "SELECT flag FROM brz_mysql_scalar_types WHERE id=1",
        true,
    )
    .await;
    scalar(
        &mysql,
        "SELECT single_value FROM brz_mysql_scalar_types WHERE id=1",
        1.25_f32,
    )
    .await;
    scalar(
        &mysql,
        "SELECT single_value FROM brz_mysql_scalar_types WHERE id=1",
        1.25_f64,
    )
    .await;
    scalar(
        &mysql,
        "SELECT double_value FROM brz_mysql_scalar_types WHERE id=1",
        2.5_f64,
    )
    .await;
    mysql.close().await;
}

#[tokio::test]
async fn tuples_decode_positions_even_when_column_names_repeat() {
    let Some(mysql) = service().await else { return };
    let row: Option<(i64, String, Option<NaiveDateTime>)> = mysql
        .fetch_optional(
            "SELECT CAST(1 AS SIGNED) AS same, 'two' AS same, CAST(NULL AS DATETIME) AS same",
            (),
        )
        .await
        .unwrap();
    assert_eq!(row, Some((1, "two".into(), None)));
    let single: (i64,) = mysql.fetch_one("SELECT 7", ()).await.unwrap();
    assert_eq!(single, (7,));
    type SixteenColumns = (
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
    );
    let (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p): SixteenColumns = mysql
        .fetch_one("SELECT 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16", ())
        .await
        .unwrap();
    assert_eq!(
        [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p],
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    );
    let raw: MysqlRow = mysql
        .fetch_one("SELECT 11 AS same, 22 AS same", ())
        .await
        .unwrap();
    assert_eq!(raw.get_at::<i64>(0).unwrap(), 11);
    assert_eq!(raw.get_at::<i64>(1).unwrap(), 22);
    assert_eq!(
        raw.get_at::<i64>(2).unwrap_err(),
        MysqlError::ColumnIndexOutOfBounds { index: 2, len: 2 }
    );
    mysql.close().await;
}

#[tokio::test]
async fn null_absence_column_count_and_decode_errors_remain_distinct() {
    let Some(mysql) = service().await else { return };
    let absent: Option<i64> = mysql
        .fetch_optional("SELECT 1 FROM DUAL WHERE FALSE", ())
        .await
        .unwrap();
    assert_eq!(absent, None);
    let null: Option<Option<i64>> = mysql.fetch_optional("SELECT NULL", ()).await.unwrap();
    assert_eq!(null, Some(None));
    let value: Option<Option<i64>> = mysql.fetch_optional("SELECT 42", ()).await.unwrap();
    assert_eq!(value, Some(Some(42)));
    assert_eq!(
        mysql
            .fetch_one::<_, _, i64>("SELECT 1 FROM DUAL WHERE FALSE", ())
            .await
            .unwrap_err(),
        MysqlError::RowNotFound
    );
    assert_eq!(
        mysql
            .fetch_optional::<_, _, i64>("SELECT NULL AS owner", ())
            .await
            .unwrap_err(),
        MysqlError::UnexpectedNull {
            column: "owner".into()
        }
    );
    assert_eq!(
        mysql
            .fetch_one::<_, _, i64>("SELECT 1, 2", ())
            .await
            .unwrap_err(),
        MysqlError::ColumnCount {
            expected: 1,
            actual: 2
        }
    );
    for (sql, actual) in [("SELECT 1", 1), ("SELECT 1,2,3", 3)] {
        assert_eq!(
            mysql
                .fetch_one::<_, _, (i64, i64)>(sql, ())
                .await
                .unwrap_err(),
            MysqlError::ColumnCount {
                expected: 2,
                actual
            }
        );
    }
    assert!(matches!(
        mysql
            .fetch_one::<_, _, i64>("SELECT 'not an integer'", ())
            .await
            .unwrap_err(),
        MysqlError::Decode { .. }
    ));
    assert!(matches!(
        mysql
            .fetch_one::<_, _, u8>("SELECT CAST(256 AS UNSIGNED)", ())
            .await
            .unwrap_err(),
        MysqlError::Decode { .. }
    ));
    mysql.close().await;
}

#[derive(Debug, PartialEq, Deserialize, FromMysqlCol)]
struct Settings {
    enabled: bool,
    labels: Vec<String>,
}

#[derive(Debug, PartialEq, Deserialize, FromMysqlCol)]
struct Envelope<T> {
    value: T,
}

#[derive(Debug, PartialEq, Deserialize, FromMysqlCol)]
enum Mode {
    Active,
    Paused,
}

#[derive(Debug, PartialEq, FromMysqlRow)]
struct User {
    id: i64,
    #[mysql(rename = "preferences")]
    settings: Settings,
}

#[tokio::test]
async fn json_deserializes_directly_into_scalars_tuple_elements_and_business_fields() {
    let Some(mysql) = service().await else { return };
    let expected = || Settings {
        enabled: true,
        labels: vec!["中文".into()],
    };
    let json = r#"{"enabled":true,"labels":["中文"]}"#;
    let settings: Option<Settings> = mysql
        .fetch_optional("SELECT CAST(? AS JSON)", (json,))
        .await
        .unwrap();
    assert_eq!(settings, Some(expected()));
    let tuple: (i64, Settings) = mysql
        .fetch_one("SELECT 1 AS same, CAST(? AS JSON) AS same", (json,))
        .await
        .unwrap();
    assert_eq!(tuple, (1, expected()));
    let user: Option<User> = mysql
        .fetch_optional("SELECT CAST(? AS JSON) AS preferences, 1 AS id", (json,))
        .await
        .unwrap();
    assert_eq!(
        user,
        Some(User {
            id: 1,
            settings: expected()
        })
    );
    let wrapped: Json<Settings> = mysql
        .fetch_one("SELECT CAST(? AS JSON)", (json,))
        .await
        .unwrap();
    assert_eq!(wrapped.into_inner(), expected());
    let generic: Envelope<i64> = mysql
        .fetch_one("SELECT CAST('{\"value\":42}' AS JSON)", ())
        .await
        .unwrap();
    assert_eq!(generic, Envelope { value: 42 });
    let mode: Mode = mysql
        .fetch_one("SELECT CAST('\"Active\"' AS JSON)", ())
        .await
        .unwrap();
    assert_eq!(mode, Mode::Active);
    let json_null: Option<serde_json::Value> = mysql
        .fetch_one("SELECT CAST('null' AS JSON)", ())
        .await
        .unwrap();
    assert_eq!(json_null, Some(serde_json::Value::Null));
    let sql_null: Option<Option<Settings>> = mysql.fetch_optional("SELECT NULL", ()).await.unwrap();
    assert_eq!(sql_null, Some(None));
    assert!(matches!(
        mysql
            .fetch_optional::<_, _, Settings>("SELECT CAST('{}' AS JSON)", ())
            .await
            .unwrap_err(),
        MysqlError::Decode {
            expected: "JSON",
            ..
        }
    ));
    mysql.close().await;
}

#[derive(Debug, PartialEq)]
struct OwnerId(i64);

impl FromMysqlCol for OwnerId {
    fn from_mysql_col(row: &MysqlRow, index: usize) -> MysqlResult<Self> {
        row.get_at(index).map(Self)
    }
}

#[derive(Debug, PartialEq)]
struct LegacyId(i64);

impl FromMysqlValue for LegacyId {
    fn from_mysql_value(row: &MysqlRow, column: &str) -> MysqlResult<Self> {
        row.get_required(column).map(Self)
    }
}

#[derive(Debug, PartialEq, FromMysqlRow)]
struct Identity {
    owner: OwnerId,
    legacy: LegacyId,
}

#[tokio::test]
async fn custom_column_and_existing_named_decoders_work_together() {
    let Some(mysql) = service().await else { return };
    scalar(&mysql, "SELECT 7", OwnerId(7)).await;
    let identity: Identity = mysql
        .fetch_one("SELECT 1 AS owner, 2 AS legacy", ())
        .await
        .unwrap();
    assert_eq!(
        identity,
        Identity {
            owner: OwnerId(1),
            legacy: LegacyId(2)
        }
    );
    let raw: MysqlRow = mysql.fetch_one("SELECT 3 AS owner", ()).await.unwrap();
    assert_eq!(raw.get_required::<OwnerId>("owner").unwrap(), OwnerId(3));
    assert_eq!(
        raw.get_required::<i64>("missing").unwrap_err(),
        MysqlError::ColumnNotFound("missing".into())
    );
    mysql.close().await;
}

#[tokio::test]
async fn scalar_and_tuple_results_work_through_generic_routing_transactions_and_streams() {
    let Some(mysql) = service().await else { return };
    mysql.execute("CREATE TABLE IF NOT EXISTS brz_mysql_column_route (id BIGINT PRIMARY KEY, value VARCHAR(64))", ()).await.unwrap();
    mysql
        .execute(
            "REPLACE INTO brz_mysql_column_route VALUES (1, 'one'), (2, 'two')",
            (),
        )
        .await
        .unwrap();
    let sharded = mysql.with_route(|_: brz_mysql::MysqlRouteValue<'_>| {
        MysqlRoute::new().with_table("records", "brz_mysql_column_route")
    });
    async fn read<M: Mysql>(mysql: &M) -> MysqlResult<Vec<(i64, String)>> {
        let id: Option<i64> = mysql
            .fetch_optional("SELECT id FROM {{records}} WHERE id=?", (1_i64,))
            .await?;
        assert_eq!(id, Some(1));
        let rows: Vec<(i64, String)> = mysql
            .with_transaction(async |tx| {
                tx.fetch_all(
                    "SELECT id,value FROM {{records}} WHERE id>=? ORDER BY id",
                    (1_i64,),
                )
                .await
            })
            .await?;
        let stream = mysql.fetch::<_, _, i64>(
            "SELECT id FROM {{records}} WHERE id>=? ORDER BY id",
            (1_i64,),
        );
        pin_mut!(stream);
        assert_eq!(stream.next().await.transpose()?, Some(1));
        assert_eq!(stream.next().await.transpose()?, Some(2));
        assert!(stream.next().await.is_none());
        Ok(rows)
    }
    assert_eq!(
        read(&sharded).await.unwrap(),
        vec![(1, "one".into()), (2, "two".into())]
    );
    mysql.close().await;
}

#[tokio::test]
async fn binary_columns_work_in_scalar_tuple_struct_and_stream_results() {
    let Some(mysql) = service().await else { return };
    let scalar: Option<BinaryColumn> = Mysql::fetch_optional(&mysql, "SELECT X'00FF41'", ())
        .await
        .unwrap();
    assert_eq!(scalar.unwrap().as_bytes(), &[0, 255, b'A']);
    let tuple: (i64, BinaryColumn) = mysql.fetch_one("SELECT 42, X'00FF'", ()).await.unwrap();
    assert_eq!(tuple.0, 42);
    assert_eq!(tuple.1.as_bytes(), &[0, 255]);
    let single: (BinaryColumn,) = mysql.fetch_one("SELECT X'41'", ()).await.unwrap();
    assert_eq!(single.0.as_bytes(), b"A");

    #[derive(FromMysqlRow)]
    struct File {
        #[mysql(rename = "payload")]
        body: BinaryColumn,
    }
    let file: File = mysql
        .fetch_one("SELECT X'0102' AS payload", ())
        .await
        .unwrap();
    assert_eq!(file.body.as_bytes(), &[1, 2]);

    let all: Vec<BinaryColumn> = mysql
        .fetch_all("SELECT X'01' UNION ALL SELECT X'02'", ())
        .await
        .unwrap();
    assert_eq!(
        all.iter().map(BinaryColumn::as_bytes).collect::<Vec<_>>(),
        [&[1][..], &[2][..]]
    );
    let stream =
        Mysql::fetch::<_, _, BinaryColumn>(&mysql, "SELECT X'03' UNION ALL SELECT X'04'", ());
    pin_mut!(stream);
    let first = stream.next().await.unwrap().unwrap();
    let second = stream.next().await.unwrap().unwrap();
    assert!(stream.next().await.is_none());
    assert_eq!((first.as_bytes(), second.as_bytes()), (&[3][..], &[4][..]));
    let from_transaction: BinaryColumn = mysql
        .with_transaction(async |transaction| transaction.fetch_one("SELECT X'05'", ()).await)
        .await
        .unwrap();
    mysql.close().await;
    assert_eq!(from_transaction.into_bytes().as_ref(), &[5]);
    assert_eq!(file.body.into_bytes().as_ref(), &[1, 2]);
}

#[tokio::test]
async fn binary_columns_distinguish_empty_null_missing_and_invalid_results() {
    let Some(mysql) = service().await else { return };
    let empty: BinaryColumn = mysql.fetch_one("SELECT X''", ()).await.unwrap();
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
    assert!(empty.into_bytes().is_empty());
    let missing: Option<BinaryColumn> = mysql
        .fetch_optional("SELECT X'00' FROM DUAL WHERE FALSE", ())
        .await
        .unwrap();
    assert!(missing.is_none());
    let null: Option<Option<BinaryColumn>> = mysql.fetch_optional("SELECT NULL", ()).await.unwrap();
    assert_eq!(null, Some(None));
    assert_eq!(
        mysql
            .fetch_one::<_, _, BinaryColumn>("SELECT NULL AS payload", ())
            .await
            .unwrap_err(),
        MysqlError::UnexpectedNull {
            column: "payload".into()
        }
    );
    assert!(matches!(
        mysql
            .fetch_one::<_, _, BinaryColumn>("SELECT 123", ())
            .await
            .unwrap_err(),
        MysqlError::Decode { .. }
    ));
    assert_eq!(
        mysql
            .fetch_one::<_, _, BinaryColumn>("SELECT X'01', X'02'", ())
            .await
            .unwrap_err(),
        MysqlError::ColumnCount {
            expected: 1,
            actual: 2
        }
    );
    let raw: MysqlRow = mysql
        .fetch_one("SELECT X'00FF' AS payload", ())
        .await
        .unwrap();
    assert_eq!(
        raw.get_required::<BinaryColumn>("payload")
            .unwrap()
            .as_bytes(),
        &[0, 255]
    );
    assert_eq!(
        raw.get_at::<BinaryColumn>(1).unwrap_err(),
        MysqlError::ColumnIndexOutOfBounds { index: 1, len: 1 }
    );
    mysql.close().await;
}
