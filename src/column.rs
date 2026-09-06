//! Typed decoding of one column, independent of its name and result-row shape.

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use serde::de::DeserializeOwned;
use sqlx::{Decode, MySql, Row as _, Type, TypeInfo, ValueRef as _, types::BigDecimal};

use crate::{BinaryColumn, Json, MysqlError, MysqlResult, MysqlRow};

/// Decode one column by its zero-based position directly from the driver buffer.
///
/// Built-in implementations cover numbers, strings, bytes, dates/times, JSON,
/// and `Option<T>` for SQL NULL. Implement this trait for a custom column type
/// using `row.get_at::<ExistingType>(index)`. It also enables single-column row
/// results, tuple elements, and fields in `#[derive(FromMysqlRow)]` structs.
/// Derive `FromMysqlCol` on a `Deserialize` type to decode it from a JSON column.
pub trait FromMysqlCol: Sized {
    fn from_mysql_col(row: &MysqlRow, index: usize) -> MysqlResult<Self>;
}

impl<T: FromMysqlCol> FromMysqlCol for Option<T> {
    fn from_mysql_col(row: &MysqlRow, index: usize) -> MysqlResult<Self> {
        if row.is_null(index)? {
            Ok(None)
        } else {
            T::from_mysql_col(row, index).map(Some)
        }
    }
}

fn decode<'row, T>(row: &'row MysqlRow, index: usize, expected: &'static str) -> MysqlResult<T>
where
    T: Decode<'row, MySql> + Type<MySql>,
{
    let column = row.column_name(index)?;
    if row.is_null(index)? {
        return Err(MysqlError::UnexpectedNull {
            column: column.to_string(),
        });
    }
    row.inner.try_get(index).map_err(|_| MysqlError::Decode {
        column: column.to_string(),
        expected,
    })
}

fn type_name(row: &MysqlRow, index: usize) -> MysqlResult<String> {
    let column = row.column_name(index)?;
    row.inner
        .try_get_raw(index)
        .map(|value| value.type_info().name().to_ascii_uppercase())
        .map_err(|_| MysqlError::Decode {
            column: column.to_string(),
            expected: "supported MySQL value",
        })
}

macro_rules! decode_column {
    ($expected:literal; $($type:ty),+ $(,)?) => {$(
        impl FromMysqlCol for $type {
            fn from_mysql_col(row: &MysqlRow, index: usize) -> MysqlResult<Self> {
                decode(row, index, $expected)
            }
        }
    )+};
}

decode_column!("signed integer"; i8, i16, i32, i64);
decode_column!("unsigned integer"; u8, u16, u32, u64);
decode_column!("floating-point number"; f32);
decode_column!("boolean"; bool);
decode_column!("bytes"; Vec<u8>);
decode_column!("date"; NaiveDate);
decode_column!("datetime"; NaiveDateTime);
decode_column!("time"; NaiveTime);
decode_column!("decimal"; BigDecimal);

impl FromMysqlCol for BinaryColumn {
    fn from_mysql_col(row: &MysqlRow, index: usize) -> MysqlResult<Self> {
        // Validate the type and NULL handling exactly as for Vec<u8>, but
        // borrow its bytes instead of making an owned payload copy.
        decode::<&[u8]>(row, index, "bytes")?;
        let value = row
            .inner
            .try_get_raw(index)
            .expect("validated column index");
        // A value taken directly from MySqlRow shares the row's Bytes storage.
        Ok(BinaryColumn::from_validated_value(
            sqlx::ValueRef::to_owned(&value),
        ))
    }
}

impl FromMysqlCol for String {
    fn from_mysql_col(row: &MysqlRow, index: usize) -> MysqlResult<Self> {
        match type_name(row, index)?.as_str() {
            "DECIMAL" | "NEWDECIMAL" => {
                decode::<BigDecimal>(row, index, "decimal").map(|value| value.to_string())
            }
            _ => decode(row, index, "string"),
        }
    }
}

impl FromMysqlCol for f64 {
    fn from_mysql_col(row: &MysqlRow, index: usize) -> MysqlResult<Self> {
        if type_name(row, index)? == "FLOAT" {
            f32::from_mysql_col(row, index).map(f64::from)
        } else {
            decode(row, index, "floating-point number")
        }
    }
}

impl FromMysqlCol for isize {
    fn from_mysql_col(row: &MysqlRow, index: usize) -> MysqlResult<Self> {
        let value = i64::from_mysql_col(row, index)?;
        let column = row.column_name(index)?;
        Self::try_from(value).map_err(|_| MysqlError::Decode {
            column: column.to_string(),
            expected: "isize",
        })
    }
}

impl FromMysqlCol for usize {
    fn from_mysql_col(row: &MysqlRow, index: usize) -> MysqlResult<Self> {
        let value = u64::from_mysql_col(row, index)?;
        let column = row.column_name(index)?;
        Self::try_from(value).map_err(|_| MysqlError::Decode {
            column: column.to_string(),
            expected: "usize",
        })
    }
}

impl<T: DeserializeOwned> FromMysqlCol for Json<T> {
    fn from_mysql_col(row: &MysqlRow, index: usize) -> MysqlResult<Self> {
        decode::<sqlx::types::Json<T>>(row, index, "JSON").map(|value| Json(value.0))
    }
}

impl FromMysqlCol for serde_json::Value {
    fn from_mysql_col(row: &MysqlRow, index: usize) -> MysqlResult<Self> {
        Json::<Self>::from_mysql_col(row, index).map(Json::into_inner)
    }
}
