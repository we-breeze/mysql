//! Direct, typed decoding from SQLx MySQL rows.

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use serde::de::DeserializeOwned;
use sqlx::{
    Column, Decode, MySql, Row as _, Type, TypeInfo, ValueRef as _,
    mysql::MySqlRow as SqlxMySqlRow, types::BigDecimal,
};

use crate::{Json, MysqlError, MysqlResult};

/// One driver-owned MySQL row.
///
/// Values stay in the original result buffer and are decoded only when the
/// target type requests them. No intermediate map or value enum is built.
#[derive(Debug)]
pub struct MysqlRow {
    pub(crate) inner: SqlxMySqlRow,
}

impl MysqlRow {
    pub(crate) fn new(inner: SqlxMySqlRow) -> Self {
        Self { inner }
    }

    pub fn columns(&self) -> impl ExactSizeIterator<Item = &str> {
        self.inner.columns().iter().map(Column::name)
    }

    pub fn get<T: FromMysqlValue>(&self, column: &str) -> MysqlResult<Option<T>> {
        let raw = self
            .inner
            .try_get_raw(column)
            .map_err(|error| column_error(column, "supported MySQL value", error))?;
        if raw.is_null() {
            Ok(None)
        } else {
            T::from_mysql_value(self, column).map(Some)
        }
    }

    pub fn get_required<T: FromMysqlValue>(&self, column: &str) -> MysqlResult<T> {
        self.get(column)?.ok_or_else(|| MysqlError::UnexpectedNull {
            column: column.to_string(),
        })
    }

    pub(crate) fn type_name(&self, column: &str) -> MysqlResult<String> {
        self.inner
            .try_get_raw(column)
            .map(|raw| raw.type_info().name().to_ascii_uppercase())
            .map_err(|error| column_error(column, "supported MySQL value", error))
    }
}

/// Converts one non-null column directly from a driver-owned row.
pub trait FromMysqlValue: Sized {
    fn from_mysql_value(row: &MysqlRow, column: &str) -> MysqlResult<Self>;
}

/// Converts an owned driver row into an application value.
pub trait FromMysqlRow: Sized {
    fn from_mysql_row(row: MysqlRow) -> MysqlResult<Self>;
}

impl FromMysqlRow for MysqlRow {
    fn from_mysql_row(row: MysqlRow) -> MysqlResult<Self> {
        Ok(row)
    }
}

fn decode<'row, T>(row: &'row MysqlRow, column: &str, expected: &'static str) -> MysqlResult<T>
where
    T: Decode<'row, MySql> + Type<MySql>,
{
    row.inner
        .try_get(column)
        .map_err(|error| column_error(column, expected, error))
}

fn column_error(column: &str, expected: &'static str, error: sqlx::Error) -> MysqlError {
    match error {
        sqlx::Error::ColumnNotFound(_) => MysqlError::ColumnNotFound(column.to_string()),
        _ => MysqlError::Decode {
            column: column.to_string(),
            expected,
        },
    }
}

impl FromMysqlValue for String {
    fn from_mysql_value(row: &MysqlRow, column: &str) -> MysqlResult<Self> {
        match row.type_name(column)?.as_str() {
            "DECIMAL" | "NEWDECIMAL" => {
                decode::<BigDecimal>(row, column, "decimal").map(|value| value.to_string())
            }
            _ => decode(row, column, "string"),
        }
    }
}

impl FromMysqlValue for Vec<u8> {
    fn from_mysql_value(row: &MysqlRow, column: &str) -> MysqlResult<Self> {
        decode(row, column, "bytes")
    }
}

impl FromMysqlValue for bool {
    fn from_mysql_value(row: &MysqlRow, column: &str) -> MysqlResult<Self> {
        decode(row, column, "boolean")
    }
}

macro_rules! decode_value {
    ($expected:literal; $($type:ty),+ $(,)?) => {$(
        impl FromMysqlValue for $type {
            fn from_mysql_value(row: &MysqlRow, column: &str) -> MysqlResult<Self> {
                decode(row, column, $expected)
            }
        }
    )+};
}

decode_value!("signed integer"; i8, i16, i32, i64);
decode_value!("unsigned integer"; u8, u16, u32, u64);
decode_value!("floating-point number"; f32);
decode_value!("date"; NaiveDate);
decode_value!("datetime"; NaiveDateTime);
decode_value!("time"; NaiveTime);
decode_value!("decimal"; BigDecimal);

impl FromMysqlValue for isize {
    fn from_mysql_value(row: &MysqlRow, column: &str) -> MysqlResult<Self> {
        let value = i64::from_mysql_value(row, column)?;
        Self::try_from(value).map_err(|_| MysqlError::Decode {
            column: column.to_string(),
            expected: "isize",
        })
    }
}

impl FromMysqlValue for usize {
    fn from_mysql_value(row: &MysqlRow, column: &str) -> MysqlResult<Self> {
        let value = u64::from_mysql_value(row, column)?;
        Self::try_from(value).map_err(|_| MysqlError::Decode {
            column: column.to_string(),
            expected: "usize",
        })
    }
}

impl FromMysqlValue for f64 {
    fn from_mysql_value(row: &MysqlRow, column: &str) -> MysqlResult<Self> {
        if row.type_name(column)? == "FLOAT" {
            f32::from_mysql_value(row, column).map(f64::from)
        } else {
            decode(row, column, "floating-point number")
        }
    }
}

impl<T> FromMysqlValue for Json<T>
where
    T: DeserializeOwned,
{
    fn from_mysql_value(row: &MysqlRow, column: &str) -> MysqlResult<Self> {
        decode::<sqlx::types::Json<T>>(row, column, "JSON").map(|value| Json(value.0))
    }
}

impl FromMysqlValue for serde_json::Value {
    fn from_mysql_value(row: &MysqlRow, column: &str) -> MysqlResult<Self> {
        Json::<Self>::from_mysql_value(row, column).map(Json::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accepts_application_row<T: FromMysqlRow>() {}
    fn accepts_column<T: FromMysqlValue>() {}

    #[test]
    fn public_conversion_contracts_accept_json_and_rows() {
        accepts_application_row::<MysqlRow>();
        accepts_column::<Json<serde_json::Value>>();
    }
}
