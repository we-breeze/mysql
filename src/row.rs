//! Driver-owned rows and typed application result shapes.

use sqlx::{Column, Row as _, ValueRef as _, mysql::MySqlRow as SqlxMySqlRow};

use crate::{FromMysqlCol, MysqlError, MysqlResult};

/// One driver-owned MySQL row. Values are decoded directly from its buffer.
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

    /// Decode a column by its zero-based position, including `Option<T>` for NULL.
    pub fn get_at<T: FromMysqlCol>(&self, index: usize) -> MysqlResult<T> {
        self.column_name(index)?;
        T::from_mysql_col(self, index)
    }

    pub fn get<T: FromMysqlValue>(&self, column: &str) -> MysqlResult<Option<T>> {
        let index = self.column_index(column)?;
        if self.is_null(index)? {
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

    pub(crate) fn column_name(&self, index: usize) -> MysqlResult<&str> {
        self.inner.columns().get(index).map(Column::name).ok_or(
            MysqlError::ColumnIndexOutOfBounds {
                index,
                len: self.inner.columns().len(),
            },
        )
    }

    fn column_index(&self, column: &str) -> MysqlResult<usize> {
        self.inner
            .try_column(column)
            .map(Column::ordinal)
            .map_err(|_| MysqlError::ColumnNotFound(column.to_string()))
    }

    pub(crate) fn is_null(&self, index: usize) -> MysqlResult<bool> {
        self.column_name(index)?;
        self.inner
            .try_get_raw(index)
            .map(|value| value.is_null())
            .map_err(|_| MysqlError::ColumnIndexOutOfBounds {
                index,
                len: self.inner.columns().len(),
            })
    }

    fn expect_columns(&self, expected: usize) -> MysqlResult<()> {
        let actual = self.inner.columns().len();
        if actual == expected {
            Ok(())
        } else {
            Err(MysqlError::ColumnCount { expected, actual })
        }
    }
}

/// Named non-null decoding used by `MysqlRow::get` and struct derives.
/// Existing custom implementations remain supported. New column types can
/// implement `FromMysqlCol` to gain both named and positional decoding.
pub trait FromMysqlValue: Sized {
    fn from_mysql_value(row: &MysqlRow, column: &str) -> MysqlResult<Self>;
}

impl<T: FromMysqlCol> FromMysqlValue for T {
    fn from_mysql_value(row: &MysqlRow, column: &str) -> MysqlResult<Self> {
        row.get_at(row.column_index(column)?)
    }
}

/// Converts a row into a scalar, positional tuple, or named application struct.
/// Scalars require one column; tuples require exactly their number of elements.
/// Use `#[derive(FromMysqlRow)]` to decode a business struct by field name.
pub trait FromMysqlRow: Sized {
    fn from_mysql_row(row: MysqlRow) -> MysqlResult<Self>;
}

impl FromMysqlRow for MysqlRow {
    fn from_mysql_row(row: MysqlRow) -> MysqlResult<Self> {
        Ok(row)
    }
}

impl<T: FromMysqlCol> FromMysqlRow for T {
    fn from_mysql_row(row: MysqlRow) -> MysqlResult<Self> {
        row.expect_columns(1)?;
        row.get_at(0)
    }
}

macro_rules! tuple_row {
    ($count:literal; $($type:ident:$index:tt),+ $(,)?) => {
        impl<$($type: FromMysqlCol),+> FromMysqlRow for ($($type,)+) {
            fn from_mysql_row(row: MysqlRow) -> MysqlResult<Self> {
                row.expect_columns($count)?;
                Ok(($(row.get_at::<$type>($index)?,)+))
            }
        }
    };
}

tuple_row!(1; A:0);
tuple_row!(2; A:0, B:1);
tuple_row!(3; A:0, B:1, C:2);
tuple_row!(4; A:0, B:1, C:2, D:3);
tuple_row!(5; A:0, B:1, C:2, D:3, E:4);
tuple_row!(6; A:0, B:1, C:2, D:3, E:4, F:5);
tuple_row!(7; A:0, B:1, C:2, D:3, E:4, F:5, G:6);
tuple_row!(8; A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7);
tuple_row!(9; A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8);
tuple_row!(10; A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9);
tuple_row!(11; A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10);
tuple_row!(12; A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10, L:11);
tuple_row!(13; A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10, L:11, M:12);
tuple_row!(14; A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10, L:11, M:12, N:13);
tuple_row!(15; A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10, L:11, M:12, N:13, O:14);
tuple_row!(16; A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10, L:11, M:12, N:13, O:14, P:15);
