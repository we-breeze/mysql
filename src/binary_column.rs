use std::fmt;

use bytes::Bytes;
use sqlx::{Decode, MySql, Value, mysql::MySqlValue};

/// An owned binary column sharing the driver's result buffer.
///
/// Decoding and cloning do not copy the payload. This value can outlive both
/// its result row and database connection; it does not keep a connection
/// checked out. The shared allocation may also contain other columns from
/// the same row, and stays alive until its last owner is dropped.
///
/// SQLx still receives the complete row. A small owner allocation is used to
/// expose the binary value as [`Bytes`], without allocating a payload Vec.
/// Use `Option<BinaryColumn>` for SQL NULL.
#[derive(Clone, PartialEq, Eq)]
pub struct BinaryColumn(Bytes);

impl BinaryColumn {
    pub(crate) fn from_validated_value(value: MySqlValue) -> Self {
        Self(Bytes::from_owner(BinaryOwner(value)))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Transfer ownership to a response body without copying the payload.
    pub fn into_bytes(self) -> Bytes {
        self.0
    }
}

impl AsRef<[u8]> for BinaryColumn {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl fmt::Debug for BinaryColumn {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BinaryColumn")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

struct BinaryOwner(MySqlValue);

impl AsRef<[u8]> for BinaryOwner {
    fn as_ref(&self) -> &[u8] {
        // Construction only accepts a column already decoded as non-null
        // &[u8]. MySqlValue preserves those immutable bytes and their format.
        <&[u8] as Decode<MySql>>::decode(Value::as_ref(&self.0))
            .expect("validated non-null binary column")
    }
}

#[cfg(all(test, feature = "integration-tests"))]
mod tests {
    use sqlx::Row;

    use super::BinaryColumn;
    use crate::{FromMysqlCol, MysqlRow};

    #[tokio::test]
    async fn large_blob_shares_driver_storage_after_row_and_connection_drop() {
        let Ok(url) = std::env::var("BREEZE_MYSQL_TEST_URL") else {
            return;
        };
        let pool = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        let row = MysqlRow::new(
            sqlx::query("SELECT REPEAT(X'00FF41', 349526) AS payload")
                .fetch_one(&pool)
                .await
                .unwrap(),
        );
        let original: &[u8] = row.inner.try_get(0).unwrap();
        let pointer = original.as_ptr();
        let column = BinaryColumn::from_mysql_col(&row, 0).unwrap();
        assert_eq!(column.as_bytes().as_ptr(), pointer);
        assert_eq!(column.len(), 349526 * 3);
        let cloned = column.clone();
        assert_eq!(cloned.as_bytes().as_ptr(), pointer);
        drop(row);
        drop(column);
        // A one-connection pool remains usable while the column is alive.
        sqlx::query("SELECT 1").execute(&pool).await.unwrap();
        pool.close().await;
        let bytes = cloned.into_bytes();
        assert_eq!(bytes.as_ptr(), pointer);
        assert!(
            bytes
                .as_chunks::<3>()
                .0
                .iter()
                .all(|chunk| *chunk == [0, 255, b'A'])
        );
    }
}
