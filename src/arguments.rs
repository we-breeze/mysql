//! Allocation-conscious MySQL argument encoding.

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use serde::Serialize;
use sqlx::{
    Arguments as _, Encode, MySql, Type, mysql::MySqlArguments as SqlxMySqlArguments,
    types::BigDecimal,
};

use crate::{MysqlError, MysqlResult};

/// A borrowed, allocation-free view used only by a configured table selector.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MysqlSelectorValue<'a> {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(&'a str),
    Bytes(&'a [u8]),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Time(NaiveTime),
    Unsupported(&'static str),
}

impl<'value> MysqlSelectorValue<'value> {
    pub fn as_u64(self) -> MysqlResult<u64> {
        match self {
            Self::U64(value) => Ok(value),
            Self::I64(value) => u64::try_from(value).map_err(|_| selector_type_error("u64")),
            _ => Err(selector_type_error("u64")),
        }
    }

    pub fn as_i64(self) -> MysqlResult<i64> {
        match self {
            Self::I64(value) => Ok(value),
            Self::U64(value) => i64::try_from(value).map_err(|_| selector_type_error("i64")),
            _ => Err(selector_type_error("i64")),
        }
    }

    pub fn as_str(self) -> MysqlResult<&'value str> {
        match self {
            Self::String(value) => Ok(value),
            _ => Err(selector_type_error("string")),
        }
    }

    pub fn as_bytes(self) -> MysqlResult<&'value [u8]> {
        match self {
            Self::Bytes(value) => Ok(value),
            _ => Err(selector_type_error("bytes")),
        }
    }
}

fn selector_type_error(expected: &'static str) -> MysqlError {
    MysqlError::InvalidQuery {
        reason: format!("the first argument cannot be used as a {expected} table selector"),
    }
}

/// A single value that can be encoded directly into MySQL arguments.
///
/// Implementations should forward newtypes to MysqlValueWriter::push.
/// Encoding writes directly into SQLx's final MySQL argument buffer.
pub trait MysqlValue: Send {
    #[doc(hidden)]
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()>;

    #[doc(hidden)]
    fn selector_value(&self) -> MysqlSelectorValue<'_>;

    #[doc(hidden)]
    fn encoded_size_hint(&self) -> usize {
        0
    }
}

/// One complete heterogeneous MySQL parameter list.
///
/// Tuples are encoded field by field without constructing an intermediate
/// vector. Homogeneous arrays and vectors are also supported.
pub trait MysqlArgs: Send {
    #[doc(hidden)]
    fn len(&self) -> usize;

    #[doc(hidden)]
    fn encoded_size_hint(&self) -> usize;

    #[doc(hidden)]
    fn first_selector_value(&self) -> Option<MysqlSelectorValue<'_>>;

    #[doc(hidden)]
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()>;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Internal encoder exposed only so application newtypes can implement
/// MysqlValue by forwarding to an existing implementation.
#[doc(hidden)]
pub struct MysqlValueWriter {
    inner: SqlxMySqlArguments,
    position: usize,
}

impl MysqlValueWriter {
    pub(crate) fn with_capacity(arguments: usize, bytes: usize) -> Self {
        let mut inner = SqlxMySqlArguments::default();
        inner.reserve(arguments, bytes);
        Self { inner, position: 0 }
    }

    pub fn push<V: MysqlValue>(&mut self, value: V) -> MysqlResult<()> {
        value.write(self)
    }

    pub(crate) fn into_inner(self) -> SqlxMySqlArguments {
        self.inner
    }

    fn push_sqlx<'query, T>(&mut self, value: T) -> MysqlResult<()>
    where
        T: Encode<'query, MySql> + Type<MySql> + 'query,
    {
        let position = self.position;
        self.position += 1;
        self.inner
            .add(value)
            .map_err(|error| MysqlError::EncodeArgument {
                position,
                message: error.to_string(),
            })
    }
}

/// JSON input/output wrapper.
///
/// On input, SQLx serializes the wrapped value directly into its MySQL
/// argument buffer without first building a serde_json::Value.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Json<T>(pub T);

impl<T> Json<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

macro_rules! signed_value {
    ($($type:ty),+ $(,)?) => {$(
        impl MysqlValue for $type {
            fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
                writer.push_sqlx(self)
            }

            fn selector_value(&self) -> MysqlSelectorValue<'_> {
                MysqlSelectorValue::I64(*self as i64)
            }

            fn encoded_size_hint(&self) -> usize {
                std::mem::size_of::<$type>()
            }
        }
    )+};
}

macro_rules! unsigned_value {
    ($($type:ty),+ $(,)?) => {$(
        impl MysqlValue for $type {
            fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
                writer.push_sqlx(self)
            }

            fn selector_value(&self) -> MysqlSelectorValue<'_> {
                MysqlSelectorValue::U64(*self as u64)
            }

            fn encoded_size_hint(&self) -> usize {
                std::mem::size_of::<$type>()
            }
        }
    )+};
}

signed_value!(i8, i16, i32, i64);
unsigned_value!(u8, u16, u32, u64);

impl MysqlValue for isize {
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.push_sqlx(self as i64)
    }

    fn selector_value(&self) -> MysqlSelectorValue<'_> {
        MysqlSelectorValue::I64(*self as i64)
    }

    fn encoded_size_hint(&self) -> usize {
        std::mem::size_of::<i64>()
    }
}

impl MysqlValue for usize {
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.push_sqlx(self as u64)
    }

    fn selector_value(&self) -> MysqlSelectorValue<'_> {
        MysqlSelectorValue::U64(*self as u64)
    }

    fn encoded_size_hint(&self) -> usize {
        std::mem::size_of::<u64>()
    }
}

impl MysqlValue for bool {
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.push_sqlx(self)
    }

    fn selector_value(&self) -> MysqlSelectorValue<'_> {
        MysqlSelectorValue::Bool(*self)
    }

    fn encoded_size_hint(&self) -> usize {
        1
    }
}

impl MysqlValue for f32 {
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.push_sqlx(self)
    }

    fn selector_value(&self) -> MysqlSelectorValue<'_> {
        MysqlSelectorValue::F64(f64::from(*self))
    }

    fn encoded_size_hint(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl MysqlValue for f64 {
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.push_sqlx(self)
    }

    fn selector_value(&self) -> MysqlSelectorValue<'_> {
        MysqlSelectorValue::F64(*self)
    }

    fn encoded_size_hint(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl MysqlValue for String {
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.push_sqlx(self)
    }

    fn selector_value(&self) -> MysqlSelectorValue<'_> {
        MysqlSelectorValue::String(self)
    }

    fn encoded_size_hint(&self) -> usize {
        self.len() + 9
    }
}

impl MysqlValue for &str {
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.push_sqlx(self)
    }

    fn selector_value(&self) -> MysqlSelectorValue<'_> {
        MysqlSelectorValue::String(self)
    }

    fn encoded_size_hint(&self) -> usize {
        self.len() + 9
    }
}

impl MysqlValue for Vec<u8> {
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.push_sqlx(self)
    }

    fn selector_value(&self) -> MysqlSelectorValue<'_> {
        MysqlSelectorValue::Bytes(self)
    }

    fn encoded_size_hint(&self) -> usize {
        self.len() + 9
    }
}

impl MysqlValue for &[u8] {
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.push_sqlx(self)
    }

    fn selector_value(&self) -> MysqlSelectorValue<'_> {
        MysqlSelectorValue::Bytes(self)
    }

    fn encoded_size_hint(&self) -> usize {
        self.len() + 9
    }
}

impl MysqlValue for NaiveDate {
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.push_sqlx(self)
    }

    fn selector_value(&self) -> MysqlSelectorValue<'_> {
        MysqlSelectorValue::Date(*self)
    }
}

impl MysqlValue for NaiveDateTime {
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.push_sqlx(self)
    }

    fn selector_value(&self) -> MysqlSelectorValue<'_> {
        MysqlSelectorValue::DateTime(*self)
    }
}

impl MysqlValue for NaiveTime {
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.push_sqlx(self)
    }

    fn selector_value(&self) -> MysqlSelectorValue<'_> {
        MysqlSelectorValue::Time(*self)
    }
}

impl MysqlValue for BigDecimal {
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.push_sqlx(self)
    }

    fn selector_value(&self) -> MysqlSelectorValue<'_> {
        MysqlSelectorValue::Unsupported("decimal")
    }
}

impl<T> MysqlValue for Json<T>
where
    T: Serialize + Send,
{
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.push_sqlx(sqlx::types::Json(self.0))
    }

    fn selector_value(&self) -> MysqlSelectorValue<'_> {
        MysqlSelectorValue::Unsupported("JSON")
    }
}

impl MysqlValue for serde_json::Value {
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        Json(self).write(writer)
    }

    fn selector_value(&self) -> MysqlSelectorValue<'_> {
        MysqlSelectorValue::Unsupported("JSON")
    }
}

impl<T> MysqlValue for Option<T>
where
    T: MysqlValue,
{
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        match self {
            Some(value) => value.write(writer),
            None => writer.push_sqlx(Option::<i64>::None),
        }
    }

    fn selector_value(&self) -> MysqlSelectorValue<'_> {
        match self {
            Some(value) => value.selector_value(),
            None => MysqlSelectorValue::Null,
        }
    }

    fn encoded_size_hint(&self) -> usize {
        self.as_ref().map_or(0, MysqlValue::encoded_size_hint)
    }
}

impl MysqlArgs for () {
    fn len(&self) -> usize {
        0
    }

    fn encoded_size_hint(&self) -> usize {
        0
    }

    fn first_selector_value(&self) -> Option<MysqlSelectorValue<'_>> {
        None
    }

    fn write(self, _writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        Ok(())
    }
}

impl<T> MysqlArgs for Vec<T>
where
    T: MysqlValue,
{
    fn len(&self) -> usize {
        self.len()
    }

    fn encoded_size_hint(&self) -> usize {
        self.iter().map(MysqlValue::encoded_size_hint).sum()
    }

    fn first_selector_value(&self) -> Option<MysqlSelectorValue<'_>> {
        self.first().map(MysqlValue::selector_value)
    }

    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        for value in self {
            value.write(writer)?;
        }
        Ok(())
    }
}

impl<T, const N: usize> MysqlArgs for [T; N]
where
    T: MysqlValue,
{
    fn len(&self) -> usize {
        N
    }

    fn encoded_size_hint(&self) -> usize {
        self.iter().map(MysqlValue::encoded_size_hint).sum()
    }

    fn first_selector_value(&self) -> Option<MysqlSelectorValue<'_>> {
        self.first().map(MysqlValue::selector_value)
    }

    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        for value in self {
            value.write(writer)?;
        }
        Ok(())
    }
}

macro_rules! tuple_args {
    ($count:expr; $first:ident:0 $(, $type:ident:$index:tt)*) => {
        impl<$first, $($type,)*> MysqlArgs for ($first, $($type,)*)
        where
            $first: MysqlValue,
            $($type: MysqlValue,)*
        {
            fn len(&self) -> usize {
                $count
            }

            fn encoded_size_hint(&self) -> usize {
                self.0.encoded_size_hint() $(+ self.$index.encoded_size_hint())*
            }

            fn first_selector_value(&self) -> Option<MysqlSelectorValue<'_>> {
                Some(self.0.selector_value())
            }

            fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
                self.0.write(writer)?;
                $(self.$index.write(writer)?;)*
                Ok(())
            }
        }
    };
}

tuple_args!(1; A:0);
tuple_args!(2; A:0, B:1);
tuple_args!(3; A:0, B:1, C:2);
tuple_args!(4; A:0, B:1, C:2, D:3);
tuple_args!(5; A:0, B:1, C:2, D:3, E:4);
tuple_args!(6; A:0, B:1, C:2, D:3, E:4, F:5);
tuple_args!(7; A:0, B:1, C:2, D:3, E:4, F:5, G:6);
tuple_args!(8; A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7);
tuple_args!(9; A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8);
tuple_args!(10; A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9);
tuple_args!(11; A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10);
tuple_args!(12; A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10, L:11);
tuple_args!(13; A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10, L:11, M:12);
tuple_args!(14; A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10, L:11, M:12, N:13);
tuple_args!(15; A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10, L:11, M:12, N:13, O:14);
tuple_args!(16; A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10, L:11, M:12, N:13, O:14, P:15);

pub(crate) fn encode_arguments<A: MysqlArgs>(arguments: A) -> MysqlResult<SqlxMySqlArguments> {
    let mut writer =
        MysqlValueWriter::with_capacity(arguments.len(), arguments.encoded_size_hint());
    arguments.write(&mut writer)?;
    Ok(writer.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TaskId(u64);

    impl MysqlValue for TaskId {
        fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
            writer.push(self.0)
        }

        fn selector_value(&self) -> MysqlSelectorValue<'_> {
            self.0.selector_value()
        }

        fn encoded_size_hint(&self) -> usize {
            self.0.encoded_size_hint()
        }
    }

    #[test]
    fn heterogeneous_tuple_exposes_first_selector_without_a_value_vector() {
        let arguments = (TaskId(17), "task", Option::<i64>::None);
        assert_eq!(arguments.len(), 3);
        assert_eq!(
            arguments.first_selector_value().unwrap().as_u64().unwrap(),
            17
        );
        encode_arguments(arguments).unwrap();
    }

    #[test]
    fn json_serializes_directly_into_mysql_arguments() {
        #[derive(Serialize)]
        struct Payload {
            enabled: bool,
        }

        encode_arguments((Json(Payload { enabled: true }),)).unwrap();
    }
}
