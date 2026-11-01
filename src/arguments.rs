//! Allocation-conscious MySQL argument encoding.

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use serde::Serialize;
use sqlx::{
    Arguments as _, Encode, MySql, Type, mysql::MySqlArguments as SqlxMySqlArguments,
    types::BigDecimal,
};
use std::fmt;

use crate::{MysqlError, MysqlResult};

/// A borrowed, allocation-free view of the first SQL argument for implicit routing.
/// Explicit keys preserve their original type through `MysqlRouteKey` instead.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MysqlRouteValue<'a> {
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

impl<'value> MysqlRouteValue<'value> {
    pub fn as_u64(self) -> MysqlResult<u64> {
        match self {
            Self::U64(value) => Ok(value),
            Self::I64(value) => u64::try_from(value).map_err(|_| route_type_error("u64")),
            _ => Err(route_type_error("u64")),
        }
    }

    pub fn as_i64(self) -> MysqlResult<i64> {
        match self {
            Self::I64(value) => Ok(value),
            Self::U64(value) => i64::try_from(value).map_err(|_| route_type_error("i64")),
            _ => Err(route_type_error("i64")),
        }
    }

    pub fn as_str(self) -> MysqlResult<&'value str> {
        match self {
            Self::String(value) => Ok(value),
            _ => Err(route_type_error("string")),
        }
    }

    pub fn as_bytes(self) -> MysqlResult<&'value [u8]> {
        match self {
            Self::Bytes(value) => Ok(value),
            _ => Err(route_type_error("bytes")),
        }
    }
}

// Adapt borrowed SQL values to the owned/static key contract only if a real
// template needs implicit routing. Numeric values stay inline; strings/bytes
// are copied once per statement, never for plain SQL or explicit keys.
pub(crate) enum ArgumentRouteKey {
    Null,
    String(String),
    Bytes(Vec<u8>),
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Time(NaiveTime),
}

impl ArgumentRouteKey {
    pub(crate) fn new(value: MysqlRouteValue<'_>) -> MysqlResult<Self> {
        Ok(match value {
            MysqlRouteValue::Null => Self::Null,
            MysqlRouteValue::String(value) => Self::String(value.to_owned()),
            MysqlRouteValue::Bytes(value) => Self::Bytes(value.to_vec()),
            MysqlRouteValue::Bool(value) => Self::Bool(value),
            MysqlRouteValue::I64(value) => Self::I64(value),
            MysqlRouteValue::U64(value) => Self::U64(value),
            MysqlRouteValue::F64(value) => Self::F64(value),
            MysqlRouteValue::Date(value) => Self::Date(value),
            MysqlRouteValue::DateTime(value) => Self::DateTime(value),
            MysqlRouteValue::Time(value) => Self::Time(value),
            MysqlRouteValue::Unsupported(_) => {
                return Err(crate::routing::invalid(
                    "first SQL argument cannot be used as a routing key; use .route(key)",
                ));
            }
        })
    }

    pub(crate) fn as_key(&self) -> &dyn crate::MysqlRouteKey {
        match self {
            Self::Null => &(),
            Self::String(value) => value,
            Self::Bytes(value) => value,
            Self::Bool(value) => value,
            Self::I64(value) => value,
            Self::U64(value) => value,
            Self::F64(value) => value,
            Self::Date(value) => value,
            Self::DateTime(value) => value,
            Self::Time(value) => value,
        }
    }
}

fn route_type_error(expected: &'static str) -> MysqlError {
    MysqlError::InvalidQuery {
        reason: format!("the routing key cannot be used as {expected}"),
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
    fn route_value(&self) -> MysqlRouteValue<'_> {
        MysqlRouteValue::Unsupported(std::any::type_name::<Self>())
    }

    #[doc(hidden)]
    fn encoded_size_hint(&self) -> usize {
        0
    }

    /// Format the value for statement error logs. Custom values can override
    /// the default type description without adding a Debug bound.
    #[doc(hidden)]
    fn fmt_log_value(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "<{}>", std::any::type_name::<Self>())
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
    fn first_route_value(&self) -> Option<MysqlRouteValue<'_>> {
        None
    }

    #[doc(hidden)]
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()>;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Format parameters for statement error logs without requiring Debug.
    /// Custom argument containers can override this default representation.
    #[doc(hidden)]
    fn fmt_log_arguments(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "<{} parameters: {}>",
            self.len(),
            std::any::type_name::<Self>()
        )
    }
}

/// Internal encoder exposed only so application newtypes can implement
/// MysqlValue by forwarding to an existing implementation.
#[doc(hidden)]
pub struct MysqlValueWriter {
    inner: SqlxMySqlArguments,
    position: usize,
    parameters: Option<String>,
}

impl MysqlValueWriter {
    pub(crate) fn with_capacity(arguments: usize, bytes: usize) -> Self {
        let mut inner = SqlxMySqlArguments::default();
        inner.reserve(arguments, bytes);
        Self {
            inner,
            position: 0,
            parameters: None,
        }
    }

    pub fn push<V: MysqlValue>(&mut self, value: V) -> MysqlResult<()> {
        value.write(self)
    }

    pub(crate) fn into_inner(self) -> SqlxMySqlArguments {
        self.inner
    }

    fn record_value<V: MysqlValue>(&mut self, value: &V) {
        use fmt::Write as _;
        if let Some(parameters) = &mut self.parameters {
            if self.position > 0 {
                parameters.push_str(", ");
            }
            if write!(parameters, "{}", ValueLog(value)).is_err() {
                parameters.push_str("<unable to format parameter>");
            }
        }
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

struct ValueLog<'value, V>(&'value V);

impl<V: MysqlValue> fmt::Display for ValueLog<'_, V> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt_log_value(formatter)
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

macro_rules! log_value {
    (display) => {
        fn fmt_log_value(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            fmt::Display::fmt(self, formatter)
        }
    };
    (debug) => {
        fn fmt_log_value(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            fmt::Debug::fmt(self, formatter)
        }
    };
    (quoted) => {
        fn fmt_log_value(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "\"{self}\"")
        }
    };
    (bytes) => {
        fn fmt_log_value(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("0x")?;
            for byte in self.iter() {
                write!(formatter, "{byte:02x}")?;
            }
            Ok(())
        }
    };
}

macro_rules! signed_value {
    ($($type:ty),+ $(,)?) => {$(
        impl MysqlValue for $type {
            log_value!(display);
            fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
                writer.record_value(&self);
                writer.push_sqlx(self)
            }

            fn route_value(&self) -> MysqlRouteValue<'_> {
                MysqlRouteValue::I64(*self as i64)
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
            log_value!(display);
            fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
                writer.record_value(&self);
                writer.push_sqlx(self)
            }

            fn route_value(&self) -> MysqlRouteValue<'_> {
                MysqlRouteValue::U64(*self as u64)
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
    log_value!(display);
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.record_value(&self);
        writer.push_sqlx(self as i64)
    }

    fn route_value(&self) -> MysqlRouteValue<'_> {
        MysqlRouteValue::I64(*self as i64)
    }

    fn encoded_size_hint(&self) -> usize {
        std::mem::size_of::<i64>()
    }
}

impl MysqlValue for usize {
    log_value!(display);
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.record_value(&self);
        writer.push_sqlx(self as u64)
    }

    fn route_value(&self) -> MysqlRouteValue<'_> {
        MysqlRouteValue::U64(*self as u64)
    }

    fn encoded_size_hint(&self) -> usize {
        std::mem::size_of::<u64>()
    }
}

impl MysqlValue for bool {
    log_value!(display);
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.record_value(&self);
        writer.push_sqlx(self)
    }

    fn route_value(&self) -> MysqlRouteValue<'_> {
        MysqlRouteValue::Bool(*self)
    }

    fn encoded_size_hint(&self) -> usize {
        1
    }
}

impl MysqlValue for f32 {
    log_value!(display);
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.record_value(&self);
        writer.push_sqlx(self)
    }

    fn route_value(&self) -> MysqlRouteValue<'_> {
        MysqlRouteValue::F64(f64::from(*self))
    }

    fn encoded_size_hint(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl MysqlValue for f64 {
    log_value!(display);
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.record_value(&self);
        writer.push_sqlx(self)
    }

    fn route_value(&self) -> MysqlRouteValue<'_> {
        MysqlRouteValue::F64(*self)
    }

    fn encoded_size_hint(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl MysqlValue for String {
    log_value!(debug);
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.record_value(&self);
        writer.push_sqlx(self)
    }

    fn route_value(&self) -> MysqlRouteValue<'_> {
        MysqlRouteValue::String(self)
    }

    fn encoded_size_hint(&self) -> usize {
        self.len() + 9
    }
}

impl MysqlValue for &str {
    log_value!(debug);
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.record_value(&self);
        writer.push_sqlx(self)
    }

    fn route_value(&self) -> MysqlRouteValue<'_> {
        MysqlRouteValue::String(self)
    }

    fn encoded_size_hint(&self) -> usize {
        self.len() + 9
    }
}

impl MysqlValue for Vec<u8> {
    log_value!(bytes);
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.record_value(&self);
        writer.push_sqlx(self)
    }

    fn route_value(&self) -> MysqlRouteValue<'_> {
        MysqlRouteValue::Bytes(self)
    }

    fn encoded_size_hint(&self) -> usize {
        self.len() + 9
    }
}

impl MysqlValue for &[u8] {
    log_value!(bytes);
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.record_value(&self);
        writer.push_sqlx(self)
    }

    fn route_value(&self) -> MysqlRouteValue<'_> {
        MysqlRouteValue::Bytes(self)
    }

    fn encoded_size_hint(&self) -> usize {
        self.len() + 9
    }
}

impl MysqlValue for NaiveDate {
    log_value!(quoted);
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.record_value(&self);
        writer.push_sqlx(self)
    }

    fn route_value(&self) -> MysqlRouteValue<'_> {
        MysqlRouteValue::Date(*self)
    }
}

impl MysqlValue for NaiveDateTime {
    log_value!(quoted);
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.record_value(&self);
        writer.push_sqlx(self)
    }

    fn route_value(&self) -> MysqlRouteValue<'_> {
        MysqlRouteValue::DateTime(*self)
    }
}

impl MysqlValue for NaiveTime {
    log_value!(quoted);
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.record_value(&self);
        writer.push_sqlx(self)
    }

    fn route_value(&self) -> MysqlRouteValue<'_> {
        MysqlRouteValue::Time(*self)
    }
}

impl MysqlValue for BigDecimal {
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.record_value(&self);
        writer.push_sqlx(self)
    }

    fn route_value(&self) -> MysqlRouteValue<'_> {
        MysqlRouteValue::Unsupported("decimal")
    }

    fn fmt_log_value(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl<T> MysqlValue for Json<T>
where
    T: Serialize + Send,
{
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        writer.record_value(&self);
        writer.push_sqlx(sqlx::types::Json(self.0))
    }

    fn route_value(&self) -> MysqlRouteValue<'_> {
        MysqlRouteValue::Unsupported("JSON")
    }

    fn fmt_log_value(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match serde_json::to_string(&self.0) {
            Ok(value) => formatter.write_str(&value),
            Err(error) => write!(formatter, "<JSON serialization failed: {error}>"),
        }
    }
}

impl MysqlValue for serde_json::Value {
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        Json(self).write(writer)
    }

    fn route_value(&self) -> MysqlRouteValue<'_> {
        MysqlRouteValue::Unsupported("JSON")
    }

    fn fmt_log_value(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl<T> MysqlValue for Option<T>
where
    T: MysqlValue,
{
    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        match self {
            Some(value) => value.write(writer),
            None => {
                writer.record_value(&Option::<i64>::None);
                writer.push_sqlx(Option::<i64>::None)
            }
        }
    }

    fn route_value(&self) -> MysqlRouteValue<'_> {
        match self {
            Some(value) => value.route_value(),
            None => MysqlRouteValue::Null,
        }
    }

    fn encoded_size_hint(&self) -> usize {
        self.as_ref().map_or(0, MysqlValue::encoded_size_hint)
    }

    fn fmt_log_value(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Some(value) => value.fmt_log_value(formatter),
            None => formatter.write_str("null"),
        }
    }
}

impl MysqlArgs for () {
    fn len(&self) -> usize {
        0
    }

    fn encoded_size_hint(&self) -> usize {
        0
    }

    fn first_route_value(&self) -> Option<MysqlRouteValue<'_>> {
        None
    }

    fn write(self, _writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        Ok(())
    }

    fn fmt_log_arguments(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[]")
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

    fn first_route_value(&self) -> Option<MysqlRouteValue<'_>> {
        self.first().map(MysqlValue::route_value)
    }

    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        for value in self {
            value.write(writer)?;
        }
        Ok(())
    }

    fn fmt_log_arguments(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_log_values(self.iter(), formatter)
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

    fn first_route_value(&self) -> Option<MysqlRouteValue<'_>> {
        self.first().map(MysqlValue::route_value)
    }

    fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
        for value in self {
            value.write(writer)?;
        }
        Ok(())
    }

    fn fmt_log_arguments(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_log_values(self.iter(), formatter)
    }
}

fn fmt_log_values<'value, T: MysqlValue + 'value>(
    values: impl Iterator<Item = &'value T>,
    formatter: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    formatter.write_str("[")?;
    for (index, value) in values.enumerate() {
        if index > 0 {
            formatter.write_str(", ")?;
        }
        value.fmt_log_value(formatter)?;
    }
    formatter.write_str("]")
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

            fn first_route_value(&self) -> Option<MysqlRouteValue<'_>> {
                Some(self.0.route_value())
            }

            fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
                self.0.write(writer)?;
                $(self.$index.write(writer)?;)*
                Ok(())
            }

            fn fmt_log_arguments(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("[")?;
                self.0.fmt_log_value(formatter)?;
                $(formatter.write_str(", ")?; self.$index.fmt_log_value(formatter)?;)*
                formatter.write_str("]")
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
    encode_arguments_with_log(arguments, false).0
}

pub(crate) fn encode_arguments_with_log<A: MysqlArgs>(
    arguments: A,
    capture_parameters: bool,
) -> (MysqlResult<SqlxMySqlArguments>, Option<String>) {
    let mut writer =
        MysqlValueWriter::with_capacity(arguments.len(), arguments.encoded_size_hint());
    writer.parameters = capture_parameters.then(|| "[".to_string());
    let result = arguments.write(&mut writer);
    let parameters = writer.parameters.take().map(|mut parameters| {
        if result.is_err() {
            parameters.push_str(" <encoding failed>");
        }
        parameters.push(']');
        parameters
    });
    (result.map(|()| writer.into_inner()), parameters)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TaskId(u64);

    impl MysqlValue for TaskId {
        fn write(self, writer: &mut MysqlValueWriter) -> MysqlResult<()> {
            writer.push(self.0)
        }

        fn route_value(&self) -> MysqlRouteValue<'_> {
            self.0.route_value()
        }

        fn encoded_size_hint(&self) -> usize {
            self.0.encoded_size_hint()
        }
    }

    #[test]
    fn heterogeneous_tuple_exposes_first_routing_key_without_a_value_vector() {
        let arguments = (TaskId(17), "task", Option::<i64>::None);
        assert_eq!(arguments.len(), 3);
        assert_eq!(arguments.first_route_value().unwrap().as_u64().unwrap(), 17);
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
