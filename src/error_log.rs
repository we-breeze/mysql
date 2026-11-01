//! Statement failures retain the original SQL and bound parameter values.
use std::{fmt, fmt::Write as _, time::Duration};

use crate::{MysqlArgs, MysqlError};

pub(crate) struct QueryLog {
    operation: &'static str,
    sql: Box<str>,
    sql_truncated: bool,
    parameters: String,
}

impl QueryLog {
    pub(crate) fn new(operation: &'static str, sql: &str) -> Option<Self> {
        if !tracing::enabled!(target: "breeze.mysql", tracing::Level::ERROR) {
            return None;
        }
        let mut length = sql.len().min(4 * 1024);
        while !sql.is_char_boundary(length) {
            length -= 1;
        }
        Some(Self {
            operation,
            sql: sql[..length].into(),
            sql_truncated: length < sql.len(),
            parameters: "<not encoded>".into(),
        })
    }

    pub(crate) fn set_parameters(&mut self, parameters: String) {
        self.parameters = parameters;
    }

    pub(crate) fn capture_parameters<A: MysqlArgs>(&mut self, arguments: &A) {
        self.parameters.clear();
        if write!(self.parameters, "{}", Arguments(arguments)).is_err() {
            self.parameters.push_str("<unable to format parameters>");
        }
    }

    pub(crate) fn error(&self, elapsed: Duration, error: &MysqlError) {
        tracing::error!(
            target: "breeze.mysql",
            operation = self.operation,
            elapsed_ms = elapsed.as_secs_f64() * 1000.0,
            error = %error,
            database_code = error.database_code(),
            sql_state = error.database_sql_state(),
            sql = %self.sql,
            sql_truncated = self.sql_truncated,
            parameters = %self.parameters,
            "MySQL statement failed",
        );
    }
}

struct Arguments<'arguments, A>(&'arguments A);

impl<A: MysqlArgs> fmt::Display for Arguments<'_, A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt_log_arguments(formatter)
    }
}

#[cfg(test)]
mod tests;
