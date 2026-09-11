//! Query-local routing supplied by the application.

use std::{any::Any, borrow::Cow, fmt, ops::Range};

use crate::{MysqlError, MysqlResult};

/// An owned routing key whose meaning is a contract between caller and policy.
/// All `Any + Send + Sync` types implement this marker automatically; keys do
/// not need to implement SQL argument encoding or `Clone`.
pub trait MysqlRouteKey: Any + Send + Sync {}

impl<T: Any + Send + Sync> MysqlRouteKey for T {}

impl dyn MysqlRouteKey {
    /// Inspect the original concrete type of an explicit routing key.
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        (self as &dyn Any).downcast_ref()
    }

    /// Read an integer key without truncation or parsing strings.
    pub fn as_u64(&self) -> MysqlResult<u64> {
        macro_rules! integer {
            ($($ty:ty),+) => {$(
                if let Some(value) = self.downcast_ref::<$ty>() {
                    return u64::try_from(*value).map_err(|_| invalid("routing key is outside u64 range"));
                }
            )+};
        }
        integer!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);
        Err(invalid("routing key is not an integer"))
    }

    pub fn as_i64(&self) -> MysqlResult<i64> {
        macro_rules! integer {
            ($($ty:ty),+) => {$(
                if let Some(value) = self.downcast_ref::<$ty>() {
                    return i64::try_from(*value).map_err(|_| invalid("routing key is outside i64 range"));
                }
            )+};
        }
        integer!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);
        Err(invalid("routing key is not an integer"))
    }

    pub fn as_str(&self) -> MysqlResult<&str> {
        if let Some(value) = self.downcast_ref::<String>() {
            return Ok(value);
        }
        self.downcast_ref::<&'static str>()
            .copied()
            .ok_or_else(|| invalid("routing key is not a string"))
    }

    pub fn as_bytes(&self) -> MysqlResult<&[u8]> {
        if let Some(value) = self.downcast_ref::<Vec<u8>>() {
            return Ok(value);
        }
        self.downcast_ref::<&'static [u8]>()
            .copied()
            .ok_or_else(|| invalid("routing key is not bytes"))
    }
}

/// A lightweight replacement that writes directly into the final SQL buffer.
/// Implement this trait for custom result objects, or return any `Display` value.
/// A result may borrow from the policy, template name, or routing key.
pub trait MysqlRouteOutput {
    fn write_to(&self, out: &mut dyn fmt::Write) -> fmt::Result;
}

impl<T: fmt::Display + ?Sized> MysqlRouteOutput for T {
    fn write_to(&self, out: &mut dyn fmt::Write) -> fmt::Result {
        write!(out, "{self}")
    }
}

/// Compute a replacement object for one template name in the SQL.
/// Each distinct name is resolved once per statement, on first poll for a stream.
/// Plain SQL never calls the policy. The key's meaning belongs to the application.
/// The concrete result is written immediately without boxing or string conversion.
pub trait MysqlRouting: Send + Sync {
    fn resolve<'a>(
        &'a self,
        template: &'a str,
        key: &'a dyn MysqlRouteKey,
    ) -> MysqlResult<impl MysqlRouteOutput + 'a>;
}

impl<F, O> MysqlRouting for F
where
    F: Fn(&str, &dyn MysqlRouteKey) -> MysqlResult<O> + Send + Sync,
    O: MysqlRouteOutput + 'static,
{
    fn resolve<'a>(
        &'a self,
        template: &'a str,
        key: &'a dyn MysqlRouteKey,
    ) -> MysqlResult<impl MysqlRouteOutput + 'a> {
        self(template, key)
    }
}

// Erase only the policy stored by the fixed-type service. Returned values keep
// their concrete type and can borrow their inputs until write_to completes.
pub(crate) trait RouteRenderer: Send + Sync {
    fn render(
        &self,
        template: &str,
        key: &dyn MysqlRouteKey,
        out: &mut dyn fmt::Write,
    ) -> MysqlResult<()>;
}

impl<R: MysqlRouting> RouteRenderer for R {
    fn render(
        &self,
        template: &str,
        key: &dyn MysqlRouteKey,
        out: &mut dyn fmt::Write,
    ) -> MysqlResult<()> {
        self.resolve(template, key)?
            .write_to(out)
            .map_err(|_| invalid("failed to write a valid template replacement"))
    }
}

// The policy never receives the String itself, so it cannot overwrite SQL.
// Reject invalid/oversized writes before growing the final buffer.
struct IdentifierWriter<'a> {
    output: &'a mut String,
    start: usize,
    failed: bool,
}

impl fmt::Write for IdentifierWriter<'_> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        if self.failed
            || value.len() > 64 - (self.output.len() - self.start)
            || !value.bytes().all(identifier_byte)
        {
            self.failed = true;
            return Err(fmt::Error);
        }
        self.output.push_str(value);
        Ok(())
    }
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && !name.as_bytes()[0].is_ascii_digit()
        && name.bytes().all(identifier_byte)
}

fn identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$'
}

pub(crate) fn invalid(reason: &str) -> MysqlError {
    MysqlError::InvalidQuery {
        reason: reason.to_owned(),
    }
}

/// Render identifier templates in one scan. Resolution and cache allocation are
/// lazy: ordinary SQL is returned by reference without invoking the resolver.
pub(crate) fn render_sql<'sql>(
    sql: &'sql str,
    mut resolve: impl FnMut(&str, &mut dyn fmt::Write) -> MysqlResult<()>,
) -> MysqlResult<Cow<'sql, str>> {
    if !sql.contains("{{") {
        return Ok(Cow::Borrowed(sql));
    }

    let bytes = sql.as_bytes();
    let mut index = 0;
    let mut output = None::<String>;
    let mut copied = 0;
    let mut replacements = Vec::<(&str, Range<usize>)>::new();
    while index < bytes.len() {
        let start = index;
        let identifier = match bytes[index] {
            b'\'' | b'"' => {
                index = skip_string(bytes, index);
                None
            }
            b'#' => {
                index = skip_line(bytes, index + 1);
                None
            }
            b'-' if bytes.get(index + 1) == Some(&b'-')
                && bytes
                    .get(index + 2)
                    .is_some_and(|byte| byte.is_ascii_whitespace()) =>
            {
                index = skip_line(bytes, index + 2);
                None
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() && &bytes[index..index + 2] != b"*/" {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
                if bytes.get(start + 2) == Some(&b'!') && sql[start..index].contains("{{") {
                    return Err(invalid(
                        "table templates are not supported in executable comments",
                    ));
                }
                None
            }
            b'`' => {
                index += 1;
                loop {
                    if index == bytes.len() {
                        if sql[start..].contains("{{") {
                            return Err(invalid("unterminated templated identifier"));
                        }
                        break None;
                    }
                    if bytes[index] == b'`' {
                        index += 1;
                        if bytes.get(index) != Some(&b'`') {
                            break Some(&sql[start + 1..index - 1]);
                        }
                    }
                    index += 1;
                }
            }
            byte if identifier_byte(byte) || bytes.get(index..index + 2) == Some(b"{{") => {
                while index < bytes.len() {
                    if bytes.get(index..index + 2) == Some(b"{{") {
                        index = template_end(sql, index)?;
                    } else if identifier_byte(bytes[index]) {
                        index += 1;
                    } else {
                        break;
                    }
                }
                Some(&sql[start..index])
            }
            _ => {
                index += 1;
                None
            }
        };
        if let Some(identifier) = identifier.filter(|identifier| identifier.contains("{{")) {
            // Reject non-ASCII identifier surroundings rather than interpreting
            // just the ASCII tail of an unsupported identifier as a table name.
            if start
                .checked_sub(1)
                .is_some_and(|prev| !bytes[prev].is_ascii())
                || bytes.get(index).is_some_and(|byte| !byte.is_ascii())
            {
                return Err(invalid("templated identifiers must be ASCII"));
            }
            let output = output.get_or_insert_with(|| String::with_capacity(sql.len()));
            output.push_str(&sql[copied..start]);
            output.push('`');
            render_identifier(identifier, output, &mut replacements, &mut resolve)?;
            output.push('`');
            copied = index;
        }
    }
    match output {
        Some(mut output) => {
            output.push_str(&sql[copied..]);
            Ok(Cow::Owned(output))
        }
        None => Ok(Cow::Borrowed(sql)),
    }
}

fn template_end(sql: &str, start: usize) -> MysqlResult<usize> {
    sql[start + 2..]
        .find("}}")
        .map(|offset| start + 2 + offset + 2)
        .ok_or_else(|| invalid("unterminated table template"))
}

fn render_identifier<'sql>(
    identifier: &'sql str,
    output: &mut String,
    replacements: &mut Vec<(&'sql str, Range<usize>)>,
    resolve: &mut impl FnMut(&str, &mut dyn fmt::Write) -> MysqlResult<()>,
) -> MysqlResult<()> {
    let identifier_start = output.len();
    let mut copied = 0;
    while let Some(offset) = identifier[copied..].find("{{") {
        let start = copied + offset;
        let end = template_end(identifier, start)?;
        let name = &identifier[start + 2..end - 2];
        if !valid_name(name) {
            return Err(invalid("table template must name an ASCII identifier"));
        }
        output.push_str(&identifier[copied..start]);
        match replacements.iter().find(|(cached, _)| *cached == name) {
            Some((_, range)) => output.extend_from_within(range.clone()),
            None => {
                let start = output.len();
                let mut writer = IdentifierWriter {
                    output,
                    start,
                    failed: false,
                };
                resolve(name, &mut writer)?;
                if writer.failed || writer.output.len() == start {
                    return Err(invalid(
                        "template replacement must contain 1..=64 ASCII identifier characters",
                    ));
                }
                replacements.push((name, start..output.len()));
            }
        }
        copied = end;
    }
    output.push_str(&identifier[copied..]);
    if !valid_name(&output[identifier_start..]) {
        return Err(invalid(
            "rendered table must be an ASCII identifier of 1..=64 characters and cannot start with a digit",
        ));
    }
    Ok(())
}

fn skip_string(sql: &[u8], mut index: usize) -> usize {
    let quote = sql[index];
    index += 1;
    while index < sql.len() {
        if sql[index] == b'\\' {
            index = (index + 2).min(sql.len());
        } else if sql[index] == quote {
            index += 1;
            if sql.get(index) == Some(&quote) {
                index += 1;
            } else {
                break;
            }
        } else {
            index += 1;
        }
    }
    index
}

fn skip_line(sql: &[u8], mut index: usize) -> usize {
    while index < sql.len() && sql[index] != b'\n' && sql[index] != b'\r' {
        index += 1;
    }
    index
}
#[cfg(test)]
mod tests {
    use super::*;

    fn render_sql<'a>(
        sql: &'a str,
        mut resolve: impl FnMut(&str) -> MysqlResult<String>,
    ) -> MysqlResult<Cow<'a, str>> {
        super::render_sql(sql, |name, out| {
            resolve(name)?
                .write_to(out)
                .map_err(|_| invalid("invalid output"))
        })
    }

    fn resolve(template: &str) -> MysqlResult<String> {
        match template {
            "tasks" => Ok("tasks_0509".into()),
            "subtasks" => Ok("subtasks_0509".into()),
            "table_suffix" | "slot" => Ok("0509".into()),
            "prefix" => Ok("tasks".into()),
            _ => Err(invalid("unknown template")),
        }
    }

    #[test]
    fn renders_full_names_fragments_joins_and_quoted_identifiers() {
        for (sql, expected) in [
            ("SELECT * FROM {{tasks}}", "SELECT * FROM `tasks_0509`"),
            (
                "SELECT * FROM tasks_{{table_suffix}}",
                "SELECT * FROM `tasks_0509`",
            ),
            ("SELECT * FROM tasks_{{slot}}", "SELECT * FROM `tasks_0509`"),
            (
                "SELECT * FROM `tasks_{{slot}}`",
                "SELECT * FROM `tasks_0509`",
            ),
            (
                "SELECT * FROM {{prefix}}_{{slot}}",
                "SELECT * FROM `tasks_0509`",
            ),
            (
                "SELECT * FROM {{prefix}}_archive",
                "SELECT * FROM `tasks_archive`",
            ),
            (
                "SELECT t.id FROM {{tasks}} t JOIN `{{subtasks}}` s ON s.task_id = t.id WHERE t.id = ?",
                "SELECT t.id FROM `tasks_0509` t JOIN `subtasks_0509` s ON s.task_id = t.id WHERE t.id = ?",
            ),
        ] {
            assert_eq!(render_sql(sql, resolve).unwrap(), expected);
        }
        assert_eq!(
            render_sql("SELECT * FROM {{tasks}}", |_| Ok("order".into())).unwrap(),
            "SELECT * FROM `order`"
        );
    }

    #[test]
    fn resolves_only_used_names_once_per_statement() {
        let mut calls = Vec::new();
        let sql = "SELECT * FROM {{tasks}} a JOIN {{subtasks}} b JOIN `{{tasks}}` c JOIN tasks_{{slot}} d JOIN other_{{slot}} e";
        render_sql(sql, |name| {
            calls.push(name.to_owned());
            resolve(name)
        })
        .unwrap();
        assert_eq!(calls, ["tasks", "subtasks", "slot"]);
        // Cached results never escape the current query.
        assert_eq!(
            render_sql("SELECT * FROM {{tasks}}", |_| Ok("tasks_0002".into())).unwrap(),
            "SELECT * FROM `tasks_0002`"
        );
    }

    #[test]
    fn cached_ranges_survive_final_sql_buffer_growth() {
        let name = "a".repeat(64);
        let sql = "SELECT '你好' FROM {{t}} a JOIN {{t}} b JOIN {{t}} c";
        let mut calls = 0;
        let rendered = super::render_sql(sql, |_, out| {
            calls += 1;
            name.write_to(out).map_err(|_| invalid("invalid output"))
        })
        .unwrap();
        assert_eq!(
            rendered,
            format!("SELECT '你好' FROM `{name}` a JOIN `{name}` b JOIN `{name}` c")
        );
        assert_eq!(calls, 1);
    }

    #[test]
    fn leaves_plain_sql_literals_and_comments_unchanged_without_resolving() {
        for sql in [
            "SELECT * FROM tasks WHERE id = ?",
            "SELECT '{{tasks}}', \"{{tasks}}\" /* {{ignored}} */ -- {{ignored}}\n# {{ignored}}",
            "SELECT 'it''s {{tasks}}', 'it\\'s {{tasks}}', '你好 {{slot}}'",
            "SELECT `odd``name` FROM tasks /* {{ignored}} */",
        ] {
            let result = render_sql(sql, |_| panic!("plain SQL must not resolve")).unwrap();
            assert!(matches!(result, Cow::Borrowed(_)));
            assert_eq!(result, sql);
        }
        let sql = "SELECT '{{tasks}}' FROM {{tasks}} /* {{subtasks}} */";
        assert_eq!(
            render_sql(sql, resolve).unwrap(),
            "SELECT '{{tasks}}' FROM `tasks_0509` /* {{subtasks}} */"
        );
    }

    #[test]
    fn rejects_unknown_malformed_and_unsafe_templates() {
        for sql in [
            "SELECT * FROM {{unknown}}",
            "SELECT * FROM {{tasks}",
            "SELECT * FROM {{ tasks }}",
            "SELECT * FROM {{}}",
            "SELECT * FROM {{中文}}",
            "SELECT * FROM {{table_suffix}}", // numeric full identifier
            "SELECT * FROM `odd name_{{slot}}`",
            "SELECT * FROM `{{tasks}}``more`",
            "SELECT * FROM `{{tasks}}",
            "SELECT * FROM 中文{{slot}}",
            "SELECT * FROM {{tasks}}中文",
            "SELECT 1 /*! FROM {{tasks}} */",
        ] {
            assert!(render_sql(sql, resolve).is_err(), "{sql}");
        }
        assert!(render_sql("SELECT * FROM {{tasks}}", |_| Err(invalid("no policy"))).is_err());
    }

    #[test]
    fn validates_replacements_and_final_identifier_length() {
        for value in [
            "",
            "a.b",
            "a`b",
            "a b",
            "a;b",
            "a'b",
            "a/*b",
            "中文",
            "{{other}}",
        ] {
            for sql in ["SELECT * FROM {{tasks}}", "SELECT * FROM tasks_{{slot}}"] {
                assert!(
                    render_sql(sql, |_| Ok(value.into())).is_err(),
                    "{value}: {sql}"
                );
            }
        }
        assert!(render_sql("SELECT * FROM {{tasks}}", |_| Ok("1tasks".into())).is_err());
        assert!(render_sql("SELECT * FROM {{tasks}}", |_| Ok("a".repeat(65))).is_err());
        assert!(render_sql("SELECT * FROM tasks_{{slot}}", |_| Ok("a".repeat(59))).is_err());
        assert!(render_sql("SELECT * FROM {{left}}{{right}}", |_| Ok("a".repeat(33))).is_err());
        assert!(render_sql("SELECT * FROM tasks_{{slot}}", |_| Ok("a".repeat(58))).is_ok());
    }

    #[test]
    fn keys_preserve_types_and_numeric_helpers_check_ranges() {
        let small: &dyn MysqlRouteKey = &17_u16;
        assert_eq!(small.downcast_ref::<u16>(), Some(&17));
        assert!(small.downcast_ref::<u64>().is_none());
        assert_eq!(small.as_u64().unwrap(), 17);
        assert_eq!(small.as_i64().unwrap(), 17);
        assert!((&-1_i64 as &dyn MysqlRouteKey).as_u64().is_err());
        assert!((&u64::MAX as &dyn MysqlRouteKey).as_i64().is_err());
        assert!((&"17" as &dyn MysqlRouteKey).as_u64().is_err());
        assert_eq!(
            (&"tenant" as &dyn MysqlRouteKey).as_str().unwrap(),
            "tenant"
        );
        assert_eq!(
            (&String::from("tenant") as &dyn MysqlRouteKey)
                .as_str()
                .unwrap(),
            "tenant"
        );
        assert_eq!(
            (&vec![1_u8, 2] as &dyn MysqlRouteKey).as_bytes().unwrap(),
            &[1, 2]
        );
    }
}
