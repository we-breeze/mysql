//! Query-local routing supplied by the application.

use std::{borrow::Cow, collections::BTreeMap};

use crate::{MysqlError, MysqlResult, MysqlRouteValue};

/// Application policy evaluated once per routed statement.
/// The key is supplied by `.route(key)`, or by the first SQL argument when no
/// explicit key is bound. The policy defines what the key means.
pub trait MysqlRouting: Send + Sync {
    fn resolve(&self, key: MysqlRouteValue<'_>) -> MysqlResult<MysqlRoute>;
}

impl<F> MysqlRouting for F
where
    F: for<'key> Fn(MysqlRouteValue<'key>) -> MysqlResult<MysqlRoute> + Send + Sync,
{
    fn resolve(&self, key: MysqlRouteValue<'_>) -> MysqlResult<MysqlRoute> {
        self(key)
    }
}

/// A resolved route. This version selects physical tables in the service's
/// single database; database selection can be added independently later.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MysqlRoute {
    table_suffix: Option<String>,
    tables: BTreeMap<String, String>,
}

impl MysqlRoute {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a nonempty ASCII identifier suffix, for example
    /// `"0509"` for the SQL template `tasks_{{table_suffix}}`.
    pub fn with_table_suffix(mut self, value: impl Into<String>) -> MysqlResult<Self> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err(invalid(
                "table suffix must contain 1..=64 ASCII letters, digits or underscores",
            ));
        }
        self.table_suffix = Some(value);
        Ok(self)
    }

    /// Map a full logical table token, such as `{{tasks}}`, to one physical
    /// table in the current database. Names cannot contain SQL or a database
    /// qualifier. `table_suffix` is reserved for suffix substitution.
    pub fn with_table(
        mut self,
        logical: impl Into<String>,
        physical: impl Into<String>,
    ) -> MysqlResult<Self> {
        let logical = logical.into();
        let physical = physical.into();
        if logical == "table_suffix" || !valid_name(&logical) || !valid_name(&physical) {
            return Err(invalid(
                "table mappings require ASCII identifiers of 1..=64 characters; table_suffix is reserved",
            ));
        }
        if self.tables.insert(logical, physical).is_some() {
            return Err(invalid("duplicate logical table mapping"));
        }
        Ok(self)
    }

    pub fn suffix(&self) -> Option<&str> {
        self.table_suffix.as_deref()
    }

    pub fn table(&self, logical: &str) -> Option<&str> {
        self.tables.get(logical).map(String::as_str)
    }
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && !name.as_bytes()[0].is_ascii_digit()
        && name.bytes().all(identifier_byte)
}

pub(crate) fn invalid(reason: &str) -> MysqlError {
    MysqlError::InvalidQuery {
        reason: reason.to_owned(),
    }
}

/// Render only identifier templates, never quoted string data or comments.
/// `require_token` catches a forgotten template on a routed query. A routed
/// transaction permits plain statements alongside its templated statements.
pub(crate) fn render_sql<'sql>(
    sql: &'sql str,
    route: Option<&MysqlRoute>,
    require_token: bool,
) -> MysqlResult<Cow<'sql, str>> {
    if !sql.contains("{{") {
        return if require_token {
            Err(invalid("routed SQL must contain a table template"))
        } else {
            Ok(Cow::Borrowed(sql))
        };
    }

    let bytes = sql.as_bytes();
    let mut index = 0;
    let mut quoted_identifier = false;
    let mut quote_start = 0;
    let mut output = None::<String>;
    let mut copied = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' | b'"' if !quoted_identifier => {
                index = skip_string(bytes, index);
            }
            b'`' => {
                if quoted_identifier && bytes.get(index + 1) == Some(&b'`') {
                    index += 2;
                } else {
                    quoted_identifier = !quoted_identifier;
                    quote_start = index;
                    index += 1;
                }
            }
            b'#' if !quoted_identifier => {
                index = skip_line(bytes, index + 1);
            }
            b'-' if !quoted_identifier
                && bytes.get(index + 1) == Some(&b'-')
                && bytes
                    .get(index + 2)
                    .is_some_and(|byte| byte.is_ascii_whitespace()) =>
            {
                index = skip_line(bytes, index + 2);
            }
            b'/' if !quoted_identifier && bytes.get(index + 1) == Some(&b'*') => {
                let start = index;
                index += 2;
                while index + 1 < bytes.len() && &bytes[index..index + 2] != b"*/" {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
                // MySQL executes version comments. Do not silently leave a
                // template inside SQL hidden in an executable comment.
                if bytes.get(start + 2) == Some(&b'!') && sql[start..index].contains("{{") {
                    return Err(invalid(
                        "table templates are not supported in executable comments",
                    ));
                }
            }
            b'{' if bytes.get(index + 1) == Some(&b'{') => {
                let close = sql[index + 2..]
                    .find("}}")
                    .map(|offset| index + 2 + offset)
                    .ok_or_else(|| invalid("unterminated table template"))?;
                let name = &sql[index + 2..close];
                if !valid_name(name) {
                    return Err(invalid("table template must name an ASCII identifier"));
                }
                let route = route.ok_or_else(|| invalid("table templates require with_route"))?;
                let end = close + 2;
                let suffix = name == "table_suffix";
                let replacement = if suffix {
                    route
                        .suffix()
                        .ok_or_else(|| invalid("route did not provide table_suffix"))?
                } else {
                    route
                        .table(name)
                        .ok_or_else(|| invalid("route did not provide the logical table mapping"))?
                };
                if suffix {
                    validate_suffix_identifier(
                        bytes,
                        index,
                        end,
                        replacement.len(),
                        quoted_identifier.then_some(quote_start),
                    )?;
                } else {
                    validate_table_token(
                        bytes,
                        index,
                        end,
                        quoted_identifier.then_some(quote_start),
                    )?;
                }
                let rendered = output.get_or_insert_with(|| String::with_capacity(sql.len()));
                rendered.push_str(&sql[copied..index]);
                // Full names are quoted identifiers, including SQL keywords.
                if !suffix && !quoted_identifier {
                    rendered.push('`');
                }
                rendered.push_str(replacement);
                if !suffix && !quoted_identifier {
                    rendered.push('`');
                }
                copied = end;
                index = end;
            }
            _ => index += 1,
        }
    }

    match output {
        Some(mut output) => {
            output.push_str(&sql[copied..]);
            Ok(Cow::Owned(output))
        }
        None if require_token => Err(invalid(
            "routed SQL must contain a table template in an identifier",
        )),
        None => Ok(Cow::Borrowed(sql)),
    }
}

fn identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$'
}

fn validate_suffix_identifier(
    sql: &[u8],
    start: usize,
    end: usize,
    suffix_len: usize,
    quote: Option<usize>,
) -> MysqlResult<()> {
    let mut prefix = start;
    while prefix > 0 && identifier_byte(sql[prefix - 1]) {
        prefix -= 1;
    }
    if prefix == start || sql[prefix].is_ascii_digit() {
        return Err(invalid(
            "{{table_suffix}} must follow an ASCII identifier prefix",
        ));
    }
    if sql.get(end).is_some_and(|byte| identifier_byte(*byte)) {
        return Err(invalid("{{table_suffix}} must end the identifier"));
    }
    if let Some(quote) = quote
        && (prefix != quote + 1 || sql.get(end) != Some(&b'`') || sql.get(end + 1) == Some(&b'`'))
    {
        return Err(invalid(
            "templated quoted identifiers must contain an ASCII prefix followed by table_suffix",
        ));
    }
    if start - prefix + suffix_len > 64 {
        return Err(invalid(
            "rendered table identifier exceeds MySQL's 64-character limit",
        ));
    }
    Ok(())
}

fn validate_table_token(
    sql: &[u8],
    start: usize,
    end: usize,
    quote: Option<usize>,
) -> MysqlResult<()> {
    if let Some(quote) = quote {
        if start != quote + 1 || sql.get(end) != Some(&b'`') || sql.get(end + 1) == Some(&b'`') {
            return Err(invalid(
                "a full table template must occupy the entire identifier",
            ));
        }
    } else if start
        .checked_sub(1)
        .is_some_and(|index| identifier_byte(sql[index]))
        || sql.get(end).is_some_and(|byte| identifier_byte(*byte))
    {
        return Err(invalid(
            "a full table template must occupy the entire identifier",
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

    fn route() -> MysqlRoute {
        MysqlRoute::new()
            .with_table_suffix("0509")
            .unwrap()
            .with_table("tasks", "tasks_0509")
            .unwrap()
            .with_table("subtasks", "subtasks_0509")
            .unwrap()
    }

    #[test]
    fn renders_suffix_full_table_join_and_quoted_identifiers() {
        let route = route();
        let sql = "SELECT t.id FROM {{tasks}} t JOIN `{{subtasks}}` s ON s.task_id = t.id JOIN `tasks_{{table_suffix}}` t2 ON t2.id = t.id WHERE t.id = ?";
        assert_eq!(
            render_sql(sql, Some(&route), true).unwrap(),
            "SELECT t.id FROM `tasks_0509` t JOIN `subtasks_0509` s ON s.task_id = t.id JOIN `tasks_0509` t2 ON t2.id = t.id WHERE t.id = ?"
        );
        assert_eq!(
            render_sql("SELECT * FROM tasks_{{table_suffix}}", Some(&route), true).unwrap(),
            "SELECT * FROM tasks_0509"
        );
    }

    #[test]
    fn leaves_literals_and_comments_byte_for_byte() {
        let literals = [
            "'{{tasks}}'",
            "\"{{tasks}}\"",
            "'it''s {{tasks}}'",
            "'it\\'s {{tasks}}'",
            "'你好 {{table_suffix}}'",
        ];
        for literal in literals {
            let sql = format!(
                "SELECT {literal} FROM {{{{tasks}}}} -- {{{{ignored}}}}\n/* {{{{ignored}}}} */ # {{{{ignored}}}}\n"
            );
            let expected = sql.replacen("FROM {{tasks}}", "FROM `tasks_0509`", 1);
            assert_eq!(render_sql(&sql, Some(&route()), true).unwrap(), expected);
        }
        let plain = "SELECT '{{tasks}}' /* {{ignored}} */";
        assert!(matches!(
            render_sql(plain, None, false).unwrap(),
            Cow::Borrowed(_)
        ));
        assert!(render_sql(plain, Some(&route()), true).is_err());
    }

    #[test]
    fn rejects_missing_unknown_malformed_and_misplaced_templates() {
        assert!(render_sql("SELECT * FROM {{tasks}}", None, false).is_err());
        let suffix_only = MysqlRoute::new().with_table_suffix("0509").unwrap();
        assert!(render_sql("SELECT * FROM {{tasks}}", Some(&suffix_only), true).is_err());
        let full_only = MysqlRoute::new().with_table("tasks", "tasks_0509").unwrap();
        assert!(
            render_sql(
                "SELECT * FROM tasks_{{table_suffix}}",
                Some(&full_only),
                true
            )
            .is_err()
        );
        for sql in [
            "SELECT * FROM {{unknown}}",
            "SELECT * FROM {{tasks}",
            "SELECT * FROM {{ tasks }}",
            "SELECT * FROM prefix_{{tasks}}",
            "SELECT * FROM {{tasks}}_suffix",
            "SELECT * FROM {{table_suffix}}",
            "SELECT * FROM tasks_{{table_suffix}}more",
            "SELECT * FROM `prefix_{{tasks}}`",
            "SELECT * FROM `odd name_{{table_suffix}}`",
            "SELECT * FROM `{{tasks}}``more`",
            "SELECT * FROM tasks",
            "SELECT 1 /*! FROM {{tasks}} */",
        ] {
            assert!(render_sql(sql, Some(&route()), true).is_err(), "{sql}");
        }
    }

    #[test]
    fn rejects_unsafe_route_results_and_overlong_identifiers() {
        for suffix in ["", "a.b", "a`b", "a b", "a;b", "a'b", "a/*b", "中文"] {
            assert!(MysqlRoute::new().with_table_suffix(suffix).is_err());
        }
        for table in [
            "",
            "1tasks",
            "db.tasks",
            "tasks; DROP TABLE users",
            "`tasks`",
        ] {
            assert!(MysqlRoute::new().with_table("tasks", table).is_err());
        }
        assert!(
            MysqlRoute::new()
                .with_table("table_suffix", "tasks")
                .is_err()
        );
        assert!(
            MysqlRoute::new()
                .with_table("tasks", "tasks_01")
                .unwrap()
                .with_table("tasks", "tasks_02")
                .is_err()
        );
        assert!(MysqlRoute::new().with_table_suffix("a".repeat(65)).is_err());
        let route = MysqlRoute::new().with_table_suffix("a".repeat(60)).unwrap();
        assert!(render_sql("SELECT * FROM tasks_{{table_suffix}}", Some(&route), true).is_err());
    }

    #[test]
    fn full_names_are_quoted_and_unused_mappings_are_allowed() {
        let route = MysqlRoute::new()
            .with_table("tasks", "order")
            .unwrap()
            .with_table("unused", "unused_01")
            .unwrap();
        assert_eq!(
            render_sql("SELECT * FROM {{tasks}}", Some(&route), true).unwrap(),
            "SELECT * FROM `order`"
        );
    }
}
