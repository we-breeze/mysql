# brz-mysql

brz-mysql exposes an application-facing MySQL contract and keeps SQLx as its
private wire-protocol and connection-pool driver. Repositories provide SQL,
typed arguments, business result types, and application-owned routing policies. They do
not handle SQLx rows, connection acquisition, or transaction completion.

Create one process-lifetime MysqlService per database dependency. Ordinary and
sharded repositories share its read/write pool. This version supports table
routing within one database; it does not implement database sharding or fanout.

## Dependency

Pin the release tag used by these examples in the consuming application:

```toml
mysql = { package = "brz-mysql", version = "0.0.5" }
```

Repository routing handles replace the earlier global sharding options.
See [Migration](#migration) when upgrading from `v0.0.1`.

## Typed queries

```rust
use brz_mysql::{FromMysqlRow, Json, MysqlService};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct TaskConfig {
    retryable: bool,
}

#[derive(Debug, FromMysqlRow)]
struct Task {
    id: u64,
    title: String,
    config: Json<TaskConfig>,
    deleted_at: Option<chrono::NaiveDateTime>,
}

let mysql = MysqlService::connect(database_url).await?;

let task: Task = mysql
    .fetch_one(
        "SELECT id, title, config, deleted_at FROM tasks WHERE id = ?",
        (task_id,),
    )
    .await?;

mysql
    .execute(
        "UPDATE tasks SET title = ? WHERE id = ?",
        ("new title", task_id),
    )
    .await?;
# Ok::<(), brz_mysql::MysqlError>(())
```

Arguments are heterogeneous tuples. Implementations exist for tuples up to 16
items, homogeneous arrays and vectors, primitive numeric values, strings,
bytes, dates and times, Option<T>, decimal values, and Json<T>. An application
newtype can implement MysqlValue by forwarding to MysqlValueWriter::push.

`FromMysqlRow` is the result conversion trait, with a derive macro for named
business structs. Use `#[mysql(rename = "column_name")]` when a field and column differ. Json<T>
serializes directly into SQLx's MySQL argument buffer and deserializes directly
from a JSON column.

### Binary downloads without a payload Vec

`BinaryColumn` is an owned column type sharing SQLx's received row storage.
It supports scalar results, tuples, derived structs, and streaming `fetch`:

```rust
use brz_mysql::{BinaryColumn, Mysql, MysqlResult};

async fn load_binary(mysql: &impl Mysql, id: i64) -> MysqlResult<Option<BinaryColumn>> {
    mysql.fetch_optional(
        "SELECT binary_data FROM skill_binaries WHERE kind_id = ? LIMIT 1",
        (id,),
    ).await
}
```

Use `as_bytes()` to borrow the payload, `len()` / `is_empty()` to inspect it,
and `into_bytes()` to move it into an HTTP response accepting `bytes::Bytes`.
Cloning and conversion preserve the payload allocation. There is a small
ownership wrapper allocation, but no intermediate payload `Vec<u8>` or copy.
The data survives row and connection release and does not hold a pool slot.
SQLx still buffers the complete row; the shared allocation may retain other
columns from that row until the response is released. This is not incremental
BLOB streaming. Use `Option<Option<BinaryColumn>>` with `fetch_optional` when
both a missing row and a NULL column are possible.

### Scalars, tuples and JSON columns

`FromMysqlCol` decodes one column by position. Implementations cover `i8` through
`i64`, `u8` through `u64`, `isize`/`usize`, `f32`/`f64`, `bool`, `String`, `Vec<u8>`, `BinaryColumn`,
Chrono date/time types, decimals, `Json<T>`, `serde_json::Value` and `Option<T>`.
Every column type also implements `FromMysqlRow` for exactly one column;
tuples of 1 to 16 column types decode the same number of columns in SELECT order.

```rust,no_run
use brz_mysql::{FromMysqlCol, Mysql, MysqlResult};
use serde::Deserialize;

#[derive(Debug, Deserialize, FromMysqlCol)]
struct Settings {
    enabled: bool,
    labels: Vec<String>,
}

async fn owner_id<M: Mysql>(mysql: &M, task_id: u64) -> MysqlResult<Option<i64>> {
    mysql.fetch_optional("SELECT user_id FROM tasks WHERE id = ?", (task_id,)).await
}

async fn user_settings<M: Mysql>(mysql: &M, id: u64)
    -> MysqlResult<Option<(u64, String, Settings)>>
{
    mysql.fetch_optional("SELECT id, name, settings FROM users WHERE id = ?", (id,)).await
}

async fn settings<M: Mysql>(mysql: &M, id: u64) -> MysqlResult<Option<Settings>> {
    mysql.fetch_optional("SELECT settings FROM users WHERE id = ?", (id,)).await
}
```

Deriving `FromMysqlCol` on a `Deserialize` type decodes one JSON column directly
into that type, including when it is a tuple element or a field in a struct
derived with `FromMysqlRow`. Existing `Json<T>` consumers keep the same API.
Use `FromMysqlRow` to construct a business struct from multiple named columns,
and `FromMysqlCol` to construct a value from one column. Implementing
`FromMysqlCol` manually also supports application newtypes through
`row.get_at::<ExistingType>(index)`.

The outer `Option` from `fetch_optional` denotes row presence. Inner
`Option<T>` values denote SQL NULL, so one nullable scalar uses
`Option<Option<T>>`. JSON `null` remains a JSON value when decoded as
`serde_json::Value`. Missing rows, NULL in a required column, decoding errors,
and scalar/tuple column-count mismatches retain distinct errors.

`fetch_one`, `fetch_optional`, `fetch_all` and streaming `fetch` share these
result conversions, including through `M: Mysql`, sharded handles and
transactions. Existing named `MysqlRow::get`/`get_required` and custom
`FromMysqlValue` implementations remain supported; `get_at` reads by zero-based
position, including results with repeated column names.

## Transactions

```rust
use brz_mysql::MysqlTransaction;

let task: Task = mysql
    .with_transaction(async |transaction| {
        transaction
            .execute(
                "UPDATE tasks SET title = ? WHERE id = ?",
                ("new title", task_id),
            )
            .await?;

        transaction
            .fetch_one(
                "SELECT id, title, config, deleted_at FROM tasks WHERE id = ?",
                (task_id,),
            )
            .await
    })
    .await?;
# Ok::<(), brz_mysql::MysqlError>(())
```

The public Mysql contract does not expose begin, commit, or rollback. Returning
Ok from the closure commits; returning Err rolls back. Cancellation or panic
drops the private SQLx transaction, which schedules rollback.

## Streaming

fetch is the primary large-result API. It is lazy and decodes one row at a
time. fetch_all is an explicit convenience that collects the same output into
Vec<T>. A polled stream owns one pooled connection until it completes or is
dropped; creating a stream without polling it does not acquire a connection.

```rust
use futures_util::{StreamExt, pin_mut};

let tasks = mysql.fetch::<_, _, Task>(
    "SELECT id, title, config, deleted_at FROM tasks WHERE id >= ? ORDER BY id",
    (first_id,),
);
pin_mut!(tasks);

while let Some(task) = tasks.next().await {
    consume(task?);
}
# Ok::<(), brz_mysql::MysqlError>(())
```

The public stream is returned as impl Stream rather than Box<dyn Stream>, so
the brz-mysql abstraction does not add a stream boxing allocation.

## Table routing

`Mysql::with_route` binds an application policy and returns an owned, fixed-type
`ShardedMysqlService`. It also implements `Mysql`, so a repository can retain it
without policy type parameters or borrowing the parent service. All handles
share the parent's pool. `MysqlServiceOptions` contains only pool/session
settings; routing does not change global service configuration.

A policy receives the current template name and a key, and returns a lightweight
`MysqlRouteOutput`. The component calls `write_to` to write it directly into the
SQL buffer. All `Display` types (including strings and numbers) implement
`MysqlRouteOutput` automatically; custom result objects can implement it directly.

```rust
use brz_mysql::{
    Mysql, MysqlError, MysqlResult, MysqlRouteKey, MysqlRouteOutput,
    MysqlRouting, MysqlService,
};

// Business types need no SQL encoding or Clone implementation.
struct ByTaskId(u64);
struct ByUserId(u64);

struct TaskRouting { shard_count: u64 } // validate as nonzero in application config
struct TableName<'a> { prefix: &'a str, slot: Option<u64> }

impl MysqlRouteOutput for TableName<'_> {
    fn write_to(&self, out: &mut dyn std::fmt::Write) -> std::fmt::Result {
        out.write_str(self.prefix)?;
        if let Some(slot) = self.slot {
            write!(out, "_{slot:04}")?;
        }
        Ok(())
    }
}

impl MysqlRouting for TaskRouting {
    fn resolve<'a>(
        &'a self,
        template: &'a str,
        key: &'a dyn MysqlRouteKey,
    ) -> MysqlResult<impl MysqlRouteOutput + 'a> {
        if !matches!(template, "tasks" | "subtasks") {
            return Err(MysqlError::InvalidQuery {
                reason: format!("unknown template: {template}"),
            });
        }
        // Example business rules, not built into the MySQL component.
        let uid = if let Some(ByTaskId(id)) = key.downcast_ref::<ByTaskId>() {
            if *id < (1 << 37) { None } else { Some((id >> 37) & 0xffff) }
        } else if let Some(ByUserId(uid)) = key.downcast_ref::<ByUserId>() {
            Some(*uid)
        } else {
            Some(key.as_u64()?) // allow numeric first-SQL-argument fallback
        };
        Ok(TableName {
            prefix: template,
            slot: uid.map(|uid| uid % self.shard_count),
        })
    }
}

let mysql = MysqlService::connect(database_url).await?;
let tasks = mysql.with_route(TaskRouting { shard_count: 1024 });

// The same number has different meanings and selects different tables.
tasks.route(ByTaskId(509)).execute(
    "DELETE FROM {{tasks}} WHERE id = ?", (509_u64,),
).await?; // tasks, for the example's legacy task ID rule

tasks.route(ByUserId(509)).execute(
    "DELETE FROM {{tasks}} WHERE user_id = ?", (509_u64,),
).await?; // tasks_0509

// No explicit key: the first argument selects the table and remains bound to ?.
tasks.execute("DELETE FROM {{tasks}} WHERE user_id = ?", (509_u64,)).await?;
```

`MysqlRouteKey` is an empty marker trait, automatically implemented for every
`Any + Send + Sync` type. `.route(key)` retains the original concrete type in an
`Arc`; strategies inspect it with `downcast_ref::<T>()`. The key never needs to
implement `MysqlValue`. Helpers `as_u64`, `as_i64`, `as_str`, and `as_bytes` support
ordinary integer/string/byte keys without parsing or truncating values.
Explicit `Option<T>` and newtypes retain their original type; their semantics
belong to the policy. Dynamic strings must be owned, not borrowed from locals.

The key precedence remains **explicit `.route(key)` > first SQL argument**.
An explicit key is never added to bound arguments. Binding a key does not change
the original handle; cloning shares its policy, key and pool. `with_route` on an
existing handle replaces the policy and clears its explicit key.

The implicit first-argument path still uses `MysqlValue::route_value()` and
`MysqlArgs::first_route_value()`. It adapts values once per templated statement:
integers become `i64`/`u64`, floats become `f64`, booleans and chrono values retain
their corresponding types, and NULL becomes `()`. Borrowed strings/bytes are
copied once to `String`/`Vec<u8>` to satisfy the `Any` contract; they remain usable
as SQL parameters. Unsupported values require an explicit `.route(key)`.
No adaptation runs for plain SQL or when an explicit key is present.

Ordinary SQL can use either the parent `MysqlService` or the same sharded handle.
SQL without templates bypasses key extraction and the policy, even with an
explicit key. It reuses the existing template check and borrows the original SQL
without copying. Forgetting a template executes the table name as written,
which can succeed if that table exists.

For templated SQL, each distinct template name is resolved and written once per
statement. Repeated names reuse a range in the final SQL buffer; no intermediate
replacement string is stored. Results can borrow from the policy, template, or
key and are consumed immediately without boxing. The final SQL and template
range cache still allocate; this is not a completely allocation-free query path.
Streams resolve only when first polled. Errors from the policy, formatter, or
identifier validation fail before a query acquires a connection.

Closures `Fn(&str, &dyn MysqlRouteKey) -> MysqlResult<O>` are also supported when
`O: MysqlRouteOutput + 'static`, for example a closure returning `Ok("tasks")`,
a number, or an owned formatting object. For results borrowing from inputs,
implement `MysqlRouting` as above. An internal adapter retains the fixed
`ShardedMysqlService` type while each policy returns its own concrete result;
`MysqlRouting` itself is no longer usable as a trait object.

### SQL templates

Template names come from the SQL. The policy is only asked for names used by the
current statement; it no longer returns a table map or handles unused names.
Templates can replace whole identifiers or fragments, including quoted names:

```sql
SELECT id FROM {{tasks}} WHERE user_id = ?;
SELECT id FROM tasks_{{slot}} WHERE user_id = ?;
SELECT id FROM `{{prefix}}_{{slot}}` WHERE user_id = ?;
SELECT t.id FROM {{tasks}} t JOIN {{subtasks}} s ON s.task_id = t.id
WHERE t.user_id = ?;
```

- `table_suffix` continues to work as a template name, but is no longer reserved.
  The policy receives that name just like `tasks`, `slot`, or `prefix`.
- Each replacement must write 1..=64 ASCII letters, digits, underscores or dollar
  signs. The complete rendered identifier must also fit 64 characters and cannot
  start with a digit. The component adds backtick quoting. Dots, quotes, SQL
  fragments and non-ASCII replacements are rejected, including formatter writes
  whose errors were ignored by the result object.
- Templates inside single/double-quoted string literals and ordinary comments
  are unchanged. The scanner follows MySQL's usual backslash/doubled-quote string
  escaping; `ANSI_QUOTES` and `NO_BACKSLASH_ESCAPES` modes are unsupported for
  rendering. Templates inside executable `/*! ... */` comments are rejected.
  This is an identifier renderer, not a SQL AST parser or general interpolator.

### Single-database transactions

A transaction does **not** require `.route(key)` before it starts. It already has
one fixed database connection. Each templated statement uses its own first SQL
argument, or an explicit key from `transaction.route(key)`:

```rust
use brz_mysql::MysqlTransaction;

tasks.with_transaction(async |transaction| {
    transaction.execute(
        "DELETE FROM {{subtasks}} WHERE user_id = ? AND task_id = ?",
        (uid, task_id),
    ).await?;
    transaction.route(other_uid).execute(
        "UPDATE {{tasks}} SET title = ? WHERE id = ?",
        (title, other_task_id),
    ).await?;
    // Plain statements use the same connection and bypass the routing policy.
    transaction.execute("UPDATE users SET is_active = ? WHERE id = ?", (true, uid)).await?;
    Ok(())
}).await?;
```

An optional `.route(key)` on the service supplies the transaction's default key;
a per-statement key overrides it without changing subsequent statements. The
policy resolves once per distinct template name in each statement. All statements
commit or roll back together, even when they address different physical tables
in this database.
There is no per-statement connection/pool switch. When multi-database support is
added, transactions in that mode will be disabled initially.

See [examples/task_repository.rs](examples/task_repository.rs) for a compilable
repository using these interfaces. Close the parent `MysqlService` once at
application shutdown; closing it closes the shared pool for every handle.

### Migration

This intentionally changes the routing API from 0.0.6:

- Replace `resolve(key: MysqlRouteValue) -> MysqlResult<MysqlRoute>` with
  `resolve(template, key: &dyn MysqlRouteKey) -> MysqlResult<impl MysqlRouteOutput>`
  (use the shared lifetime shown above for borrowed results).
- `MysqlRoute`, `with_table`, and `with_table_suffix` are removed. Match the
  current template name and return its replacement as a string, `Display`
  value, or lightweight object implementing `write_to`.
- Explicit custom keys no longer implement `MysqlValue` or convert themselves
  into a primitive enum. Use `downcast_ref` in the strategy. `MysqlRouteValue`
  remains only as the first-SQL-argument adapter for existing `MysqlValue` and
  `MysqlArgs` implementations.
- A policy now runs once per distinct template name, rather than once for the
  entire statement. Template names in existing SQL can be kept.

There is no automatic legacy/base-table fallback. The example's `ByTaskId` and
`ByUserId` behavior is defined entirely by its application policy.

## Allocation boundary

MysqlArgs reserves and writes values directly into SQLx's final MySQL argument
buffer. It does not first build Vec<MysqlValue>. MysqlRow wraps the driver row
and decodes only requested fields, without copying the row into an intermediate
map.

The shared Breeze EphemeralBytesArena is intentionally not used here. Redis
owns its full wire-frame encoding, so an arena allocation can be the final
socket-write storage. SQLx owns MySqlArguments as an internal Vec<u8>; placing
an arena in front of it would add an extra copy. If Breeze later owns the MySQL
wire transport, the arena belongs at that protocol-frame boundary.

## Development

```bash
cargo fmt --all --check
cargo test --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Real integration tests use MySQL 5.7.18:

```bash
./scripts/integration-test.sh
```

The included benchmark is intended for functional comparison. Use a dedicated
native MySQL instance for trustworthy measurements:

```bash
./tools/mysql-bench/bench.sh --ops 100000 --concurrency 64
MYSQL_URL=mysql://... ./tools/mysql-bench/bench_local.sh
```

### Driver defaults and traffic replay

The component retains SQLx's normal connection initialization (`SET NAMES` and
SQL mode setup) and prepared-statement execution. Applications do not change
these behaviors to accommodate a recorder or replay server. A replay framework
must accept the driver's initialization, correlate text and prepared queries
without changing SQL values, and return the appropriate column metadata,
binary rows, and transaction status flags.

The routing policy affects physical table identifiers only. It does not choose
a wire protocol or classify API versus process-owned dependencies; those are
driver and traffic-framework responsibilities respectively.

### MySQL 指标

创建连接池时按 URL 的 host 注册四个 `MYSQL` 指标，不包含端口、数据库、用户名或密码；同一 host 的多个连接池共享计数：

| 指标 | 操作 |
| --- | --- |
| `<host>_get` | `fetch_optional`、`fetch_one` |
| `<host>_list` | `fetch`、`fetch_all` |
| `<host>_update` | `execute`，包括插入、更新和删除 |
| `<host>_transaction` | `with_transaction`，从获取事务连接到提交或回滚完成 |

名称只在连接池创建时拼接，查询时使用缓存句柄。计时包含连接等待、SQL 执行和结果解码，沿用资源指标的 50ms 慢调用阈值。事务内语句同时计入各自操作指标；`ping` 不计入。

每次查询计数一次，列表不会按行重复计数。`fetch_optional` 返回 `None` 属于成功，`fetch_one` 的 `RowNotFound` 属于失败。流第一次被 poll 时开始计时，读至结束且没有错误才算成功；开始后提前丢弃的流、取消的调用，以及回滚的事务均记为失败。尚未 poll 的 future/stream 不计数。分片路由在进入底层查询之前发生的解析错误不计入数据库调用指标。

## Releases

CI runs formatting, Clippy, and tests. To publish, open **Actions → Publish → Run workflow** on `main`. Leave `retry_tag` empty to allocate the next `v0.0.x` tag. The workflow validates the code, commits the version, pushes the commit and tag atomically, and publishes to crates.io using the organization secret `CARGO_REGISTRY_TOKEN`.

If publication fails after the tag was pushed, rerun with that existing tag in `retry_tag`. A normal push or pull request does not publish. Historical tags retain their original version numbers; use new release tags for registry packages.

## License

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.

The workflow releases `brz-mysql-derive` before `brz-mysql` at the same version. It verifies the macro package before tagging, then verifies the main package after the macro becomes available in the registry. Retrying skips an uploaded package only when its checksum matches the locally packaged artifact.
