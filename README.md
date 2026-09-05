# brz-mysql

brz-mysql exposes an application-facing MySQL contract and keeps SQLx as its
private wire-protocol and connection-pool driver. Repositories provide SQL,
typed arguments, business result types, and application-owned routing policies. They do
not handle SQLx rows, connection acquisition, or transaction completion.

Create one process-lifetime MysqlService per database dependency. Ordinary and
sharded repositories share its read/write pool. This version supports table
routing within one database; it does not implement database sharding or fanout.

## Dependency

Pin the Git revision used by these examples in the consuming application:

```toml
brz-mysql = { git = "https://github.com/we-breeze/mysql.git", rev = "c33d21e21571f0957aed1fa5f7ce54d79f70beac" }
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

### Scalars, tuples and JSON columns

`FromMysqlCol` decodes one column by position. Implementations cover `i8` through
`i64`, `u8` through `u64`, `isize`/`usize`, `f32`/`f64`, `bool`, `String`, `Vec<u8>`,
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

```rust
use brz_mysql::{
    Mysql, MysqlResult, MysqlRoute, MysqlRouteValue, MysqlRouting,
    MysqlService, ShardedMysqlService,
};

struct TaskRouting {
    shard_count: u64, // validated as nonzero when loading application config
}

impl MysqlRouting for TaskRouting {
    fn resolve(&self, key: MysqlRouteValue<'_>) -> MysqlResult<MysqlRoute> {
        let uid = key.as_u64()?;
        let suffix = format!("{:04}", uid % self.shard_count);
        MysqlRoute::new()
            .with_table_suffix(&suffix)?
            .with_table("tasks", format!("tasks_{suffix}"))?
            .with_table("subtasks", format!("subtasks_{suffix}"))
    }
}

struct TaskRepository {
    mysql: ShardedMysqlService,
}

let mysql = MysqlService::connect(database_url).await?;
let repository = TaskRepository {
    mysql: mysql.with_route(TaskRouting { shard_count: 1024 }),
};

// No explicit routing key: uid is the first SQL argument and is also bound to ?.
let task: Task = repository.mysql.fetch_one(
    "SELECT id, title, config, deleted_at FROM {{tasks}} WHERE user_id = ? AND id = ?",
    (uid, task_id),
).await?;

// Explicit key: uid selects the table; title remains the first SQL argument.
repository.mysql.route(uid).execute(
    "UPDATE tasks_{{table_suffix}} SET title = ? WHERE id = ?",
    (title, task_id),
).await?;
```

The key precedence is **explicit `.route(key)` > first SQL argument**. An
explicit key is never added to the bound arguments. Creating a keyed handle
does not mutate the repository's handle; concurrent users can safely share the
same repository. `with_route` on an existing sharded handle replaces the policy
and clears any explicit key. Policies and retained keys must be owned/static;
shared application configuration can be held in an `Arc`.

Ordinary queries continue through `mysql.fetch_one(sql, args)` without calling
any policy. A sharded query requires at least one table template and a routing
key. Missing keys, rejected key types, missing template mappings, and invalid
identifiers fail before the query acquires a connection. A policy receives a
`MysqlRouteValue` and owns the meaning of that value: the component does not
infer whether a number represents a user id or an encoded task id.

The policy is evaluated once per query; `fetch` evaluates it only when the stream
is first polled. Closures implementing
`Fn(MysqlRouteValue<'_>) -> MysqlResult<MysqlRoute>` are supported as policies.
Custom `MysqlValue` implementations can expose their default routing key through
`route_value`; custom `MysqlArgs` can implement `first_route_value`. These methods
are not used by ordinary queries.

### SQL templates

Both template forms are supported, including multiple occurrences and joins:

```sql
SELECT id FROM tasks_{{table_suffix}} WHERE user_id = ?;
SELECT t.id FROM {{tasks}} t JOIN {{subtasks}} s ON s.task_id = t.id
WHERE t.user_id = ?;
```

- `table_suffix` is reserved for the suffix supplied by `with_table_suffix`.
  It must follow an ASCII identifier prefix and end that identifier.
- Other names refer to full tables supplied by `with_table(logical, physical)`.
  Full table tokens occupy a complete identifier and are rendered with backtick
  quoting. Already backtick-quoted templates are also supported.
- A policy may supply mappings that a particular query does not use. Every
  template used in the query must have a matching route result.
- Suffixes accept nonempty ASCII letters, digits and underscores. Table names
  accept ASCII letters, digits, underscores and dollar signs, cannot begin with
  a digit, and cannot contain database qualifiers. Identifiers are bounded by
  MySQL's 64-character limit. No SQL fragments are accepted as route results.
- Templates inside single/double-quoted string literals and ordinary comments
  are left unchanged. The scanner follows MySQL's usual backslash/doubled-quote
  string escaping; SQL using `ANSI_QUOTES` or `NO_BACKSLASH_ESCAPES` is not
  supported for template rendering. Templates inside executable `/*! ... */`
  comments are rejected. This is an identifier template renderer, not a SQL AST
  parser or a general-purpose text interpolation facility.

### Single-database transactions

A transaction does **not** require `.route(key)` before it starts. It already has
one fixed database connection. Each templated statement uses its own first SQL
argument, or an explicit key from `transaction.route(key)`:

```rust
use brz_mysql::MysqlTransaction;

repository.mysql.with_transaction(async |transaction| {
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
policy resolves once per templated statement. All statements commit or roll back
together, even when they address different physical tables in this database.
There is no per-statement connection/pool switch. When multi-database support is
added, transactions in that mode will be disabled initially.

See [examples/task_repository.rs](examples/task_repository.rs) for a compilable
repository using these interfaces. Close the parent `MysqlService` once at
application shutdown; closing it closes the shared pool for every handle.

### Migration

This is an intentional API replacement: `MysqlTableSharding`,
`MysqlTableSelector`, `MysqlTableSelection`, `MysqlSelectorValue`,
`MysqlServiceOptions::table_sharding`, and `with_table_sharding` are removed.
Move selection logic into `MysqlRouting`, attach it with `with_route`, and return
`MysqlRoute` mappings. The new `MysqlRouteValue` exposes the typed routing key.
There is no automatic legacy/base-table fallback.

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
