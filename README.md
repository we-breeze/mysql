# brz-mysql

brz-mysql exposes an application-facing MySQL contract and keeps SQLx as its
private wire-protocol and connection-pool driver. Repositories provide SQL,
typed arguments, and result structs; they do not handle SQLx rows, transaction
completion, or physical table names.

The first version matches Wegent's current topology: one read/write pool with
optional table selection. Reader/writer pools and database routing can be added
inside MysqlService without changing repository queries.

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

FromMysqlRow is a derive macro for named structs. Use
#[mysql(rename = "column_name")] when a field and column differ. Json<T>
serializes directly into SQLx's MySQL argument buffer and deserializes directly
from a JSON column.

## Transactions

```rust
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

## Table selection

A table-sharded MysqlService is constructed with a selector. Every query sent
through that service must contain a configured logical-table token, and its
first SQL argument must be the table-selection value.

```rust
use brz_mysql::{
    MysqlSelectorValue, MysqlService, MysqlServiceOptions, MysqlTableSelection,
    MysqlTableSharding,
};

let sharding = MysqlTableSharding::new(
    16,
    ["tasks", "subtasks"],
    |first: MysqlSelectorValue<'_>| {
        Ok(MysqlTableSelection::Shard((first.as_u64()? % 16) as u32))
    },
)?;

let mysql = MysqlService::connect_with_options(
    database_url,
    MysqlServiceOptions::default().with_table_sharding(sharding),
)
.await?;

let task: Task = mysql
    .fetch_one(
        "SELECT id, title, config, deleted_at          FROM {{tasks}} WHERE owner_user_id = ? AND id = ?",
        (owner_user_id, task_id),
    )
    .await?;
# Ok::<(), brz_mysql::MysqlError>(())
```

The selector can return Base for a legacy table or Shard(index). Physical names
are precomputed when the service configuration is built. The first argument is
still encoded normally as the first SQL placeholder; there is no separate
routing parameter and no all-partition fanout API.

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
