//! Compile with `cargo check --example task_repository`.
//! Run with DATABASE_URL pointing to a database containing your task tables.

use brz_mysql::{
    FromMysqlRow, Mysql, MysqlResult, MysqlRouteKey, MysqlRouteOutput, MysqlRouting, MysqlService,
    MysqlTransaction,
};

// These types need no MysqlValue implementation and preserve their meaning.
struct ByTaskId(u64);
struct ByUserId(u64);

struct TaskRouting {
    shard_count: u64,
}

struct TableName<'a> {
    prefix: &'a str,
    slot: Option<u64>,
}

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
            return Err(brz_mysql::MysqlError::InvalidQuery {
                reason: format!("unknown template: {template}"),
            });
        }
        // Example application rules: legacy task IDs use the base table;
        // new-format IDs carry the owner's uid in bits 37..53.
        let uid = if let Some(ByTaskId(id)) = key.downcast_ref::<ByTaskId>() {
            if *id < (1 << 37) {
                None
            } else {
                Some((id >> 37) & 0xffff)
            }
        } else if let Some(ByUserId(uid)) = key.downcast_ref::<ByUserId>() {
            Some(*uid)
        } else {
            // Keep first-SQL-argument routing for ordinary numeric user IDs.
            Some(key.as_u64()?)
        };
        // No String allocation: borrow the template and retain just the slot.
        Ok(TableName {
            prefix: template,
            slot: uid.map(|uid| uid % self.shard_count),
        })
    }
}

#[derive(Debug, FromMysqlRow)]
struct Task {
    id: u64,
    title: String,
}

struct TaskRepository {
    mysql: MysqlService,
}

impl TaskRepository {
    fn new<M: Mysql>(mysql: &M) -> Self {
        Self {
            mysql: mysql.with_route(TaskRouting { shard_count: 1024 }),
        }
    }

    async fn find(&self, uid: u64, task_id: u64) -> MysqlResult<Option<Task>> {
        // First-argument routing; uid is still bound to the first SQL placeholder.
        self.mysql
            .fetch_optional(
                "SELECT id, title FROM {{tasks}} WHERE user_id = ? AND id = ?",
                (uid, task_id),
            )
            .await
    }

    async fn find_by_task_id(&self, task_id: u64) -> MysqlResult<Option<Task>> {
        self.mysql
            .route(ByTaskId(task_id))
            .fetch_optional("SELECT id, title FROM {{tasks}} WHERE id = ?", (task_id,))
            .await
    }

    async fn rename(&self, uid: u64, task_id: u64, title: &str) -> MysqlResult<()> {
        self.mysql
            .route(ByUserId(uid))
            .execute(
                "UPDATE {{tasks}} SET title = ? WHERE id = ?",
                (title, task_id),
            )
            .await?;
        Ok(())
    }

    async fn rename_atomically(&self, changes: &[(u64, u64, String)]) -> MysqlResult<()> {
        // One database: a transaction may update multiple physical tables.
        self.mysql
            .with_transaction(async |tx| {
                for (uid, task_id, title) in changes {
                    tx.route(ByUserId(*uid))
                        .execute(
                            "UPDATE {{tasks}} SET title = ? WHERE id = ?",
                            (title.as_str(), *task_id),
                        )
                        .await?;
                }
                Ok(())
            })
            .await
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mysql = MysqlService::connect(&std::env::var("DATABASE_URL")?).await?;
    let tasks = TaskRepository::new(&mysql);
    // Importing this example does not write any rows. These methods show the
    // write/transaction interface and can be called by the owning application.
    let _ = TaskRepository::find_by_task_id;
    let _ = TaskRepository::rename;
    let _ = TaskRepository::rename_atomically;
    if let Some(task) = tasks.find(509, 69_956_427_469_753).await? {
        println!("{}: {}", task.id, task.title);
    }
    mysql.close().await;
    Ok(())
}
