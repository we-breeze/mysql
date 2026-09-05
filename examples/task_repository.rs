//! Compile with `cargo check --example task_repository`.
//! Run with DATABASE_URL pointing to a database containing your task tables.

use brz_mysql::{
    FromMysqlRow, Mysql, MysqlResult, MysqlRoute, MysqlRouteValue, MysqlRouting, MysqlService,
    MysqlTransaction, ShardedMysqlService,
};

struct TaskRouting {
    shard_count: u64,
}

impl MysqlRouting for TaskRouting {
    fn resolve(&self, key: MysqlRouteValue<'_>) -> MysqlResult<MysqlRoute> {
        // The application owns uid semantics and validates its configuration.
        let uid = key.as_u64()?;
        let suffix = format!("{:04}", uid % self.shard_count);
        MysqlRoute::new()
            .with_table_suffix(&suffix)?
            .with_table("tasks", format!("tasks_{suffix}"))?
            .with_table("subtasks", format!("subtasks_{suffix}"))
    }
}

#[derive(Debug, FromMysqlRow)]
struct Task {
    id: u64,
    title: String,
}

struct TaskRepository {
    mysql: ShardedMysqlService,
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

    async fn rename(&self, uid: u64, task_id: u64, title: &str) -> MysqlResult<()> {
        self.mysql
            .route(uid)
            .execute(
                "UPDATE tasks_{{table_suffix}} SET title = ? WHERE id = ?",
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
                    tx.route(*uid)
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
    let _ = TaskRepository::rename;
    let _ = TaskRepository::rename_atomically;
    if let Some(task) = tasks.find(509, 69_956_427_469_753).await? {
        println!("{}: {}", task.id, task.title);
    }
    mysql.close().await;
    Ok(())
}
