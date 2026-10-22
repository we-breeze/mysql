//! Compile the transaction futures under the Send requirement used by servers.
use brz_mysql::{Mysql, MysqlResult, MysqlService, MysqlTransaction, MysqlTransactionService};

fn require_send(_: impl Send) {}

async fn generic_transaction<M: Mysql>(mysql: &M, value: &str) -> MysqlResult<()> {
    mysql
        .with_transaction(async |transaction| {
            transaction
                .execute("DELETE FROM widgets WHERE name = ?", (value,))
                .await?;
            Ok(())
        })
        .await
}

async fn callback_wrapper<T, F>(mysql: &MysqlService, operation: F) -> MysqlResult<T>
where
    T: Send,
    F: for<'a> AsyncFnOnce(&'a mut MysqlTransactionService) -> MysqlResult<T> + Send,
{
    mysql
        .with_transaction(async |transaction| operation(transaction).await)
        .await
}

#[test]
fn outer_generic_and_wrapped_transactions_are_send_with_borrowed_captures() {
    fn check(mysql: &MysqlService, value: &str) {
        require_send(generic_transaction(mysql, value));
        require_send(callback_wrapper(mysql, async |transaction| {
            transaction
                .execute("DELETE FROM widgets WHERE name = ?", (value,))
                .await?;
            Ok(())
        }));
    }
    let _ = check;
}

#[test]
fn nested_and_routed_transactions_are_send_with_borrowed_captures() {
    fn check(mysql: &MysqlService, value: &str) {
        require_send(mysql.with_transaction(async |outer| {
            outer
                .route(1_u64)
                .with_transaction(async |inner| {
                    inner
                        .with_transaction(async |deep| {
                            deep.execute("DELETE FROM widgets WHERE name = ?", (value,))
                                .await?;
                            Ok(())
                        })
                        .await
                })
                .await
        }));
    }
    let _ = check;
}
