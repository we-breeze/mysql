//! Connection access shared by ordinary and explicitly configured root scopes.
use super::owned::ConfiguredTransaction;
use crate::{MysqlResult, mysql_service::map_sqlx_error};
use sqlx::{MySql, MySqlConnection};
use std::ops::{Deref, DerefMut};

pub(crate) enum InnerTransaction {
    Ordinary(sqlx::Transaction<'static, MySql>),
    Configured(ConfiguredTransaction),
}

impl InnerTransaction {
    pub(crate) async fn commit(self) -> MysqlResult<()> {
        match self {
            Self::Ordinary(transaction) => transaction.commit().await.map_err(map_sqlx_error),
            Self::Configured(transaction) => transaction.commit().await,
        }
    }
    pub(crate) async fn rollback(self) -> MysqlResult<()> {
        match self {
            Self::Ordinary(transaction) => transaction.rollback().await.map_err(map_sqlx_error),
            Self::Configured(transaction) => transaction.rollback().await,
        }
    }
}

impl Deref for InnerTransaction {
    type Target = MySqlConnection;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Ordinary(transaction) => transaction,
            Self::Configured(transaction) => transaction.connection(),
        }
    }
}
impl DerefMut for InnerTransaction {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Ordinary(transaction) => transaction,
            Self::Configured(transaction) => transaction.connection_mut(),
        }
    }
}
