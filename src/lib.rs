//! Breeze MySQL protocol and connection-pool capability.
//!
//! Logical SQL, row-to-domain mapping, and repository policy stay in product
//! code. This crate owns the driver, parameter binding, transactions, pool
//! lifecycle, and physical database/table routing without exposing `sqlx` as
//! part of its public API.

extern crate self as brz_mysql;

mod api;
mod arguments;
mod mysql_service;
mod row;

pub use api::{
    Mysql, MysqlError, MysqlExecution, MysqlResult, MysqlServiceOptions, MysqlTableSelection,
    MysqlTableSelector, MysqlTableSharding, MysqlTransaction, PoolStats,
};
pub use arguments::{Json, MysqlArgs, MysqlSelectorValue, MysqlValue, MysqlValueWriter};
pub use brz_mysql_derive::FromMysqlRow;
pub use mysql_service::{MysqlService, MysqlTransactionService};
pub use row::{FromMysqlRow, FromMysqlValue, MysqlRow};
