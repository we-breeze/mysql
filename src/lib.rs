//! Breeze MySQL protocol and connection-pool capability.
//!
//! Logical SQL, row-to-domain mapping, and repository policy stay in product
//! code. This crate owns the driver, parameter binding, transactions, pool
//! lifecycle, and physical database/table routing without exposing `sqlx` as
//! part of its public API.

extern crate self as brz_mysql;

mod api;
mod arguments;
mod binary_column;
mod column;
mod metrics;
mod mysql_service;
mod query_routing;
mod routed_transaction;
mod routing;
mod row;

pub use api::{
    Mysql, MysqlError, MysqlExecution, MysqlResult, MysqlServiceOptions, MysqlTransaction,
    PoolStats,
};
pub use arguments::{Json, MysqlArgs, MysqlRouteValue, MysqlValue, MysqlValueWriter};
pub use binary_column::BinaryColumn;
pub use brz_mysql_derive::{FromMysqlCol, FromMysqlRow};
pub use column::FromMysqlCol;
pub use mysql_service::{MysqlService, MysqlTransactionService};
pub use routed_transaction::RoutedMysqlTransaction;
pub use routing::{MysqlRouteKey, MysqlRouteOutput, MysqlRouting};
pub use row::{FromMysqlRow, FromMysqlValue, MysqlRow};
