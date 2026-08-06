//! brz-mysql: Breeze platform MySQL access capability.
//!
//! This crate is the single place that depends on the raw `sqlx` driver. It
//! owns the MySQL connection/pool lifecycle and re-exports the driver surface
//! so product code depends on `brz-mysql` instead of `sqlx` directly. SQL and
//! table mapping remain with the product; this crate only provides access.
//!
//! The connection options built by [`recorded_mysql_connect_options`] are pinned
//! to the recorded source wire behavior (pipes-as-concat, no engine
//! substitution, no timezone, no automatic SET NAMES). They MUST NOT drift, or
//! target MySQL traffic diverges from the recorded baseline.

// Re-export the full sqlx driver surface under the brz-mysql namespace. Product
// code migrates by replacing `sqlx::` with `brz_mysql::`; behavior is identical,
// which is required for recorded-traffic matching. A narrower capability port
// can be introduced later without changing call sites that already route here.
pub use sqlx::*;

use std::str::FromStr;

/// Build MySQL connect options pinned to the recorded source session semantics.
///
/// Mirrors the options the source uses when talking to MySQL through the
/// traffic-e2e recorder, so target MySQL wire traffic matches the recorded
/// baseline exactly.
pub fn recorded_mysql_connect_options(
    url: &str,
) -> Result<sqlx::mysql::MySqlConnectOptions, sqlx::Error> {
    Ok(sqlx::mysql::MySqlConnectOptions::from_str(url)?
        .pipes_as_concat(false)
        .no_engine_substitution(false)
        .timezone(None)
        .set_names(false))
}
