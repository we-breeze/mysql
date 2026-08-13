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

use std::{str::FromStr, time::Duration};
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct MySqlPoolConfig {
    pub url: String,
    pub max_connections: u32,
    pub acquire_timeout: Duration,
    pub idle_timeout: Duration,
    pub max_lifetime: Duration,
    pub slow_acquire_threshold: Duration,
}

#[derive(Clone, Debug)]
pub struct MySqlResourceConfig {
    pub reader: MySqlPoolConfig,
    pub writer: MySqlPoolConfig,
}

#[derive(Clone, Debug)]
pub struct MySqlResource {
    reader: sqlx::MySqlPool,
    writer: sqlx::MySqlPool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolStats {
    pub size: u32,
    pub idle: usize,
    pub max_connections: u32,
}

#[derive(Debug, Error)]
pub enum ResourceError {
    #[error("invalid {role} MySQL pool configuration: {reason}")]
    InvalidConfig {
        role: &'static str,
        reason: &'static str,
    },
    #[error("invalid {role} MySQL connection URL: {source}")]
    InvalidUrl {
        role: &'static str,
        #[source]
        source: sqlx::Error,
    },
}

impl MySqlResource {
    /// Builds bounded read/write pools without opening a connection.
    pub fn connect_lazy(config: &MySqlResourceConfig) -> Result<Self, ResourceError> {
        Ok(Self {
            reader: build_pool("reader", &config.reader)?,
            writer: build_pool("writer", &config.writer)?,
        })
    }

    pub fn reader(&self) -> &sqlx::MySqlPool {
        &self.reader
    }

    pub fn writer(&self) -> &sqlx::MySqlPool {
        &self.writer
    }

    pub fn reader_stats(&self) -> PoolStats {
        pool_stats(&self.reader)
    }

    pub fn writer_stats(&self) -> PoolStats {
        pool_stats(&self.writer)
    }

    pub async fn close(&self) {
        tokio::join!(self.reader.close(), self.writer.close());
    }
}

fn pool_stats(pool: &sqlx::MySqlPool) -> PoolStats {
    PoolStats {
        size: pool.size(),
        idle: pool.num_idle(),
        max_connections: pool.options().get_max_connections(),
    }
}

fn build_pool(
    role: &'static str,
    config: &MySqlPoolConfig,
) -> Result<sqlx::MySqlPool, ResourceError> {
    validate_pool_config(role, config)?;
    let options = recorded_mysql_connect_options(&config.url)
        .map_err(|source| ResourceError::InvalidUrl { role, source })?;
    Ok(sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(config.max_connections)
        .min_connections(0)
        .acquire_timeout(config.acquire_timeout)
        .idle_timeout(config.idle_timeout)
        .max_lifetime(config.max_lifetime)
        .acquire_slow_threshold(config.slow_acquire_threshold)
        .connect_lazy_with(options))
}

fn validate_pool_config(role: &'static str, config: &MySqlPoolConfig) -> Result<(), ResourceError> {
    let invalid = |reason| ResourceError::InvalidConfig { role, reason };
    if config.max_connections == 0 {
        return Err(invalid("max_connections must be greater than zero"));
    }
    if config.acquire_timeout.is_zero() {
        return Err(invalid("acquire_timeout must be greater than zero"));
    }
    if config.idle_timeout.is_zero() {
        return Err(invalid("idle_timeout must be greater than zero"));
    }
    if config.max_lifetime.is_zero() {
        return Err(invalid("max_lifetime must be greater than zero"));
    }
    if config.slow_acquire_threshold.is_zero() {
        return Err(invalid("slow_acquire_threshold must be greater than zero"));
    }
    if config.slow_acquire_threshold > config.acquire_timeout {
        return Err(invalid(
            "slow_acquire_threshold must not exceed acquire_timeout",
        ));
    }
    Ok(())
}

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

#[cfg(test)]
mod tests {
    use super::{MySqlPoolConfig, MySqlResource, MySqlResourceConfig, ResourceError};
    use std::time::Duration;

    fn pool(url: &str) -> MySqlPoolConfig {
        MySqlPoolConfig {
            url: url.to_string(),
            max_connections: 4,
            acquire_timeout: Duration::from_secs(2),
            idle_timeout: Duration::from_secs(60),
            max_lifetime: Duration::from_secs(300),
            slow_acquire_threshold: Duration::from_millis(500),
        }
    }

    #[tokio::test]
    async fn resource_is_lazy_and_keeps_roles_separate() {
        let resource = MySqlResource::connect_lazy(&MySqlResourceConfig {
            reader: pool("mysql://reader:secret@127.0.0.1:3306/read_db"),
            writer: pool("mysql://writer:secret@127.0.0.1:3307/write_db"),
        })
        .unwrap();
        assert_eq!(resource.reader().size(), 0);
        assert_eq!(resource.writer().size(), 0);
        assert_eq!(resource.reader().options().get_max_connections(), 4);
        assert_eq!(
            resource.reader_stats(),
            super::PoolStats {
                size: 0,
                idle: 0,
                max_connections: 4,
            }
        );
        resource.close().await;
    }

    #[test]
    fn rejects_invalid_bounds_without_exposing_url() {
        let mut invalid = pool("mysql://user:top-secret@127.0.0.1/db");
        invalid.max_connections = 0;
        let error = MySqlResource::connect_lazy(&MySqlResourceConfig {
            reader: invalid,
            writer: pool("mysql://writer:secret@127.0.0.1/db"),
        })
        .unwrap_err()
        .to_string();
        assert!(error.contains("max_connections"));
        assert!(!error.contains("top-secret"));
    }

    #[test]
    fn rejects_slow_threshold_beyond_acquire_budget() {
        let mut invalid = pool("mysql://reader:secret@127.0.0.1/db");
        invalid.slow_acquire_threshold = Duration::from_secs(3);
        let error = MySqlResource::connect_lazy(&MySqlResourceConfig {
            reader: invalid,
            writer: pool("mysql://writer:secret@127.0.0.1/db"),
        })
        .unwrap_err();
        assert!(matches!(error, ResourceError::InvalidConfig { .. }));
    }

    #[test]
    fn invalid_url_error_is_credential_safe() {
        let error = MySqlResource::connect_lazy(&MySqlResourceConfig {
            reader: pool("mysql://user:top-secret@[broken/db"),
            writer: pool("mysql://writer:secret@127.0.0.1/db"),
        })
        .unwrap_err();
        assert!(!error.to_string().contains("top-secret"));
        assert!(!format!("{error:?}").contains("top-secret"));
    }
}
