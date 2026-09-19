use std::time::Instant;

#[cfg(feature = "metrics")]
use brz_metrics::Metric;

#[derive(Clone, Copy, Debug)]
pub(crate) struct MetricHandle {
    #[cfg(feature = "metrics")]
    inner: Metric,
}

impl MetricHandle {
    #[cfg(feature = "metrics")]
    fn mysql(name: &str) -> Self {
        Self {
            inner: Metric::mysql(name),
        }
    }

    fn record(self, elapsed: std::time::Duration, success: bool) {
        #[cfg(feature = "metrics")]
        self.inner.record(elapsed, success);
        #[cfg(not(feature = "metrics"))]
        let _ = (elapsed, success);
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct MysqlMetrics {
    pub(crate) read: MetricHandle,
    pub(crate) write: MetricHandle,
    pub(crate) transaction: MetricHandle,
}

impl MysqlMetrics {
    #[cfg(feature = "metrics")]
    pub(crate) fn new(name: &str) -> Self {
        Self {
            read: MetricHandle::mysql(&format!("{name}_r")),
            write: MetricHandle::mysql(&format!("{name}_w")),
            transaction: MetricHandle::mysql(&format!("{name}_t")),
        }
    }

    #[cfg(not(feature = "metrics"))]
    pub(crate) fn new(_name: &str) -> Self {
        Self {
            read: MetricHandle {},
            write: MetricHandle {},
            transaction: MetricHandle {},
        }
    }
}

pub(crate) struct Observation {
    metric: Option<MetricHandle>,
    started: Instant,
    finished: bool,
    #[cfg(feature = "slow-log")]
    detail: Detail,
}

#[cfg(feature = "slow-log")]
enum Detail {
    Query(Box<str>),
    Transaction,
}

impl Observation {
    pub(crate) fn query(metric: Option<MetricHandle>, _sql: &str) -> Self {
        Self {
            metric,
            started: Instant::now(),
            finished: false,
            #[cfg(feature = "slow-log")]
            detail: Detail::Query(truncate_detail(_sql.as_bytes())),
        }
    }

    pub(crate) fn transaction(metric: MetricHandle) -> Self {
        Self {
            metric: Some(metric),
            started: Instant::now(),
            finished: false,
            #[cfg(feature = "slow-log")]
            detail: Detail::Transaction,
        }
    }

    pub(crate) fn finish(mut self, success: bool) {
        let elapsed = self.started.elapsed();
        if let Some(metric) = self.metric.take() {
            metric.record(elapsed, success);
        }
        self.log_slow(elapsed, success);
        self.finished = true;
    }

    #[cfg(feature = "slow-log")]
    fn log_slow(&self, elapsed: std::time::Duration, success: bool) {
        if elapsed < std::time::Duration::from_secs(1) {
            return;
        }
        tracing::warn!(
            target: "breeze.slow",
            "{}",
            SlowLogLine {
                detail: &self.detail,
                elapsed_ms: elapsed.as_millis(),
                success,
            },
        );
    }

    #[cfg(not(feature = "slow-log"))]
    fn log_slow(&self, _elapsed: std::time::Duration, _success: bool) {}
}

#[cfg(feature = "slow-log")]
fn truncate_detail(bytes: &[u8]) -> Box<str> {
    const MAX_DETAIL_BYTES: usize = 2 * 1024;
    String::from_utf8_lossy(&bytes[..bytes.len().min(MAX_DETAIL_BYTES)]).into()
}

#[cfg(feature = "slow-log")]
struct SlowLogLine<'a> {
    detail: &'a Detail,
    elapsed_ms: u128,
    success: bool,
}

#[cfg(feature = "slow-log")]
impl std::fmt::Display for SlowLogLine<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.detail {
            Detail::Query(sql) => write!(
                formatter,
                "mysql query {}ms {} {}",
                self.elapsed_ms, self.success, sql,
            ),
            Detail::Transaction => write!(
                formatter,
                "mysql transaction {}ms {} -",
                self.elapsed_ms, self.success,
            ),
        }
    }
}

impl Drop for Observation {
    fn drop(&mut self) {
        if !self.finished {
            let elapsed = self.started.elapsed();
            if let Some(metric) = self.metric {
                metric.record(elapsed, false);
            }
            self.log_slow(elapsed, false);
        }
    }
}

#[cfg(all(test, feature = "slow-log"))]
mod slow_log_tests {
    use super::*;

    #[test]
    fn slow_mysql_lines_are_positional_and_keep_sql_last() {
        let query = Detail::Query("SELECT SLEEP(1)".into());
        assert_eq!(
            SlowLogLine {
                detail: &query,
                elapsed_ms: 1200,
                success: true,
            }
            .to_string(),
            "mysql query 1200ms true SELECT SLEEP(1)",
        );
        assert_eq!(
            SlowLogLine {
                detail: &Detail::Transaction,
                elapsed_ms: 3200,
                success: false,
            }
            .to_string(),
            "mysql transaction 3200ms false -",
        );
    }
}
