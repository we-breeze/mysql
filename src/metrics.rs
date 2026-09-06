use std::time::Instant;

use brz_metrics::Metric;

#[derive(Clone, Copy, Debug)]
pub(crate) struct MysqlMetrics {
    pub(crate) get: Metric,
    pub(crate) list: Metric,
    pub(crate) update: Metric,
    pub(crate) transaction: Metric,
}

impl MysqlMetrics {
    pub(crate) fn new(host: &str) -> Self {
        Self {
            get: Metric::mysql(&format!("{host}_get")),
            list: Metric::mysql(&format!("{host}_list")),
            update: Metric::mysql(&format!("{host}_update")),
            transaction: Metric::mysql(&format!("{host}_transaction")),
        }
    }
}

pub(crate) struct Observation {
    metric: Option<Metric>,
    started: Instant,
}

impl Observation {
    pub(crate) fn new(metric: Metric) -> Self {
        Self {
            metric: Some(metric),
            started: Instant::now(),
        }
    }

    pub(crate) fn finish(mut self, success: bool) {
        self.metric
            .take()
            .unwrap()
            .record(self.started.elapsed(), success);
    }
}

impl Drop for Observation {
    fn drop(&mut self) {
        if let Some(metric) = self.metric {
            metric.record(self.started.elapsed(), false);
        }
    }
}
