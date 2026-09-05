use brz_mysql::{FromMysqlRow, MysqlService, MysqlServiceOptions};
use std::{
    env,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

#[derive(FromMysqlRow)]
struct One {
    one: i32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = required("--url")?;
    let operations = parsed("--ops", 100_000_usize)?;
    let concurrency = parsed("--concurrency", 64_usize)?;
    let max_connections = parsed("--connections", 16_u32)?;
    let options = MysqlServiceOptions {
        max_connections,
        min_connections: 0,
        acquire_timeout: Duration::from_secs(5),
        idle_timeout: Some(Duration::from_secs(60)),
        max_lifetime: Some(Duration::from_secs(300)),
        slow_acquire_threshold: Duration::from_millis(500),
        test_before_acquire: true,
        charset: "utf8mb4".to_string(),
        timezone: None,
    };
    let service = Arc::new(MysqlService::connect_with_options(&url, options).await?);

    let permits = Arc::new(Semaphore::new(concurrency));
    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..operations {
        let permit = permits.clone().acquire_owned().await?;
        let service = service.clone();
        tasks.spawn(async move {
            let started = Instant::now();
            let result = service.fetch_one::<_, _, One>("SELECT 1 AS one", ()).await;
            drop(permit);
            let result = result.map(|row| row.one);
            (started.elapsed(), result)
        });
    }
    let mut latencies = Vec::with_capacity(operations);
    let mut errors = 0_usize;
    while let Some(result) = tasks.join_next().await {
        let (latency, query) = result?;
        latencies.push(latency);
        errors += usize::from(query.is_err());
    }
    let elapsed = started.elapsed();
    latencies.sort_unstable();
    println!(
        "ops={} errors={} concurrency={} connections={} elapsed_s={:.3} throughput_ops_s={:.1} p50_us={} p95_us={} p99_us={} max_us={}",
        operations,
        errors,
        concurrency,
        max_connections,
        elapsed.as_secs_f64(),
        operations as f64 / elapsed.as_secs_f64(),
        percentile(&latencies, 50).as_micros(),
        percentile(&latencies, 95).as_micros(),
        percentile(&latencies, 99).as_micros(),
        latencies.last().copied().unwrap_or_default().as_micros(),
    );
    service.close().await;
    if errors > 0 {
        return Err(format!("{errors} operations failed").into());
    }
    Ok(())
}

fn percentile(samples: &[Duration], percentile: usize) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }
    samples[((samples.len() - 1) * percentile) / 100]
}

fn required(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    optional(name).ok_or_else(|| format!("missing required argument {name}").into())
}

fn parsed<T>(name: &str, default: T) -> Result<T, Box<dyn std::error::Error>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + 'static,
{
    match optional(name) {
        Some(value) => Ok(value.parse()?),
        None => Ok(default),
    }
}

fn optional(name: &str) -> Option<String> {
    let mut arguments = env::args().skip(1);
    while let Some(argument) = arguments.next() {
        if argument == name {
            return arguments.next();
        }
    }
    None
}
