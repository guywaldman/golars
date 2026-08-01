use std::env;
use std::time::Instant;

use polars::prelude::*;

#[derive(Clone, Copy)]
enum Query {
    FilterSelect,
    GroupedAggregation,
}

impl Query {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "filter_select" => Ok(Self::FilterSelect),
            "grouped_aggregation" => Ok(Self::GroupedAggregation),
            _ => Err(format!("unknown query {value:?}")),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::FilterSelect => "filter_select",
            Self::GroupedAggregation => "grouped_aggregation",
        }
    }
}

fn execute(path: &str, query: Query, cutoff: i64) -> PolarsResult<DataFrame> {
    let scan = LazyFrame::scan_parquet(PlPath::new(path), ScanArgsParquet::default())?
        .filter(col("id").gt_eq(lit(cutoff)));
    match query {
        Query::FilterSelect => scan.select([col("group_id"), col("value")]).collect(),
        Query::GroupedAggregation => scan
            .group_by([col("group_id")])
            .agg([col("value").sum().alias("total")])
            .collect(),
    }
}

fn max_rss_bytes() -> u64 {
    #[cfg(unix)]
    {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
        let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        if result == 0 {
            let usage = unsafe { usage.assume_init() };
            #[cfg(target_os = "macos")]
            return usage.ru_maxrss as u64;
            #[cfg(not(target_os = "macos"))]
            return (usage.ru_maxrss as u64) * 1024;
        }
    }
    0
}

fn parse_value<T: std::str::FromStr>(args: &[String], name: &str, default: T) -> Result<T, String> {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| {
            pair[1]
                .parse()
                .map_err(|_| format!("invalid value for {name}"))
        })
        .unwrap_or(Ok(default))
}

fn required_value(args: &[String], name: &str) -> Result<String, String> {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
        .ok_or_else(|| format!("{name} is required"))
}

fn main() -> Result<(), String> {
    let args: Vec<String> = env::args().collect();
    let path = required_value(&args, "--dataset")?;
    let query = Query::parse(&parse_value(&args, "--query", "filter_select".to_string())?)?;
    let cutoff = parse_value(&args, "--cutoff", 500_000_i64)?;
    let iterations = parse_value(&args, "--iterations", 5_usize)?;
    let warmup = parse_value(&args, "--warmup", 1_usize)?;
    if iterations == 0 {
        return Err("iterations must be positive".to_string());
    }

    for _ in 0..warmup {
        execute(&path, query, cutoff).map_err(|err| err.to_string())?;
    }
    let started = Instant::now();
    let mut rows = 0_u64;
    for _ in 0..iterations {
        let frame = execute(&path, query, cutoff).map_err(|err| err.to_string())?;
        rows += frame.height() as u64;
    }
    let elapsed_ns = started.elapsed().as_nanos() as u64;
    let avg_ns = elapsed_ns / iterations as u64;
    println!(
        "{{\"implementation\":\"polars\",\"query\":\"{}\",\"iterations\":{},\"rows\":{},\"elapsed_ns\":{},\"avg_ns\":{},\"max_rss_bytes\":{}}}",
        query.name(),
        iterations,
        rows / iterations as u64,
        elapsed_ns,
        avg_ns,
        max_rss_bytes(),
    );
    Ok(())
}
