use std::env;
use std::ops::Mul;
use std::time::Instant;

use polars::prelude::*;
use polars_json::ndjson::deserialize::deserialize_iter;

#[derive(Clone, Copy)]
enum Query {
    ProjectionOnly,
    FilterSelect,
    CompoundFilter,
    DerivedProjection,
    GroupedAggregation,
    MultiAggregation,
    StringFilter,
    LimitPushdown,
    SortedTopK,
    WindowAggregation,
    SelfJoin,
    UniqueTenant,
    FullAggregation,
    JSONDecode,
}

impl Query {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "projection_only" => Ok(Self::ProjectionOnly),
            "filter_select" => Ok(Self::FilterSelect),
            "compound_filter" => Ok(Self::CompoundFilter),
            "derived_projection" => Ok(Self::DerivedProjection),
            "grouped_aggregation" => Ok(Self::GroupedAggregation),
            "multi_aggregation" => Ok(Self::MultiAggregation),
            "string_filter" => Ok(Self::StringFilter),
            "limit_pushdown" => Ok(Self::LimitPushdown),
            "sorted_topk" => Ok(Self::SortedTopK),
            "window_aggregation" => Ok(Self::WindowAggregation),
            "self_join" => Ok(Self::SelfJoin),
            "unique_tenant" => Ok(Self::UniqueTenant),
            "full_aggregation" => Ok(Self::FullAggregation),
            "json_decode" => Ok(Self::JSONDecode),
            _ => Err(format!("unknown query {value:?}")),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::ProjectionOnly => "projection_only",
            Self::FilterSelect => "filter_select",
            Self::CompoundFilter => "compound_filter",
            Self::DerivedProjection => "derived_projection",
            Self::GroupedAggregation => "grouped_aggregation",
            Self::MultiAggregation => "multi_aggregation",
            Self::StringFilter => "string_filter",
            Self::LimitPushdown => "limit_pushdown",
            Self::SortedTopK => "sorted_topk",
            Self::WindowAggregation => "window_aggregation",
            Self::SelfJoin => "self_join",
            Self::UniqueTenant => "unique_tenant",
            Self::FullAggregation => "full_aggregation",
            Self::JSONDecode => "json_decode",
        }
    }
}

fn scan(path: &str) -> PolarsResult<LazyFrame> {
    LazyFrame::scan_parquet(PlPath::new(path), ScanArgsParquet::default())
}

fn execute(path: &str, query: Query, cutoff: i64) -> PolarsResult<DataFrame> {
    let filtered = scan(path)?.filter(col("id").gt_eq(lit(cutoff)));
    match query {
        Query::ProjectionOnly => scan(path)?
            .select([col("group_id"), col("value"), col("payload")])
            .collect(),
        Query::FilterSelect => filtered.select([col("group_id"), col("value")]).collect(),
        Query::CompoundFilter => filtered
            .filter(col("tenant").eq(lit("tenant-07")))
            .select([col("tenant"), col("group_id"), col("value")])
            .collect(),
        Query::DerivedProjection => filtered
            .with_columns([
                col("value")
                    .mul(col("quantity").cast(DataType::Float64))
                    .alias("revenue"),
                col("payload").str().len_chars().alias("payload_len"),
            ])
            .select([col("tenant"), col("revenue"), col("payload_len")])
            .collect(),
        Query::GroupedAggregation => filtered
            .group_by([col("group_id")])
            .agg([col("value").sum().alias("total")])
            .collect(),
        Query::MultiAggregation => filtered
            .group_by([col("tenant")])
            .agg([
                col("value").sum().alias("value_sum"),
                col("value").mean().alias("value_mean"),
                col("value").count().alias("value_count"),
            ])
            .collect(),
        Query::StringFilter => scan(path)?
            .filter(col("payload").str().starts_with(lit("payload-00")))
            .select([col("tenant"), col("payload"), col("value")])
            .collect(),
        Query::LimitPushdown => filtered
            .select([col("group_id"), col("value")])
            .limit(1_000)
            .collect(),
        Query::SortedTopK => filtered
            .select([col("id"), col("tenant"), col("value")])
            .sort(
                ["value"],
                SortMultipleOptions::new().with_order_descending(true),
            )
            .limit(100)
            .collect(),
        Query::WindowAggregation => filtered
            .with_columns([col("value")
                .sum()
                .over([col("tenant")])
                .alias("tenant_total")])
            .select([col("tenant"), col("value"), col("tenant_total")])
            .collect(),
        Query::SelfJoin => {
            let left = scan(path)?
                .filter(col("id").lt(lit(cutoff)))
                .select([col("tenant"), col("group_id"), col("value")])
                .limit(2_000);
            let right = scan(path)?
                .filter(col("id").gt_eq(lit(cutoff)))
                .select([col("tenant"), col("group_id"), col("quantity")])
                .limit(2_000);
            left.join(
                right,
                [col("tenant")],
                [col("tenant")],
                JoinArgs::new(JoinType::Inner).with_coalesce(JoinCoalesce::KeepColumns),
            )
            .collect()
        }
        Query::UniqueTenant => scan(path)?
            .select([col("tenant"), col("group_id"), col("value")])
            .unique_stable(col("tenant").into_selector(), UniqueKeepStrategy::First)
            .collect(),
        Query::FullAggregation => scan(path)?
            .group_by([col("tenant")])
            .agg([
                col("value").sum().alias("value_sum"),
                col("value").mean().alias("value_mean"),
                col("value").count().alias("value_count"),
            ])
            .collect(),
        Query::JSONDecode => {
            let schema = DataType::Struct(vec![
                Field::new("id".into(), DataType::Int64),
                Field::new("tenant".into(), DataType::String),
                Field::new("amount".into(), DataType::Float64),
                Field::new("tags".into(), DataType::List(DataType::Int64.boxed())),
            ]);
            scan(path)?
                .select([json_decode_expr(col("json_payload"), schema)?.alias("decoded")])
                .collect()
        }
    }
}

fn json_decode_expr(input: Expr, schema: DataType) -> PolarsResult<Expr> {
    let output_dtype = schema.clone();
    Ok(input.map(
        move |column| {
            let series = column.as_materialized_series();
            let strings = series.str()?;
            let rows: Vec<&str> = strings.iter().map(|row| row.unwrap_or("null")).collect();
            let buffer_size: usize = rows.iter().map(|row| row.len()).sum();
            let array = deserialize_iter(
                rows.iter().copied(),
                schema.to_arrow(CompatLevel::newest()),
                buffer_size,
                rows.len(),
                true,
            )?;
            Ok(Series::try_from((series.name().clone(), array))?.into_column())
        },
        move |_, field| Ok(Field::new(field.name().clone(), output_dtype.clone())),
    ))
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
    let rss_before = max_rss_bytes();
    let started = Instant::now();
    let mut rows = 0_u64;
    let mut width = 0_usize;
    for _ in 0..iterations {
        let frame = execute(&path, query, cutoff).map_err(|err| err.to_string())?;
        rows += frame.height() as u64;
        if width == 0 {
            width = frame.width();
        } else if width != frame.width() {
            return Err("query returned inconsistent widths".to_string());
        }
    }
    let elapsed_ns = started.elapsed().as_nanos() as u64;
    let avg_ns = elapsed_ns / iterations as u64;
    let max_rss = max_rss_bytes();
    println!(
        "{{\"implementation\":\"polars\",\"query\":\"{}\",\"iterations\":{},\"rows\":{},\"width\":{},\"elapsed_ns\":{},\"avg_ns\":{},\"max_rss_bytes\":{},\"rss_delta_bytes\":{}}}",
        query.name(),
        iterations,
        rows / iterations as u64,
        width,
        elapsed_ns,
        avg_ns,
        max_rss,
        max_rss.saturating_sub(rss_before),
    );
    Ok(())
}
