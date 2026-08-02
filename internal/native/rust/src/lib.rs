use std::{fs::File, io::Cursor, panic, slice};

use base64::Engine as _;
use polars::prelude::*;
use polars_json::ndjson::deserialize::deserialize_iter;
use serde::Deserialize;
use serde_json::{Map, Number, Value};

const ABI_VERSION: u32 = 1;

#[unsafe(no_mangle)]
pub extern "C" fn golars_native_abi_version() -> u32 {
    ABI_VERSION
}

#[unsafe(no_mangle)]
pub extern "C" fn golars_native_polars_version(output: *mut u8, capacity: usize) -> usize {
    if output.is_null() || capacity == 0 {
        return polars::VERSION.len();
    }

    let bytes = polars::VERSION.as_bytes();
    let length = bytes.len().min(capacity);
    // The caller owns the output buffer and guarantees that it is writable.
    unsafe {
        slice::from_raw_parts_mut(output, length).copy_from_slice(&bytes[..length]);
    }
    bytes.len()
}

#[unsafe(no_mangle)]
pub extern "C" fn golars_native_probe() -> u64 {
    // Keep a concrete reference to the Polars crate in the exported library.
    DataFrame::empty().height() as u64
}

#[unsafe(no_mangle)]
pub extern "C" fn golars_native_execute_frame(
    request: *const u8,
    request_length: usize,
    frame: *mut usize,
    error: *mut *mut u8,
    error_length: *mut usize,
) -> i32 {
    if request.is_null() || frame.is_null() {
        write_error("invalid native backend request", error, error_length);
        return 1;
    }
    let request = unsafe { slice::from_raw_parts(request, request_length) };
    let result = panic::catch_unwind(|| collect_query(request));
    match result {
        Ok(Ok(dataframe)) => {
            unsafe {
                *frame = Box::into_raw(Box::new(dataframe)) as usize;
            }
            0
        }
        Ok(Err(message)) => {
            write_error(&message, error, error_length);
            1
        }
        Err(_) => {
            write_error("native backend panicked", error, error_length);
            2
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn golars_native_dataframe_free(frame: usize) {
    if frame == 0 {
        return;
    }
    unsafe {
        drop(Box::from_raw(frame as *mut DataFrame));
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn golars_native_dataframe_height(frame: usize) -> usize {
    if frame == 0 {
        return 0;
    }
    unsafe { (*(frame as *const DataFrame)).height() }
}

#[unsafe(no_mangle)]
pub extern "C" fn golars_native_dataframe_width(frame: usize) -> usize {
    if frame == 0 {
        return 0;
    }
    unsafe { (*(frame as *const DataFrame)).width() }
}

#[unsafe(no_mangle)]
pub extern "C" fn golars_native_dataframe_ipc(
    frame: usize,
    output: *mut *mut u8,
    output_length: *mut usize,
    error: *mut *mut u8,
    error_length: *mut usize,
) -> i32 {
    dataframe_ipc(frame, output, output_length, error, error_length, false)
}

#[unsafe(no_mangle)]
pub extern "C" fn golars_native_dataframe_schema_ipc(
    frame: usize,
    output: *mut *mut u8,
    output_length: *mut usize,
    error: *mut *mut u8,
    error_length: *mut usize,
) -> i32 {
    dataframe_ipc(frame, output, output_length, error, error_length, true)
}

#[unsafe(no_mangle)]
pub extern "C" fn golars_native_free(buffer: *mut u8, length: usize) {
    if buffer.is_null() {
        return;
    }
    // The buffer was allocated by this library and its length is returned to
    // the caller alongside the pointer.
    unsafe {
        drop(Box::from_raw(slice::from_raw_parts_mut(buffer, length)));
    }
}

#[derive(Deserialize)]
struct QueryRequest {
    #[serde(default)]
    path: String,
    format: String,
    #[serde(default)]
    input: String,
    #[serde(default)]
    has_header: bool,
    #[serde(default)]
    delimiter: String,
    #[serde(default)]
    operations: Vec<QueryOperation>,
}

#[derive(Deserialize)]
struct QueryOperation {
    kind: String,
    #[serde(default)]
    exprs: Vec<QueryExpr>,
    #[serde(default)]
    keys: Vec<QueryExpr>,
    #[serde(default)]
    right_keys: Vec<QueryExpr>,
    #[serde(default)]
    columns: Vec<String>,
    #[serde(default)]
    join_type: String,
    #[serde(default)]
    suffix: String,
    #[serde(default)]
    right: Option<Box<QueryRequest>>,
    #[serde(default)]
    limit: u64,
}

#[derive(Deserialize)]
struct QueryExpr {
    kind: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    literal: Option<QueryLiteral>,
    #[serde(default)]
    args: Vec<QueryExpr>,
    #[serde(default)]
    window_keys: Vec<QueryExpr>,
    #[serde(default)]
    dtype: String,
    #[serde(default)]
    json_decode: Option<JsonDecodeOptions>,
    #[serde(default)]
    descending: bool,
}

#[derive(Clone, Deserialize)]
struct JsonDecodeOptions {
    schema: JsonType,
    #[serde(default)]
    on_error: String,
    #[serde(default = "default_json_coerce")]
    coerce: bool,
    #[serde(default)]
    strict_required_fields: bool,
}

#[derive(Clone, Deserialize)]
struct JsonField {
    name: String,
    #[serde(rename = "type")]
    dtype: JsonType,
    #[serde(default)]
    nullable: bool,
}

#[derive(Clone, Deserialize)]
struct JsonType {
    kind: String,
    #[serde(default)]
    elem: Option<Box<JsonType>>,
    #[serde(default)]
    fields: Vec<JsonField>,
    #[serde(default)]
    unit: String,
    #[serde(default)]
    timezone: String,
}

fn default_json_coerce() -> bool {
    true
}

#[derive(Deserialize)]
struct QueryLiteral {
    kind: String,
    #[serde(default)]
    value: Value,
}

fn collect_query(request: &[u8]) -> Result<DataFrame, String> {
    let request: QueryRequest = serde_json::from_slice(request)
        .map_err(|error| format!("invalid native backend request: {error}"))?;
    build_query(&request)?
        .collect()
        .map_err(|error| error.to_string())
}

fn build_query(request: &QueryRequest) -> Result<LazyFrame, String> {
    let mut frame = source_frame(request)?;
    for operation in &request.operations {
        frame = match operation.kind.as_str() {
            "filter" if operation.exprs.len() == 1 => frame.filter(to_expr(&operation.exprs[0])?),
            "select" => frame.select(to_exprs(&operation.exprs)?),
            "with_columns" => frame.with_columns(to_exprs(&operation.exprs)?),
            "limit" => frame.limit(operation.limit as IdxSize),
            "tail" => frame.tail(operation.limit as IdxSize),
            "sort" => {
                let expressions = to_exprs(&operation.exprs)?;
                let descending = operation.exprs.iter().map(|expr| expr.descending);
                frame.sort_by_exprs(
                    expressions,
                    SortMultipleOptions::new().with_order_descending_multi(descending),
                )
            }
            "group_by" => {
                let keys = to_exprs(&operation.keys)?;
                let expressions = to_exprs(&operation.exprs)?;
                frame.group_by_stable(keys).agg(expressions)
            }
            "join" => {
                let right = operation
                    .right
                    .as_deref()
                    .ok_or_else(|| "native join is missing its right query".to_owned())?;
                let right = build_query(right)?;
                let join_type = to_join_type(&operation.join_type)?;
                frame
                    .join_builder()
                    .with(right)
                    .left_on(to_exprs(&operation.exprs)?)
                    .right_on(to_exprs(&operation.right_keys)?)
                    .how(join_type)
                    .coalesce(JoinCoalesce::KeepColumns)
                    .maintain_order(MaintainOrderJoin::Left)
                    .suffix(if operation.suffix.is_empty() {
                        "_right"
                    } else {
                        operation.suffix.as_str()
                    })
                    .finish()
            }
            "unique" => {
                let subset =
                    (!operation.columns.is_empty()).then(|| cols(operation.columns.clone()));
                frame.unique_stable(subset, UniqueKeepStrategy::First)
            }
            "explode" => frame.explode(cols(operation.columns.clone())),
            "unnest" => frame.unnest(cols(operation.columns.clone()), None),
            kind => return Err(format!("unsupported native operation {kind:?}")),
        };
    }
    Ok(frame)
}

fn source_frame(request: &QueryRequest) -> Result<LazyFrame, String> {
    if !request.input.is_empty() {
        let input = base64::engine::general_purpose::STANDARD
            .decode(&request.input)
            .map_err(|error| format!("decode native IPC input: {error}"))?;
        let dataframe = IpcReader::new(Cursor::new(input))
            .finish()
            .map_err(|error| format!("read native IPC input: {error}"))?;
        return Ok(dataframe.lazy());
    }
    if request.path.is_empty() {
        return Err("native query has neither a path nor IPC input".to_owned());
    }
    match request.format.as_str() {
        "parquet" => LazyFrame::scan_parquet(
            PlPath::new(request.path.as_str()),
            ScanArgsParquet::default(),
        )
        .map_err(|error| error.to_string()),
        "csv" => {
            let mut reader = LazyCsvReader::new(PlPath::new(request.path.as_str()))
                .with_has_header(request.has_header);
            if let Some(delimiter) = request.delimiter.as_bytes().first().copied() {
                reader = reader.with_separator(delimiter);
            }
            reader.finish().map_err(|error| error.to_string())
        }
        "ipc_file" => LazyFrame::scan_ipc(
            PlPath::new(request.path.as_str()),
            IpcScanOptions::default(),
            UnifiedScanArgs::default(),
        )
        .map_err(|error| error.to_string()),
        "ipc_stream" => {
            let dataframe = IpcStreamReader::new(
                File::open(request.path.as_str()).map_err(|error| error.to_string())?,
            )
            .finish()
            .map_err(|error| error.to_string())?;
            Ok(dataframe.lazy())
        }
        format => Err(format!("unsupported native format {format:?}")),
    }
}

fn dataframe_ipc(
    frame: usize,
    output: *mut *mut u8,
    output_length: *mut usize,
    error: *mut *mut u8,
    error_length: *mut usize,
    schema_only: bool,
) -> i32 {
    if frame == 0 || output.is_null() || output_length.is_null() {
        write_error("invalid native dataframe arguments", error, error_length);
        return 1;
    }
    let result = panic::catch_unwind(|| {
        let dataframe = unsafe { &*(frame as *const DataFrame) };
        let mut view = if schema_only {
            dataframe.slice(0, 0)
        } else {
            dataframe.clone()
        };
        dataframe_ipc_bytes(&mut view)
    });
    match result {
        Ok(Ok(data)) => {
            write_output(data, output, output_length);
            0
        }
        Ok(Err(message)) => {
            write_error(&message, error, error_length);
            1
        }
        Err(_) => {
            write_error("native backend panicked", error, error_length);
            2
        }
    }
}

fn dataframe_ipc_bytes(dataframe: &mut DataFrame) -> Result<Vec<u8>, String> {
    let mut output = Vec::new();
    IpcWriter::new(&mut output)
        .with_compat_level(CompatLevel::oldest())
        .finish(dataframe)
        .map_err(|error| error.to_string())?;
    Ok(output)
}

fn to_exprs(expressions: &[QueryExpr]) -> Result<Vec<Expr>, String> {
    expressions.iter().map(to_expr).collect()
}

fn to_expr(expression: &QueryExpr) -> Result<Expr, String> {
    let mut result = match expression.kind.as_str() {
        "column" => Ok(col(expression.name.as_str())),
        "literal" => to_literal(expression.literal.as_ref()),
        "alias" => {
            let value = one_arg(expression)?.alias(expression.name.as_str());
            Ok(value)
        }
        "cast" => Ok(one_arg(expression)?.cast(to_dtype(&expression.dtype)?)),
        "aggregate" => to_aggregate(expression),
        "call" => to_call(expression),
        kind => Err(format!("unsupported native expression kind {kind:?}")),
    }?;
    if expression.kind == "aggregate" {
        let name = if expression.name == "count_rows" {
            "count".to_owned()
        } else if let Some(argument) = expression.args.first() {
            format!("{}_{}", query_expr_output_name(argument), expression.name)
        } else {
            expression.name.clone()
        };
        result = result.alias(name);
    }
    if !expression.window_keys.is_empty() {
        result = result.over(to_exprs(&expression.window_keys)?);
    }
    Ok(result)
}

fn query_expr_output_name(expression: &QueryExpr) -> String {
    match expression.kind.as_str() {
        "column" | "alias" => expression.name.clone(),
        "cast" | "call" => expression
            .args
            .first()
            .map(query_expr_output_name)
            .unwrap_or_else(|| "literal".to_owned()),
        "aggregate" => {
            if expression.name == "count_rows" {
                "count".to_owned()
            } else {
                format!(
                    "{}_{}",
                    expression
                        .args
                        .first()
                        .map(query_expr_output_name)
                        .unwrap_or_else(|| "literal".to_owned()),
                    expression.name
                )
            }
        }
        _ => "literal".to_owned(),
    }
}

fn to_literal(literal: Option<&QueryLiteral>) -> Result<Expr, String> {
    let literal = literal.ok_or_else(|| "native literal is missing its value".to_owned())?;
    match literal.kind.as_str() {
        "null" => Ok(lit(NULL)),
        "bool" => literal
            .value
            .as_bool()
            .map(lit)
            .ok_or_else(|| "native bool literal is invalid".to_owned()),
        "string" => literal
            .value
            .as_str()
            .map(lit)
            .ok_or_else(|| "native string literal is invalid".to_owned()),
        "int" => literal
            .value
            .as_i64()
            .map(lit)
            .ok_or_else(|| "native int literal is invalid".to_owned()),
        "uint" => literal
            .value
            .as_u64()
            .map(lit)
            .ok_or_else(|| "native uint literal is invalid".to_owned()),
        "float" => literal
            .value
            .as_f64()
            .map(lit)
            .ok_or_else(|| "native float literal is invalid".to_owned()),
        "timestamp" => literal
            .value
            .as_i64()
            .map(|value| lit(value).cast(DataType::Datetime(TimeUnit::Nanoseconds, None)))
            .ok_or_else(|| "native timestamp literal is invalid".to_owned()),
        "list" => to_list_literal(&literal.value),
        kind => Err(format!("unsupported native literal kind {kind:?}")),
    }
}

fn to_list_literal(value: &Value) -> Result<Expr, String> {
    let values = value
        .as_array()
        .ok_or_else(|| "native list literal is invalid".to_owned())?;
    if values.iter().all(Value::is_i64) {
        let values = values
            .iter()
            .map(|value| value.as_i64().unwrap())
            .collect::<Vec<_>>();
        return Ok(lit(Series::new("literal".into(), values)));
    }
    if values.iter().all(Value::is_u64) {
        let values = values
            .iter()
            .map(|value| value.as_u64().unwrap())
            .collect::<Vec<_>>();
        return Ok(lit(Series::new("literal".into(), values)));
    }
    if values.iter().all(Value::is_f64) {
        let values = values
            .iter()
            .map(|value| value.as_f64().unwrap())
            .collect::<Vec<_>>();
        return Ok(lit(Series::new("literal".into(), values)));
    }
    if values.iter().all(Value::is_string) {
        let values = values
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect::<Vec<_>>();
        return Ok(lit(Series::new("literal".into(), values)));
    }
    Err("native list literal has mixed or unsupported values".to_owned())
}

fn one_arg(expression: &QueryExpr) -> Result<Expr, String> {
    if expression.args.len() != 1 {
        return Err(format!(
            "native expression {:?} requires one argument",
            expression.kind
        ));
    }
    to_expr(&expression.args[0])
}

fn two_args(expression: &QueryExpr) -> Result<(Expr, Expr), String> {
    if expression.args.len() != 2 {
        return Err(format!(
            "native expression {:?} requires two arguments",
            expression.name
        ));
    }
    Ok((to_expr(&expression.args[0])?, to_expr(&expression.args[1])?))
}

fn to_aggregate(expression: &QueryExpr) -> Result<Expr, String> {
    match expression.name.as_str() {
        "count_rows" => Ok(len().cast(DataType::Int64)),
        "sum" => Ok(one_arg(expression)?.sum()),
        "mean" => Ok(one_arg(expression)?.mean()),
        "min" => Ok(one_arg(expression)?.min()),
        "max" => Ok(one_arg(expression)?.max()),
        "count" => Ok(one_arg(expression)?.count().cast(DataType::Int64)),
        "first" => Ok(one_arg(expression)?.first()),
        "last" => Ok(one_arg(expression)?.last()),
        name => Err(format!("unsupported native aggregate {name:?}")),
    }
}

fn to_call(expression: &QueryExpr) -> Result<Expr, String> {
    if expression.name == "all_columns" {
        return Ok(all().as_expr());
    }
    match expression.name.as_str() {
        "json_decode" => {
            let options = expression
                .json_decode
                .as_ref()
                .ok_or_else(|| "native JSON decode options are missing".to_owned())?;
            json_decode_expr(one_arg(expression)?, options)
        }
        "equal" => {
            let (left, right) = two_args(expression)?;
            Ok(left.eq(right))
        }
        "not_equal" => {
            let (left, right) = two_args(expression)?;
            Ok(left.neq(right))
        }
        "greater" => {
            let (left, right) = two_args(expression)?;
            Ok(left.gt(right))
        }
        "greater_equal" => {
            let (left, right) = two_args(expression)?;
            Ok(left.gt_eq(right))
        }
        "less" => {
            let (left, right) = two_args(expression)?;
            Ok(left.lt(right))
        }
        "less_equal" => {
            let (left, right) = two_args(expression)?;
            Ok(left.lt_eq(right))
        }
        "add" => {
            let (left, right) = two_args(expression)?;
            Ok(left + right)
        }
        "subtract" => {
            let (left, right) = two_args(expression)?;
            Ok(left - right)
        }
        "multiply" => {
            let (left, right) = two_args(expression)?;
            Ok(left * right)
        }
        "divide" => {
            let (left, right) = two_args(expression)?;
            Ok(left / right)
        }
        "modulo" => {
            let (left, right) = two_args(expression)?;
            Ok(left % right)
        }
        "and_kleene" => {
            let (left, right) = two_args(expression)?;
            Ok(left.and(right))
        }
        "or_kleene" => {
            let (left, right) = two_args(expression)?;
            Ok(left.or(right))
        }
        "invert" => Ok(one_arg(expression)?.not()),
        "is_null" => Ok(one_arg(expression)?.is_null()),
        "is_valid" => Ok(one_arg(expression)?.is_not_null()),
        "str_contains" => {
            let (left, right) = two_args(expression)?;
            Ok(left.str().contains(right, false))
        }
        "str_starts_with" => {
            let (left, right) = two_args(expression)?;
            Ok(left.str().starts_with(right))
        }
        "str_ends_with" => {
            let (left, right) = two_args(expression)?;
            Ok(left.str().ends_with(right))
        }
        "str_lower" => Ok(one_arg(expression)?.str().to_lowercase()),
        "str_upper" => Ok(one_arg(expression)?.str().to_uppercase()),
        "str_length" => Ok(one_arg(expression)?.str().len_chars().cast(DataType::Int64)),
        "coalesce" => {
            let (left, right) = two_args(expression)?;
            Ok(left.fill_null(right))
        }
        "when_then" => {
            if expression.args.len() != 3 {
                return Err("native when_then requires three arguments".to_owned());
            }
            Ok(when(to_expr(&expression.args[0])?)
                .then(to_expr(&expression.args[1])?)
                .otherwise(to_expr(&expression.args[2])?))
        }
        "is_in" => {
            let (left, right) = two_args(expression)?;
            Ok(left.is_in(right, false))
        }
        "is_between" => {
            if expression.args.len() != 3 {
                return Err("native is_between requires three arguments".to_owned());
            }
            Ok(to_expr(&expression.args[0])?.is_between(
                to_expr(&expression.args[1])?,
                to_expr(&expression.args[2])?,
                ClosedInterval::Both,
            ))
        }
        "rank" => Ok(one_arg(expression)?
            .rank(
                RankOptions {
                    method: RankMethod::Dense,
                    descending: false,
                },
                None,
            )
            .cast(DataType::Int64)),
        "dense_rank" => Ok(one_arg(expression)?
            .rank(
                RankOptions {
                    method: RankMethod::Dense,
                    descending: false,
                },
                None,
            )
            .cast(DataType::Int64)),
        "row_number" => Ok(len().cast(DataType::Int64)),
        "cum_sum" => Ok(one_arg(expression)?.cum_sum(false)),
        "dt_year" => Ok(one_arg(expression)?.dt().year()),
        "dt_month" => Ok(one_arg(expression)?.dt().month()),
        "dt_day" => Ok(one_arg(expression)?.dt().day()),
        "is_nan" => Ok(one_arg(expression)?.is_nan()),
        "is_not_nan" => Ok(one_arg(expression)?.is_not_nan()),
        "is_finite" => Ok(one_arg(expression)?.is_finite()),
        "is_infinite" => Ok(one_arg(expression)?.is_infinite()),
        name => Err(format!("unsupported native call {name:?}")),
    }
}

fn json_decode_expr(input: Expr, options: &JsonDecodeOptions) -> Result<Expr, String> {
    let dtype = json_dtype(&options.schema)?;
    let output_dtype = dtype.clone();
    let options = options.clone();
    Ok(input.map(
        move |column| decode_json_column(column, dtype.clone(), options.clone()),
        move |_, field| Ok(Field::new(field.name().clone(), output_dtype.clone())),
    ))
}

fn json_dtype(schema: &JsonType) -> Result<DataType, String> {
    match schema.kind.as_str() {
        "null" => Ok(DataType::Null),
        "bool" => Ok(DataType::Boolean),
        "int8" => Ok(DataType::Int8),
        "int16" => Ok(DataType::Int16),
        "int32" => Ok(DataType::Int32),
        "int64" => Ok(DataType::Int64),
        "uint8" => Ok(DataType::UInt8),
        "uint16" => Ok(DataType::UInt16),
        "uint32" => Ok(DataType::UInt32),
        "uint64" => Ok(DataType::UInt64),
        "float32" => Ok(DataType::Float32),
        "float64" => Ok(DataType::Float64),
        "string" | "large_string" => Ok(DataType::String),
        "binary" | "large_binary" => Ok(DataType::Binary),
        "date32" | "date64" => Ok(DataType::Date),
        "timestamp" => {
            let timezone = if schema.timezone.is_empty() {
                None
            } else {
                TimeZone::opt_try_new(Some(schema.timezone.clone()))
                    .map_err(|error| error.to_string())?
            };
            Ok(DataType::Datetime(json_time_unit(&schema.unit)?, timezone))
        }
        "duration" => Ok(DataType::Duration(json_time_unit(&schema.unit)?)),
        "list" => Ok(DataType::List(
            json_dtype(
                schema
                    .elem
                    .as_deref()
                    .ok_or_else(|| "JSON list schema is missing its element type".to_owned())?,
            )?
            .boxed(),
        )),
        "struct" => Ok(DataType::Struct(
            schema
                .fields
                .iter()
                .map(|field| {
                    Ok(Field::new(
                        field.name.clone().into(),
                        json_dtype(&field.dtype)?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?,
        )),
        kind => Err(format!("unsupported JSON decode schema kind {kind:?}")),
    }
}

fn json_time_unit(unit: &str) -> Result<TimeUnit, String> {
    match unit {
        // Polars stores temporal values at millisecond precision or finer.
        // A seconds schema is represented at millisecond precision by the
        // native execution layer.
        "s" => Ok(TimeUnit::Milliseconds),
        "ms" => Ok(TimeUnit::Milliseconds),
        "us" => Ok(TimeUnit::Microseconds),
        "ns" | "" => Ok(TimeUnit::Nanoseconds),
        other => Err(format!("unsupported JSON time unit {other:?}")),
    }
}

fn decode_json_column(
    column: Column,
    dtype: DataType,
    options: JsonDecodeOptions,
) -> PolarsResult<Column> {
    let series = column.as_materialized_series();
    let strings = series.str()?;
    let rows: Vec<&str> = strings.iter().map(|row| row.unwrap_or("null")).collect();
    let arrow_dtype = dtype.to_arrow(CompatLevel::newest());
    let buffer_size: usize = rows.iter().map(|row| row.len()).sum();

    let mut normalized = None;
    if options.strict_required_fields || !options.coerce {
        normalized = Some(normalize_json_rows(&rows, &options.schema, &options)?);
    }
    let input_rows = normalized
        .as_ref()
        .map(|rows| rows.iter().map(String::as_str).collect::<Vec<_>>());
    let fast_rows = input_rows.as_deref().unwrap_or(&rows);
    let fast_buffer_size = input_rows
        .as_ref()
        .map(|rows| rows.iter().map(|row| row.len()).sum())
        .unwrap_or(buffer_size);

    match deserialize_iter(
        fast_rows.iter().copied(),
        arrow_dtype.clone(),
        fast_buffer_size,
        fast_rows.len(),
        true,
    ) {
        Ok(array) => Ok(Series::try_from((series.name().clone(), array))?.into_column()),
        Err(_error) => {
            let normalized = normalize_parse_errors(
                fast_rows,
                &arrow_dtype,
                fast_buffer_size,
                &options.schema,
                &options,
            )?;
            let array = deserialize_iter(
                normalized.iter().map(String::as_str),
                arrow_dtype,
                normalized.iter().map(String::len).sum(),
                normalized.len(),
                true,
            )?;
            Ok(Series::try_from((series.name().clone(), array))?.into_column())
        }
    }
}

fn normalize_parse_errors(
    rows: &[&str],
    dtype: &polars::prelude::ArrowDataType,
    buffer_size: usize,
    schema: &JsonType,
    options: &JsonDecodeOptions,
) -> PolarsResult<Vec<String>> {
    let mut normalized = Vec::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        match deserialize_iter(
            std::iter::once(*row),
            dtype.clone(),
            buffer_size.min(row.len()),
            1,
            true,
        ) {
            Ok(_) => normalized.push((*row).to_owned()),
            Err(error) => {
                let replacement = match normalize_json_row(row, schema, options) {
                    Ok(Some(value)) => {
                        let value_buffer = value.len();
                        match deserialize_iter(
                            std::iter::once(value.as_str()),
                            dtype.clone(),
                            value_buffer,
                            1,
                            true,
                        ) {
                            Ok(_) => value,
                            Err(error) if options.on_error == "error" => {
                                return Err(
                                    polars_err!(ComputeError: "JSON decode failed at row {}: {}", index, error),
                                );
                            }
                            Err(_) => "null".to_owned(),
                        }
                    }
                    Ok(None) if options.on_error == "error" => {
                        return Err(
                            polars_err!(ComputeError: "JSON decode failed at row {}: {}", index, error),
                        );
                    }
                    Ok(None) => "null".to_owned(),
                    Err(message) if options.on_error == "error" => {
                        return Err(
                            polars_err!(ComputeError: "JSON decode failed at row {}: {}", index, message),
                        );
                    }
                    Err(_) => "null".to_owned(),
                };
                normalized.push(replacement)
            }
        }
    }
    Ok(normalized)
}

fn normalize_json_rows(
    rows: &[&str],
    schema: &JsonType,
    options: &JsonDecodeOptions,
) -> PolarsResult<Vec<String>> {
    let mut normalized = Vec::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        match normalize_json_row(row, schema, options) {
            Ok(Some(value)) => normalized.push(value),
            Ok(None) if options.on_error != "error" => normalized.push("null".to_owned()),
            Ok(None) => {
                return Err(
                    polars_err!(ComputeError: "JSON decode failed at row {}: top-level value does not match the schema", index),
                );
            }
            Err(_message) if options.on_error != "error" => normalized.push("null".to_owned()),
            Err(message) => {
                return Err(
                    polars_err!(ComputeError: "JSON decode failed at row {}: {}", index, message),
                );
            }
        }
    }
    Ok(normalized)
}

fn normalize_json_row(
    row: &str,
    schema: &JsonType,
    options: &JsonDecodeOptions,
) -> Result<Option<String>, String> {
    let value: Value = serde_json::from_str(row).map_err(|error| error.to_string())?;
    let Some(value) = normalize_json_value(
        &value,
        schema,
        options.coerce,
        options.strict_required_fields,
        true,
    )?
    else {
        return Ok(None);
    };
    serde_json::to_string(&value)
        .map(Some)
        .map_err(|error| error.to_string())
}

fn normalize_json_value(
    value: &Value,
    schema: &JsonType,
    coerce: bool,
    strict_required: bool,
    nullable: bool,
) -> Result<Option<Value>, String> {
    if value.is_null() {
        return if nullable || !strict_required {
            Ok(Some(Value::Null))
        } else {
            Err("required value is null".to_owned())
        };
    }
    match schema.kind.as_str() {
        "struct" => {
            let Some(object) = value.as_object() else {
                return if nullable || !strict_required {
                    Ok(None)
                } else {
                    Err("required value is not an object".to_owned())
                };
            };
            let mut output = Map::with_capacity(schema.fields.len());
            for field in &schema.fields {
                match object.get(&field.name) {
                    Some(value) => match normalize_json_value(
                        value,
                        &field.dtype,
                        coerce,
                        strict_required,
                        field.nullable,
                    )? {
                        Some(value) => {
                            output.insert(field.name.clone(), value);
                        }
                        None if strict_required && !field.nullable => {
                            return Err(format!("required field {:?} is invalid", field.name));
                        }
                        None => {
                            output.insert(field.name.clone(), Value::Null);
                        }
                    },
                    None if strict_required && !field.nullable => {
                        return Err(format!("required field {:?} is missing", field.name));
                    }
                    None => {}
                }
            }
            Ok(Some(Value::Object(output)))
        }
        "list" => {
            let Some(values) = value.as_array() else {
                return if nullable || !strict_required {
                    Ok(None)
                } else {
                    Err("required value is not an array".to_owned())
                };
            };
            let element = schema
                .elem
                .as_deref()
                .ok_or_else(|| "JSON list schema is missing its element type".to_owned())?;
            let mut output = Vec::with_capacity(values.len());
            for value in values {
                match normalize_json_value(value, element, coerce, strict_required, true)? {
                    Some(value) => output.push(value),
                    None => output.push(Value::Null),
                }
            }
            Ok(Some(Value::Array(output)))
        }
        _ => match coerce_scalar(value, schema, coerce) {
            Some(value) => Ok(Some(value)),
            None if nullable || !strict_required => Ok(None),
            None => Err(format!("required value does not match {}", schema.kind)),
        },
    }
}

fn coerce_scalar(value: &Value, schema: &JsonType, coerce: bool) -> Option<Value> {
    match schema.kind.as_str() {
        "bool" => {
            if let Some(value) = value.as_bool() {
                Some(Value::Bool(value))
            } else if coerce {
                value
                    .as_str()
                    .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
                        "true" => Some(Value::Bool(true)),
                        "false" => Some(Value::Bool(false)),
                        _ => None,
                    })
            } else {
                None
            }
        }
        "int8" | "int16" | "int32" | "int64" => json_integer(value, coerce).and_then(|value| {
            let valid = match schema.kind.as_str() {
                "int8" => i8::try_from(value).is_ok(),
                "int16" => i16::try_from(value).is_ok(),
                "int32" => i32::try_from(value).is_ok(),
                _ => i64::try_from(value).is_ok(),
            };
            valid.then(|| Value::Number(Number::from(value as i64)))
        }),
        "uint8" | "uint16" | "uint32" | "uint64" => json_integer(value, coerce).and_then(|value| {
            let valid = value >= 0
                && match schema.kind.as_str() {
                    "uint8" => u8::try_from(value).is_ok(),
                    "uint16" => u16::try_from(value).is_ok(),
                    "uint32" => u32::try_from(value).is_ok(),
                    _ => u64::try_from(value).is_ok(),
                };
            valid.then(|| Value::Number(Number::from(value as u64)))
        }),
        "float32" | "float64" => json_float(value, coerce)
            .and_then(Number::from_f64)
            .map(Value::Number),
        "string" | "large_string" => match value {
            Value::String(value) => Some(Value::String(value.clone())),
            _ if coerce => Some(Value::String(value.to_string())),
            _ => None,
        },
        "binary" | "large_binary" => value.as_str().map(|value| Value::String(value.to_owned())),
        "date32" | "date64" | "timestamp" => value
            .as_str()
            .map(|value| Value::String(value.to_owned()))
            .or_else(|| coerce.then(|| value.clone()).filter(Value::is_number)),
        "duration" => json_integer(value, coerce)
            .and_then(|value| i64::try_from(value).ok())
            .map(|value| Value::Number(Number::from(value))),
        "null" if value.is_null() => Some(Value::Null),
        _ => None,
    }
}

fn json_integer(value: &Value, coerce: bool) -> Option<i128> {
    if let Some(value) = value.as_i64() {
        return Some(value as i128);
    }
    if let Some(value) = value.as_u64() {
        return Some(value as i128);
    }
    if coerce {
        if let Some(value) = value.as_f64() {
            if value.is_finite() && value.fract() == 0.0 {
                return Some(value as i128);
            }
        }
        if let Some(value) = value.as_str() {
            return value.trim().parse().ok();
        }
    }
    None
}

fn json_float(value: &Value, coerce: bool) -> Option<f64> {
    if let Some(number) = value.as_number() {
        if number.is_f64() {
            return number.as_f64();
        }
        if coerce {
            return number.as_f64();
        }
    }
    coerce
        .then(|| value.as_str()?.trim().parse().ok())
        .flatten()
}

fn to_join_type(name: &str) -> Result<JoinType, String> {
    match name {
        "inner" => Ok(JoinType::Inner),
        "left" => Ok(JoinType::Left),
        "right" => Ok(JoinType::Right),
        "full" => Ok(JoinType::Full),
        "semi" => Ok(JoinType::Semi),
        "anti" => Ok(JoinType::Anti),
        "cross" => Ok(JoinType::Cross),
        name => Err(format!("unsupported native join type {name:?}")),
    }
}

fn to_dtype(name: &str) -> Result<DataType, String> {
    match name {
        "bool" => Ok(DataType::Boolean),
        "int8" => Ok(DataType::Int8),
        "int16" => Ok(DataType::Int16),
        "int32" => Ok(DataType::Int32),
        "int64" => Ok(DataType::Int64),
        "uint8" => Ok(DataType::UInt8),
        "uint16" => Ok(DataType::UInt16),
        "uint32" => Ok(DataType::UInt32),
        "uint64" => Ok(DataType::UInt64),
        "float32" => Ok(DataType::Float32),
        "float64" => Ok(DataType::Float64),
        "string" => Ok(DataType::String),
        name => Err(format!("unsupported native cast type {name:?}")),
    }
}

fn write_output(data: Vec<u8>, output: *mut *mut u8, output_length: *mut usize) {
    if output.is_null() || output_length.is_null() {
        return;
    }
    let mut data = data.into_boxed_slice();
    let length = data.len();
    let pointer = data.as_mut_ptr();
    std::mem::forget(data);
    unsafe {
        *output = pointer;
        *output_length = length;
    }
}

fn write_error(message: &str, output: *mut *mut u8, output_length: *mut usize) {
    if output.is_null() || output_length.is_null() {
        return;
    }
    let mut bytes = message.as_bytes().to_vec().into_boxed_slice();
    let length = bytes.len();
    let pointer = bytes.as_mut_ptr();
    std::mem::forget(bytes);
    unsafe {
        *output = pointer;
        *output_length = length;
    }
}
