use std::{panic, slice};

use polars::prelude::*;
use serde::Deserialize;
use serde_json::Value;

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
    path: String,
    format: String,
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
    dtype: String,
    #[serde(default)]
    descending: bool,
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
    if request.format != "parquet" {
        return Err(format!("unsupported native format {:?}", request.format));
    }
    let mut frame = LazyFrame::scan_parquet(
        PlPath::new(request.path.as_str()),
        ScanArgsParquet::default(),
    )
    .map_err(|error| error.to_string())?;
    for operation in request.operations {
        frame = match operation.kind.as_str() {
            "filter" if operation.exprs.len() == 1 => frame.filter(to_expr(&operation.exprs[0])?),
            "select" => frame.select(to_exprs(&operation.exprs)?),
            "with_columns" => frame.with_columns(to_exprs(&operation.exprs)?),
            "limit" => frame.limit(operation.limit as IdxSize),
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
                frame.group_by(keys).agg(expressions)
            }
            kind => return Err(format!("unsupported native operation {kind:?}")),
        };
    }
    frame.collect().map_err(|error| error.to_string())
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
    match expression.kind.as_str() {
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
        kind => Err(format!("unsupported native literal kind {kind:?}")),
    }
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
        "is_nan" => Ok(one_arg(expression)?.is_nan()),
        "is_not_nan" => Ok(one_arg(expression)?.is_not_nan()),
        "is_finite" => Ok(one_arg(expression)?.is_finite()),
        "is_infinite" => Ok(one_arg(expression)?.is_infinite()),
        name => Err(format!("unsupported native call {name:?}")),
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
