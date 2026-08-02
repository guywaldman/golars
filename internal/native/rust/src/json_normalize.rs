use serde_json::{Map, Number, Value};

use super::{JsonDecodeOptions, JsonType};

pub(super) fn normalize_json_row(
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
