use bytes::BytesMut;
use futures::Stream;
use log::error;
use pgwire::api::portal::Format;
use pgwire::api::results::FieldInfo;
use pgwire::error::PgWireResult;
use pgwire::messages::data::DataRow;
use std::sync::Arc;
use tokio_postgres::{types::Type, Row, Statement};

pub fn row_desc_from_stmt(stmt: &Statement, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
    stmt.columns()
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            let field_type = col.type_();
            Ok(FieldInfo::new(
                col.name().to_owned(),
                None,
                None,
                field_type.clone(),
                format.format_for(idx),
            ))
        })
        .collect()
}

pub fn encode_row_data(
    rows: Vec<Row>,
    schema: Arc<Vec<FieldInfo>>,
) -> impl Stream<Item = PgWireResult<DataRow>> {
    futures::stream::iter(rows.into_iter().map(move |row| {
        let mut buffer = BytesMut::new();
        for (idx, field) in schema.iter().enumerate() {
            let pg_type = field.datatype();
            match pg_type {
                &Type::INT4 => {
                    let value: Option<i32> = row.get(idx);
                    encode_value(&mut buffer, value.map(|v| v.to_string()));
                }
                &Type::NUMERIC => {
                    let value: Option<rust_decimal::Decimal> = row.get(idx);
                    encode_value(&mut buffer, value.map(|v| v.to_string()));
                }
                &Type::TEXT | &Type::VARCHAR => {
                    let value: Option<String> = row.get(idx);
                    encode_value(&mut buffer, value);
                }
                &Type::BOOL => {
                    let value: Option<bool> = row.get(idx);
                    encode_value(&mut buffer, value.map(|v| v.to_string()));
                }
                &Type::FLOAT4 => {
                    let value: Option<f32> = row.get(idx);
                    encode_value(&mut buffer, value.map(|v| v.to_string()));
                }
                &Type::FLOAT8 => {
                    let value: Option<f64> = row.get(idx);
                    encode_value(&mut buffer, value.map(|v| v.to_string()));
                }
                &Type::INT8 => {
                    let value: Option<i64> = row.get(idx);
                    encode_value(&mut buffer, value.map(|v| v.to_string()));
                }
                _ => {
                    encode_value(&mut buffer, None::<String>);
                    error!("Unexpected Type")
                }
            }
        }
        Ok(DataRow::new(buffer, schema.len() as i16))
    }))
}

fn encode_value(buffer: &mut BytesMut, value: Option<String>) {
    match value {
        Some(v) => {
            buffer.extend_from_slice(&(v.len() as i32).to_be_bytes());
            buffer.extend_from_slice(v.as_bytes());
        }
        None => {
            buffer.extend_from_slice(&(-1_i32).to_be_bytes());
        }
    }
}
