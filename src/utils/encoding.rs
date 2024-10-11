use bytes::BytesMut;
use futures::Stream;
use log::error;
use pgwire::api::portal::Format;
use pgwire::api::results::FieldInfo;
use pgwire::error::PgWireResult;
use pgwire::messages::data::DataRow;
use snowflake_connector_rs::SnowflakeRow;
use std::sync::Arc;
use tokio_postgres::{types::Type, Row, Statement};

mod common {
    use super::*;

    pub fn encode_value(buffer: &mut BytesMut, value: Option<String>) {
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
}

mod postgres {
    use super::*;

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
                let value = match pg_type {
                    &Type::INT4 => row.get::<_, Option<i32>>(idx).map(|v| v.to_string()),
                    &Type::NUMERIC => row
                        .get::<_, Option<rust_decimal::Decimal>>(idx)
                        .map(|v| v.to_string()),
                    &Type::TEXT | &Type::VARCHAR => row.get::<_, Option<String>>(idx),
                    &Type::BOOL => row.get::<_, Option<bool>>(idx).map(|v| v.to_string()),
                    &Type::FLOAT4 => row.get::<_, Option<f32>>(idx).map(|v| v.to_string()),
                    &Type::FLOAT8 => row.get::<_, Option<f64>>(idx).map(|v| v.to_string()),
                    &Type::INT8 => row.get::<_, Option<i64>>(idx).map(|v| v.to_string()),
                    _ => {
                        error!("Unexpected Type: {:?}", pg_type);
                        None
                    }
                };
                common::encode_value(&mut buffer, value);
            }
            Ok(DataRow::new(buffer, schema.len() as i16))
        }))
    }
}

mod snowflake {
    use super::*;

    pub fn row_desc_from_stmt(
        rows: &[SnowflakeRow],
        format: &Format,
    ) -> PgWireResult<Vec<FieldInfo>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        let column_types = rows[0].column_types();
        column_types
            .into_iter()
            .map(|col| {
                let field_type = map_snowflake_type_to_pg(col.column_type().snowflake_type());
                Ok(FieldInfo::new(
                    col.name().to_owned(),
                    None,
                    None,
                    field_type,
                    format.format_for(col.index()),
                ))
            })
            .collect()
    }

    fn map_snowflake_type_to_pg(snowflake_type: &str) -> Type {
        match snowflake_type.to_uppercase().as_str() {
            "NUMBER" | "INT" | "INTEGER" => Type::INT8,
            "FLOAT" => Type::FLOAT8,
            "VARCHAR" | "TEXT" | "STRING" => Type::TEXT,
            "BOOLEAN" => Type::BOOL,
            "DATE" => Type::DATE,
            "TIMESTAMP" | "TIMESTAMP_NTZ" => Type::TIMESTAMP,
            "VARIANT" | "OBJECT" | "ARRAY" => Type::JSONB,
            _ => Type::UNKNOWN,
        }
    }

    pub fn encode_row_data(
        rows: Vec<SnowflakeRow>,
        schema: Arc<Vec<FieldInfo>>,
    ) -> impl Stream<Item = PgWireResult<DataRow>> {
        futures::stream::iter(rows.into_iter().map(move |row| {
            let mut buffer = BytesMut::new();
            for field in schema.iter() {
                let value = row.get::<String>(field.name()).ok();
                common::encode_value(&mut buffer, value);
            }
            Ok(DataRow::new(buffer, schema.len() as i16))
        }))
    }
}

// Public API
pub use postgres::encode_row_data as encode_postgres_row_data;
pub use postgres::row_desc_from_stmt as postgres_row_desc_from_stmt;
pub use snowflake::encode_row_data as encode_snowflake_row_data;
pub use snowflake::row_desc_from_stmt as snowflake_row_desc_from_stmt;
