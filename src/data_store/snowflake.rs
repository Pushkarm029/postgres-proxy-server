use std::sync::Arc;
use std::time::Duration;

use super::encode_value;
use crate::config::SnowflakeConfig;
use crate::data_store::{DataStoreClient, DataStoreError, DataStoreMapping};
use async_trait::async_trait;
use bytes::BytesMut;
use futures::Stream;
use pgwire::api::portal::Format;
use pgwire::api::results::FieldInfo;
use pgwire::api::results::{QueryResponse, Response};
use pgwire::error::PgWireResult;
use pgwire::messages::data::DataRow;
use snowflake_connector_rs::SnowflakeRow;
use snowflake_connector_rs::SnowflakeSession;
use snowflake_connector_rs::{SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig};
use tokio_postgres::types::Type;

pub struct SnowflakeDataStore {
    client: SnowflakeClient,
}

pub struct SnowflakeMapping;

impl SnowflakeDataStore {
    pub fn new(config: SnowflakeConfig) -> Result<Self, DataStoreError> {
        Ok(SnowflakeDataStore {
            client: SnowflakeClient::new(
                &config.user,
                SnowflakeAuthMethod::Password(config.password.clone()),
                SnowflakeClientConfig {
                    account: config.account.clone(),
                    warehouse: config.warehouse,
                    database: config.database,
                    schema: config.schema,
                    role: config.role,
                    timeout: config.timeout.map(Duration::from_secs),
                },
            )
            .map_err(|e| DataStoreError::ConnectionError(e.to_string()))?,
        })
    }

    async fn connect(&self) -> Result<SnowflakeSession, DataStoreError> {
        self.client.create_session().await.map_err(|e| {
            DataStoreError::ConnectionError(format!("Failed to connect to Snowflake, {}", e))
        })
    }
}

impl DataStoreMapping for SnowflakeMapping {
    fn get_dialect(&self) -> &dyn sqlparser::dialect::Dialect {
        &sqlparser::dialect::SnowflakeDialect {}
    }

    fn map_function(&self, pg_function: &str) -> Option<String> {
        match pg_function {
            "now()" => Some("CURRENT_TIMESTAMP()".to_string()),
            // Map other Postgres functions to Snowflake equivalents
            _ => Some(pg_function.to_string()),
        }
    }
}

#[async_trait]
impl DataStoreClient for SnowflakeDataStore {
    type Mapping = SnowflakeMapping;

    fn get_mapping() -> Self::Mapping {
        SnowflakeMapping {}
    }

    async fn execute(&self, query: &str) -> Result<Vec<Response>, DataStoreError> {
        println!("Executing SQL: {}", query);
        let session = self.connect().await?;

        let rows = session
            .query(query)
            .await
            .map_err(|e| DataStoreError::QueryError(e.to_string()))?;

        let field_info = row_desc_from_stmt(&rows, &Format::UnifiedText)
            .map_err(|e| DataStoreError::ColumnNotFound(e.to_string()))?;
        let field_info_arc = Arc::new(field_info);
        let data_rows = encode_row_data(rows, field_info_arc.clone());
        Ok(vec![Response::Query(QueryResponse::new(
            field_info_arc,
            Box::pin(data_rows),
        ))])
    }
}

pub fn row_desc_from_stmt(rows: &[SnowflakeRow], format: &Format) -> PgWireResult<Vec<FieldInfo>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let column_types = rows[0].column_types();
    column_types
        .into_iter()
        .map(|col| {
            let field_type = map_type_to_pg(col.column_type().snowflake_type());
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

pub fn encode_row_data(
    rows: Vec<SnowflakeRow>,
    schema: Arc<Vec<FieldInfo>>,
) -> impl Stream<Item = PgWireResult<DataRow>> {
    futures::stream::iter(rows.into_iter().map(move |row| {
        let mut buffer = BytesMut::new();
        for field in schema.iter() {
            let value = row.get::<String>(field.name()).ok();
            encode_value(&mut buffer, value);
        }
        Ok(DataRow::new(buffer, schema.len() as i16))
    }))
}

fn map_type_to_pg(snowflake_type: &str) -> Type {
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
