use super::encode_value;
use crate::config::PostgresConfig;
use crate::data_store::{DataStoreClient, DataStoreError, DataStoreMapping};
use async_trait::async_trait;
use bytes::BytesMut;
use futures::Stream;
use log::error;
use pgwire::api::results::FieldInfo;
use pgwire::api::{
    portal::Format,
    results::{QueryResponse, Response},
};
use pgwire::error::PgWireResult;
use pgwire::messages::data::DataRow;
use sqlparser::dialect::PostgreSqlDialect;
use std::sync::Arc;
use tokio_postgres::{types::Type, Row, Statement};
use tokio_postgres::{Client, NoTls};

pub struct PostgresDataStore {
    client: Client,
}

pub struct PostgresMapping;

impl PostgresDataStore {
    pub async fn new(config: PostgresConfig) -> Result<Self, DataStoreError> {
        let connection_string = format!(
            "postgres://{}:{}@{}/{}",
            config.user, config.password, config.host, config.dbname
        );

        let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
            .await
            .map_err(|e| DataStoreError::ConnectionError(e.to_string()))?;

        // Spawn a task to manage the connection
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        Ok(PostgresDataStore { client })
    }
}

impl DataStoreMapping for PostgresMapping {
    fn get_dialect(&self) -> &dyn sqlparser::dialect::Dialect {
        &PostgreSqlDialect {}
    }

    // pass through since input functions are in postgres dialect
    fn map_function(&self, pg_function: &str) -> Option<String> {
        Some(pg_function.to_string())
    }

    // Implement type mapping if necessary
    // fn map_type(&self, pg_type: &PostgresType) -> Option<String> {
    //     // Example mapping
    // }
}

#[async_trait]
impl DataStoreClient for PostgresDataStore {
    type Mapping = PostgresMapping;

    fn get_mapping() -> Self::Mapping {
        PostgresMapping {}
    }

    async fn execute(&self, sql: &str) -> Result<Vec<Response>, DataStoreError> {
        println!("Executing SQL: {}", sql);
        let rows = self
            .client
            .query(sql, &[])
            .await
            .map_err(|e| DataStoreError::ColumnNotFound(e.to_string()))?;

        let stmt = self
            .client
            .prepare(sql)
            .await
            .map_err(|e| DataStoreError::ColumnNotFound(e.to_string()))?;

        let field_info = row_desc_from_stmt(&stmt, &Format::UnifiedText)
            .map_err(|e| DataStoreError::ColumnNotFound(e.to_string()))?;
        let field_info_arc = Arc::new(field_info);
        let data_rows = encode_row_data(rows, field_info_arc.clone());
        Ok(vec![Response::Query(QueryResponse::new(
            field_info_arc,
            Box::pin(data_rows),
        ))])
    }
}

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
            encode_value(&mut buffer, value);
        }
        Ok(DataRow::new(buffer, schema.len() as i16))
    }))
}
