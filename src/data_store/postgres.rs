use super::DataStore;
use crate::data_store::{DataStoreClient, DataStoreError, DataStoreMapping};
use async_trait::async_trait;
use pgwire::api::{
    portal::Format,
    results::{QueryResponse, Response, Tag},
};
use sqlparser::dialect::PostgreSqlDialect;
use tokio_postgres::{Client, NoTls};

pub struct PostgresConfig {
    pub user: String,
    pub password: String,
    pub host: String,
    pub dbname: String,
}

pub struct PostgresDataStore {
    config: PostgresConfig,
    client: Client,
}

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

        Ok(PostgresDataStore { config, client })
    }
}

impl<'a> Clone for PostgresDataStore {
    fn clone(&self) -> Self {
        // Implement a way to clone the client, or manage a new connection
        // Note: tokio_postgres::Client is not cloneable, so you would need
        // to re-establish the connection or share the same client connection
        todo!("Make the data store/connection cloneable")
    }
}

impl DataStoreMapping for PostgresDataStore {
    fn get_dialect(&self) -> &dyn sqlparser::dialect::Dialect {
        &PostgreSqlDialect {}
    }

    fn map_function(&self, pg_function: &str) -> Option<String> {
        match pg_function {
            "now()" => Some("CURRENT_TIMESTAMP".to_string()),
            _ => Some(pg_function.to_string()),
        }
    }

    // Implement type mapping if necessary
    // fn map_type(&self, pg_type: &PostgresType) -> Option<String> {
    //     // Example mapping
    // }
}

use crate::utils::encoding::encode_row_data;
use crate::utils::encoding::row_desc_from_stmt;
use std::sync::Arc;

#[async_trait]
impl DataStoreClient for PostgresDataStore {
    async fn execute(&self, sql: &str) -> Result<Vec<Response>, DataStoreError> {
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

impl DataStore for PostgresDataStore {}
