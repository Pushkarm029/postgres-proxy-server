use crate::data_store::{DataStoreClient, DataStoreError, DataStoreMapping};
use crate::utils::config::PostgresConfig;
use crate::utils::encoding::{encode_postgres_row_data, postgres_row_desc_from_stmt};
use async_trait::async_trait;
use pgwire::api::{
    portal::Format,
    results::{QueryResponse, Response},
};
use sqlparser::dialect::PostgreSqlDialect;
use std::sync::Arc;
use tokio_postgres::{Client, NoTls};

pub struct PostgresDataStore {
    config: PostgresConfig,
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

        Ok(PostgresDataStore { config, client })
    }
}

// impl<'a> Clone for PostgresDataStore {
//     fn clone(&self) -> Self {
//         // Implement a way to clone the client, or manage a new connection
//         // Note: tokio_postgres::Client is not cloneable, so you would need
//         // to re-establish the connection or share the same client connection
//         todo!("Make the data store/connection cloneable")
//     }
// }

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

        let field_info = postgres_row_desc_from_stmt(&stmt, &Format::UnifiedText)
            .map_err(|e| DataStoreError::ColumnNotFound(e.to_string()))?;
        let field_info_arc = Arc::new(field_info);
        let data_rows = encode_postgres_row_data(rows, field_info_arc.clone());
        Ok(vec![Response::Query(QueryResponse::new(
            field_info_arc,
            Box::pin(data_rows),
        ))])
    }
}
