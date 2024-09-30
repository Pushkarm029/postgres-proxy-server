use crate::data_store::{DataStoreClient, DataStoreError, DataStoreMapping};
use async_trait::async_trait;
use pgwire::messages::data::DataRow;
use sqlparser::dialect::PostgreSqlDialect;
use tokio_postgres::{Client, NoTls};

pub struct PostgresConfig {
    pub host: String,
    pub user: String,
    pub password: String,
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
            "now()" => Some("CURRENT_TIMESTAMP".to_string()), // Example of mapping
            _ => Some(pg_function.to_string()), // PostgreSQL supports most functions directly
        }
    }

    // Implement type mapping if necessary
    // fn map_type(&self, pg_type: &PostgresType) -> Option<String> {
    //     // Example mapping
    // }
}

// #[async_trait]
impl DataStoreClient for PostgresDataStore {
    fn execute(&self, sql: &str) -> Result<Vec<DataRow>, DataStoreError> {
        // let rows = self.client.query(query, &[]).await.map_err(|e| {
        //     DataStoreError::QueryError(format!("Error executing query: {}", e))
        // })?;

        // let mut data_rows = Vec::new();

        // for row in rows {
        //     let mut data_row = DataRow::new();

        //     // Add columns to data_row (for simplicity, this example assumes all columns are text)
        //     for (i, column) in row.columns().iter().enumerate() {
        //         let value: String = row.try_get(i).unwrap_or_default();
        //         data_row.add_column(value.into_bytes());
        //     }

        //     data_rows.push(data_row);
        // }

        // Ok(data_rows)
        todo!("TODO")
    }
}
