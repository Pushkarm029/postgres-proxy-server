use crate::data_store::{DataStore, DataStoreError, Row};
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
        // let connection_string = format!(
        //     "host={} user={} password={} dbname={}",
        //     config.host, config.user, config.password, config.dbname
        // );

        let connection_string: String = format!(
            "postgres://{}:{}@{}/{}",
            config.user, config.password, config.host, config.dbname
        );
        // postgres://postgres:postgres@localhost:5432/information_schema

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
        todo!("Make the data store/connection cloneable")
    }
}

#[async_trait]
impl DataStore for PostgresDataStore {
    fn get_dialect(&self) -> &dyn sqlparser::dialect::Dialect {
        &PostgreSqlDialect {}
    }

    fn map_function(&self, pg_function: &str) -> Option<String> {
        match pg_function {
            "now()" => Some("CURRENT_TIMESTAMP".to_string()),
            _ => Some(pg_function.to_string()), // No mapping needed, PostgreSQL supports most functions
        }
    }

    // async fn execute(&self, query: &str) -> Result<Vec<DataRow>, DataStoreError> {
    fn execute(&self, query: &str) -> Result<Vec<DataRow>, DataStoreError> {
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
