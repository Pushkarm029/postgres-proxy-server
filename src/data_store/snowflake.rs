use crate::data_store::{DataStoreClient, DataStoreError, DataStoreMapping};
// use odbc::{odbc_safe::AutocommitOn, safe::Odbc3, Connection, Environment, ResultSetState, Statement};
use super::DataStore;
use async_trait::async_trait;
use pgwire::api::results::Response;
use pgwire::messages::data::DataRow;
use snowflake_connector_rs::SnowflakeSession;
use snowflake_connector_rs::{SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig};

#[derive(Clone)]
pub struct SnowflakeConfig {
    pub account: String,
    pub user: String,
    pub password: String,
    pub warehouse: String,
    pub database: String,
    pub schema: String,
}

pub struct SnowflakeDataStore {
    config: SnowflakeConfig,
    client: SnowflakeClient,
}

// TODO: Fix me
impl Clone for SnowflakeDataStore {
    fn clone(&self) -> Self {
        SnowflakeDataStore {
            config: self.config.clone(),
            client: SnowflakeClient::new(
                &self.config.user,
                SnowflakeAuthMethod::Password(self.config.password.clone()),
                SnowflakeClientConfig {
                    account: self.config.account.clone(),
                    warehouse: Some(self.config.warehouse.clone()),
                    database: Some(self.config.database.clone()),
                    schema: Some(self.config.schema.clone()),
                    ..Default::default()
                },
            )
            .unwrap(),
        }
    }
}

impl SnowflakeDataStore {
    pub fn new(config: SnowflakeConfig) -> Result<Self, DataStoreError> {
        Ok(SnowflakeDataStore {
            config: config.clone(),
            client: SnowflakeClient::new(
                &config.account,
                SnowflakeAuthMethod::Password(config.password.clone()),
                SnowflakeClientConfig {
                    account: config.account.clone(),
                    warehouse: Some(config.warehouse.clone()),
                    database: Some(config.database.clone()),
                    schema: Some(config.schema.clone()),
                    ..Default::default()
                },
            )
            .unwrap(),
        })
    }

    async fn connect(&self) -> Result<SnowflakeSession, DataStoreError> {
        self.client.create_session().await.map_err(|e| {
            DataStoreError::ConnectionError(format!(
                "Failed to connect to Snowflake, {}",
                e.to_string()
            ))
        })
    }
}

impl DataStoreMapping for SnowflakeDataStore {
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
    async fn execute(&self, query: &str) -> Result<Vec<Response>, DataStoreError> {
        // let config = self.config.clone();
        let session = self.connect().await?;

        let rows = session
            .query(query)
            .await
            .map_err(|e| DataStoreError::QueryError(e.to_string()))?;
        let data = vec![DataRow::default()];

        todo!("Implement SnowflakeDataStore::execute");
        // Ok(data)
    }
}

impl DataStore for SnowflakeDataStore {}
