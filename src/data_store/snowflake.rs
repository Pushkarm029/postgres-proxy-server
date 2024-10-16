use std::sync::Arc;
use std::time::Duration;

use crate::data_store::{DataStoreClient, DataStoreError, DataStoreMapping};
use crate::utils::config::SnowflakeConfig;
use crate::utils::encoding::{encode_snowflake_row_data, snowflake_row_desc_from_stmt};
use async_trait::async_trait;
use pgwire::api::portal::Format;
use pgwire::api::results::{QueryResponse, Response};
use snowflake_connector_rs::SnowflakeSession;
use snowflake_connector_rs::{SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig};
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
        let session = self.connect().await?;

        let rows = session
            .query(query)
            .await
            .map_err(|e| DataStoreError::QueryError(e.to_string()))?;

        let field_info = snowflake_row_desc_from_stmt(&rows, &Format::UnifiedText)
            .map_err(|e| DataStoreError::ColumnNotFound(e.to_string()))?;
        let field_info_arc = Arc::new(field_info);
        let data_rows = encode_snowflake_row_data(rows, field_info_arc.clone());
        Ok(vec![Response::Query(QueryResponse::new(
            field_info_arc,
            Box::pin(data_rows),
        ))])
    }
}
