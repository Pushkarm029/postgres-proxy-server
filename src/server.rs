use crate::data_store::postgres::{PostgresConfig, PostgresDataStore};
use crate::data_store::DataStoreType;
use crate::data_store::{SnowflakeConfig, SnowflakeDataStore};
use crate::processor::ProcessorFactory;
use crate::semantic_model::local_store::LocalSemanticModelStore;
use crate::semantic_model::S3SemanticModelStore;
use crate::semantic_model::SemanticModelType;
use crate::utils::config::{
    Config, PostgresDataStoreEnvConfig, S3SemanticModelEnvConfig, SnowflakeDataStoreEnvConfig,
};
use envconfig::Envconfig;
use log::{error, info};
use std::sync::Arc;
use tokio::net::TcpListener;

async fn run_tcp_server() {
    env_logger::init();
    let config = Config::init_from_env()
        .map_err(|e| {
            error!("Failed to initialize config: {}", e);
            e
        })
        .unwrap();
    let semantic_model_store: SemanticModelType = match config.semantic_model_store.as_str() {
        "local" => {
            info!("Using LocalSemanticModelStore");
            SemanticModelType::Local(LocalSemanticModelStore::new())
        }
        _ => {
            info!("Using S3SemanticModelStore");
            let s3_semantic_model_config = S3SemanticModelEnvConfig::init_from_env()
                .map_err(|e| {
                    error!("Failed to initialize config for S3 Semantic Store: {}", e);
                    e
                })
                .unwrap();
            SemanticModelType::S3(
                S3SemanticModelStore::new(
                    s3_semantic_model_config.tenant,
                    s3_semantic_model_config.s3_bucket_name,
                )
                .await,
            )
        }
    };

    let data_store: DataStoreType = match config.data_store.as_str() {
        "postgres" => {
            info!("Using PostgresDataStore");
            let postgres_data_store_config = PostgresDataStoreEnvConfig::init_from_env()
                .map_err(|e| {
                    error!("Failed to initialize config for Postgres DataStore: {}", e);
                    e
                })
                .unwrap();
            DataStoreType::Postgres(
                PostgresDataStore::new(PostgresConfig {
                    user: postgres_data_store_config.user,
                    password: postgres_data_store_config.password,
                    host: postgres_data_store_config.host,
                    dbname: postgres_data_store_config.dbname,
                })
                .await
                .map_err(|e| {
                    error!("Failed to create Postgres DataStore: {}", e);
                    e
                })
                .unwrap(),
            )
        }
        _ => {
            info!("Using SnowflakeDataStore");
            let snowflake_data_store_config = SnowflakeDataStoreEnvConfig::init_from_env().unwrap();
            DataStoreType::Snowflake(
                SnowflakeDataStore::new(SnowflakeConfig {
                    account: snowflake_data_store_config.snowflake_account,
                    user: snowflake_data_store_config.snowflake_user,
                    password: snowflake_data_store_config.snowflake_password,
                    warehouse: snowflake_data_store_config.snowflake_warehouse,
                    database: snowflake_data_store_config.snowflake_database,
                    schema: snowflake_data_store_config.snowflake_schema,
                })
                .map_err(|e| {
                    error!("Failed to create Snowflake DataStore: {}", e);
                    e
                })
                .unwrap(),
            )
        }
    };

    let factory = Arc::new(ProcessorFactory::new(data_store, semantic_model_store));
    let server_address = format!("{}:{}", config.server_host, config.server_port);

    info!("Starting server at {}", server_address);

    let listener = TcpListener::bind(server_address.clone())
        .await
        .unwrap_or_else(|err| {
            error!("Failed to bind server address: {}", err);
            std::process::exit(1);
        });

    info!("Listening for connections on {}", server_address);

    loop {
        match listener.accept().await {
            Ok((tcp_stream, addr)) => {
                info!("New connection accepted from: {}", addr);

                let factory_ref = factory.clone();
                tokio::spawn(async move {
                    pgwire::tokio::process_socket(tcp_stream, None, factory_ref).await
                });
            }
            Err(e) => {
                error!("Failed to accept connection: {}", e);
            }
        }
    }
}

#[tokio::main]
pub async fn main() {
    run_tcp_server().await;
}
