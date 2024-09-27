use crate::data_store::{SnowflakeConfig, SnowflakeDataStore};
use crate::processor::ProcessorFactory;
use crate::semantic_model::S3SemanticModelStore;
use envconfig::Envconfig;
use log::{error, info};
use std::sync::Arc;
use tokio::net::TcpListener;

#[derive(Envconfig)]
struct Config {
    #[envconfig(from = "SERVER_HOST")]
    server_host: String,

    #[envconfig(from = "SERVER_PORT")]
    server_port: u16,

    #[envconfig(from = "SNOWFLAKE_ACCOUNT")]
    snowflake_account: String,

    #[envconfig(from = "SNOWFLAKE_USER")]
    snowflake_user: String,

    #[envconfig(from = "SNOWFLAKE_PASSWORD")]
    snowflake_password: String,

    #[envconfig(from = "SNOWFLAKE_WAREHOUSE")]
    snowflake_warehouse: String,

    #[envconfig(from = "SNOWFLAKE_DATABASE")]
    snowflake_database: String,

    #[envconfig(from = "S3_BUCKET_NAME")]
    s3_bucket_name: String,
}

async fn run_tcp_server() {
    env_logger::init();

    let temp_tenant_id_for_testing = "tenant1";
    let config = Config::init_from_env().unwrap();
    // let semantic_model = S3SemanticModelStore::new(
    //     temp_tenant_id_for_testing.to_string(),
    //     config.s3_bucket_name,
    // );

    // let semantic_model = semantic_model.await;
    // let snowflake_config = SnowflakeConfig {
    //     account: config.snowflake_account.clone(),
    //     user: config.snowflake_user.clone(),
    //     password: config.snowflake_password.clone(),
    //     warehouse: config.snowflake_warehouse.clone(),
    //     database: config.snowflake_database.clone(),
    //     schema: temp_tenant_id_for_testing.to_string(),
    // };
    // let data_store = SnowflakeDataStore::new(snowflake_config).unwrap_or_else(|err| {
    //     error!("Failed to create Snowflake dialect: {}", err);
    //     std::process::exit(1);
    // });

    let factory = Arc::new(ProcessorFactory::new().await);
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
