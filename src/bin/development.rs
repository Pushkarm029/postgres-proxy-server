use log::error;
use std::process;

use eqtble_sql::{
    config::{Config, SnowflakeConfig},
    data_store::snowflake::SnowflakeDataStore,
    processor::ProcessorFactory,
    semantic_model::local_store::LocalSemanticModelStore,
    ProxyServer,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let config = Config::new().map_err(|e| {
        error!("Failed to initialize config: {}", e);
        e
    })?;

    let snowflake_config = SnowflakeConfig::new().map_err(|e| {
        error!("Failed to initialize Snowflake config: {}", e);
        e
    })?;

    let data_store = SnowflakeDataStore::new(snowflake_config).map_err(|e| {
        error!("Failed to create SnowflakeDataStore: {}", e);
        e
    })?;

    let semantic_model_store = LocalSemanticModelStore::new().map_err(|e| {
        error!("Failed to create LocalSemanticModelStore: {}", e);
        e
    })?;

    let factory = ProcessorFactory::new(data_store, semantic_model_store);

    let server = ProxyServer::new(config, factory);

    if let Err(e) = server.run().await {
        error!("Server encountered an error: {}", e);
        process::exit(1);
    }

    Ok(())
}
