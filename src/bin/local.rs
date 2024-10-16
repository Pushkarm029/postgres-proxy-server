use log::error;
use std::process;

use postgres_proxy_server::{
    config::{Config, PostgresConfig},
    data_store::postgres::PostgresDataStore,
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

    let postgres_config = PostgresConfig::new().map_err(|e| {
        error!("Failed to initialize Postgres config: {}", e);
        e
    })?;

    let data_store = PostgresDataStore::new(postgres_config).await.map_err(|e| {
        error!("Failed to create PostgresDataStore: {}", e);
        e
    })?;

    let semantic_model_store = LocalSemanticModelStore::new()?;

    let factory = ProcessorFactory::new(data_store, semantic_model_store);

    let server = ProxyServer::new(config, factory);

    if let Err(e) = server.run().await {
        error!("Server encountered an error: {}", e);
        process::exit(1);
    }

    Ok(())
}
