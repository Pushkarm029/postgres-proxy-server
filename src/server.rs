use crate::data_store::postgres::PostgresDataStore;
use crate::data_store::snowflake::SnowflakeDataStore;
use crate::data_store::DataStoreClient;
use crate::processor::ProcessorFactory;
use crate::semantic_model::local_store::LocalSemanticModelStore;
use crate::semantic_model::s3_store::S3SemanticModelStore;
use crate::semantic_model::SemanticModelStore;
use crate::utils::config::SnowflakeConfig;
use crate::utils::config::{Config, PostgresConfig, S3Config};
use envconfig::Envconfig;
use log::{error, info};
use std::process;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
struct ProxyServer<D, S> {
    config: Config,
    factory: Arc<ProcessorFactory<D, S>>,
}

impl<D, S> ProxyServer<D, S>
where
    D: DataStoreClient + Send + Sync + 'static,
    S: SemanticModelStore + Send + Sync + 'static,
{
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let server_address = format!("{}:{}", self.config.server_host, self.config.server_port);
        info!("Starting server at {}", server_address);

        let listener = TcpListener::bind(&server_address).await.map_err(|err| {
            error!("Failed to bind server address: {}", err);
            err
        })?;

        info!("Listening for connections on {}", server_address);

        // Gracefully handle shutdown signals
        let signal_future = signal::ctrl_c();
        tokio::select! {
            _ = self.accept_connections(listener) => {},
            _ = signal_future => {
                info!("Shutdown signal received. Stopping server...");
            }
        }

        Ok(())
    }

    async fn accept_connections(&self, listener: TcpListener) {
        loop {
            match listener.accept().await {
                Ok((tcp_stream, addr)) => {
                    info!("New connection accepted from: {}", addr);
                    let factory_ref = self.factory.clone();
                    tokio::spawn(async move {
                        if let Err(e) =
                            pgwire::tokio::process_socket(tcp_stream, None, factory_ref).await
                        {
                            error!("Error processing socket: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let config = Config::init_from_env().map_err(|e| {
        error!("Failed to initialize config: {}", e);
        e
    })?;

    match (
        config.data_store.as_str(),
        config.semantic_model_store.as_str(),
    ) {
        ("postgres", "local") => {
            let server =
                <ProxyServer<PostgresDataStore, LocalSemanticModelStore>>::with_config(config)
                    .await?;

            if let Err(e) = server.run().await {
                error!("Server encountered an error: {}", e);
                process::exit(1);
            }
        }
        ("snowflake", "s3") => {
            let server =
                <ProxyServer<SnowflakeDataStore, S3SemanticModelStore>>::with_config(config)
                    .await?;

            if let Err(e) = server.run().await {
                error!("Server encountered an error: {}", e);
                process::exit(1);
            }
        }
        _ => {
            error!("Unsupported data store and semantic model store combination");
            return Err("Unsupported data store and semantic model store combination".into());
        }
    }

    Ok(())
}

impl ProxyServer<PostgresDataStore, LocalSemanticModelStore> {
    pub async fn with_config(config: Config) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            config,
            factory: Arc::new(ProcessorFactory::new(
                PostgresDataStore::new(PostgresConfig::init_from_env().map_err(|e| {
                    error!("Failed to initialize Postgres DataStore config: {}", e);
                    e
                })?)
                .await
                .map_err(|e| {
                    error!("Failed to create PostgresDataStore: {}", e);
                    e
                })?,
                LocalSemanticModelStore::mock(),
            )),
        })
    }
}

impl ProxyServer<SnowflakeDataStore, S3SemanticModelStore> {
    pub async fn with_config(config: Config) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            config,
            factory: Arc::new(ProcessorFactory::new(
                SnowflakeDataStore::new(SnowflakeConfig::init_from_env().map_err(|e| {
                    error!("Failed to initialize Postgres DataStore config: {}", e);
                    e
                })?)
                .map_err(|e| {
                    error!("Failed to create SnowflakeDataStore: {}", e);
                    e
                })?,
                S3SemanticModelStore::new(S3Config::init_from_env().map_err(|e| {
                    error!("Failed to initialize Postgres DataStore config: {}", e);
                    e
                })?)
                .await,
            )),
        })
    }
}
