use crate::data_store::postgres::PostgresDataStore;
use crate::data_store::DataStoreClient;
use crate::processor::ProcessorFactory;
use crate::semantic_model::local_store::LocalSemanticModelStore;
use crate::semantic_model::SemanticModelStore;
use crate::utils::config::{Config, PostgresConfig};
use envconfig::Envconfig;
use log::{error, info};
use std::sync::Arc;
use tokio::net::TcpListener;

struct ProxyServer<D, S> {
    config: Config,
    factory: Arc<ProcessorFactory<D, S>>,
}

impl ProxyServer<PostgresDataStore, LocalSemanticModelStore> {
    pub async fn with_config(config: Config) -> Self {
        info!("Using PostgresDataStore");
        let postgres_config = PostgresConfig::init_from_env()
            .map_err(|e| {
                error!("Failed to initialize config for Postgres DataStore: {}", e);
                e
            })
            .unwrap();
        let data_store = PostgresDataStore::new(postgres_config)
            .await
            .map_err(|e| {
                error!("Failed to create Postgres DataStore: {}", e);
                e
            })
            .unwrap();
        let semantic_model_store = match config.semantic_model_store.as_str() {
            "local" => {
                info!("Using LocalSemanticModelStore");
                LocalSemanticModelStore::mock()
            }
            val => panic!("Incorrect semantic model type: {}", val),
        };

        Self {
            config,
            factory: Arc::new(ProcessorFactory::new(data_store, semantic_model_store)),
        }
    }
}

impl<D, S> ProxyServer<D, S>
where
    D: DataStoreClient + Send + Sync + 'static,
    S: SemanticModelStore + Send + Sync + 'static,
{
    pub async fn run(&self) {
        let server_address = format!("{}:{}", self.config.server_host, self.config.server_port);
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

                    let factory_ref = self.factory.clone();
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
}

#[tokio::main]
pub async fn main() {
    env_logger::init();
    let config = Config::init_from_env()
        .map_err(|e| {
            error!("Failed to initialize config: {}", e);
            e
        })
        .unwrap();
    let server = ProxyServer::with_config(config).await;
    server.run().await;
}
