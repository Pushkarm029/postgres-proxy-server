use crate::config::Config;
use crate::data_store::DataStoreClient;
use crate::processor::ProcessorFactory;
use crate::semantic_model::SemanticModelStore;
use log::{error, info};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;

pub struct ProxyServer<D, S> {
    pub config: Config,
    pub factory: Arc<ProcessorFactory<D, S>>,
}

impl<D, S> ProxyServer<D, S>
where
    D: DataStoreClient + Send + Sync + 'static,
    S: SemanticModelStore + Send + Sync + 'static,
{
    pub fn new(config: Config, factory: ProcessorFactory<D, S>) -> Self {
        Self {
            config,
            factory: Arc::new(factory),
        }
    }

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
