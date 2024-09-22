use log::{error, info};
use std::sync::Arc;
use tokio::net::TcpListener;

mod processor;
mod query_handler;
mod schema;
mod tests;
mod utils;

use crate::processor::ProcessorFactory;
use crate::utils::config::get_server_binding_address;

#[tokio::main]
async fn main() {
    env_logger::init();
    let factory = Arc::new(ProcessorFactory::new().await);
    let server_address = get_server_binding_address();

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
