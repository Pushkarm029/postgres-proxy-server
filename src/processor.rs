use async_trait::async_trait;
use pgwire::api::{
    auth::noop::NoopStartupHandler,
    copy::NoopCopyHandler,
    query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler},
    PgWireHandlerFactory,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};

use crate::query_handler::handle_query;
use crate::utils::config::DB_ADDRESS;

pub struct Processor {
    client: Arc<Mutex<Client>>,
}

#[async_trait]
impl SimpleQueryHandler for Processor {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> pgwire::error::PgWireResult<Vec<pgwire::api::results::Response<'a>>> {
        handle_query(self.client.clone(), query).await
    }
}

impl Processor {
    pub async fn new() -> Self {
        let (client, connection) = tokio_postgres::connect(DB_ADDRESS, NoTls)
            .await
            .expect("Failed to connect to database");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Self {
            client: Arc::new(Mutex::new(client)),
        }
    }
}

pub struct ProcessorFactory {
    handler: Arc<Processor>,
}

impl ProcessorFactory {
    pub async fn new() -> Self {
        Self {
            handler: Arc::new(Processor::new().await),
        }
    }
}

impl PgWireHandlerFactory for ProcessorFactory {
    type StartupHandler = NoopStartupHandler;
    type SimpleQueryHandler = Processor;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(NoopStartupHandler)
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }
}
