use crate::data_store::DataStore;
use crate::query_handler::QueryHandler;
use crate::semantic_model::SemanticModelStore;
use async_trait::async_trait;
use pgwire::api::results::Response;
use pgwire::api::{
    auth::noop::NoopStartupHandler,
    copy::NoopCopyHandler,
    query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler},
    PgWireHandlerFactory,
};
use pgwire::error::PgWireResult;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Processor {
    query_handler: Arc<Mutex<Box<QueryHandler>>>,
}

#[async_trait]
impl SimpleQueryHandler for Processor {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>> {
        let query_handler = self.query_handler.lock().await;
        query_handler.handle(query).map_err(|e| e.into())
    }
}

impl Processor{
    pub async fn new(
        data_store: &dyn DataStore,
        semantic_model: &dyn SemanticModelStore,
    ) -> Self {
        Self {
            query_handler: Arc::new(Mutex::new(QueryHandler::new(data_store, semantic_model))),
        }
    }
}

pub struct ProcessorFactory {
    handler: Arc<Processor>,
}

impl ProcessorFactory {
    pub async fn new(
        data_store: &dyn DataStore,
        semantic_model: &dyn SemanticModelStore,
    ) -> Self {
        Self {
            handler: Arc::new(Processor::new(data_store, semantic_model).await),
        }
    }
}

impl<'a> PgWireHandlerFactory for ProcessorFactory {
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
