use crate::data_store::DataStoreClient;
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

pub struct Processor<D, S> {
    query_handler: Arc<QueryHandler<D, S>>,
}

#[async_trait]
impl<D, S> SimpleQueryHandler for Processor<D, S>
where
    D: DataStoreClient + Send + Sync,
    S: SemanticModelStore + Send + Sync,
{
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        'life0: 'a,
    {
        let query_handler = &self.query_handler;
        query_handler.handle(query).await
    }
}

impl<D, S> Processor<D, S>
where
    D: DataStoreClient,
    S: SemanticModelStore,
{
    pub fn new(data_store: D, semantic_model: S) -> Self {
        Self {
            query_handler: Arc::new(QueryHandler::new(data_store, semantic_model)),
        }
    }
}

pub struct ProcessorFactory<D, S> {
    handler: Arc<Processor<D, S>>,
}

impl<D, S> ProcessorFactory<D, S>
where
    D: DataStoreClient,
    S: SemanticModelStore,
{
    pub fn new(data_store: D, semantic_model: S) -> Self {
        Self {
            handler: Arc::new(Processor::new(data_store, semantic_model)),
        }
    }
}

impl<D, S> PgWireHandlerFactory for ProcessorFactory<D, S>
where
    D: DataStoreClient + Send + Sync,
    S: SemanticModelStore + Send + Sync,
{
    type StartupHandler = NoopStartupHandler;
    type SimpleQueryHandler = Processor<D, S>;
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
