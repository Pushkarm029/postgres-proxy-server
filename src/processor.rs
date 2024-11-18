use crate::auth::Authentication;
use crate::data_store::DataStoreClient;
use crate::semantic_model::SemanticModelStore;
use crate::sql_parser::{SqlError, SqlParser};
use async_trait::async_trait;
use log::debug;
use pgwire::api::auth::cleartext::CleartextPasswordAuthStartupHandler;
use pgwire::api::auth::DefaultServerParameterProvider;
use pgwire::api::results::Response;
use pgwire::api::{
    copy::NoopCopyHandler,
    query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler},
    PgWireHandlerFactory,
};
use pgwire::error::PgWireResult;
use pgwire::error::{ErrorInfo, PgWireError};
use std::sync::Arc;

pub struct QueryHandler<D, S> {
    data_store: D,
    semantic_model: S,
}

impl<D, S> QueryHandler<D, S>
where
    D: DataStoreClient,
    S: SemanticModelStore,
{
    pub fn new(data_store: D, semantic_model: S) -> Self {
        Self {
            data_store,
            semantic_model,
        }
    }

    pub async fn handle(&self, query: &str) -> PgWireResult<Vec<Response>> {
        debug!("Initial query: {}", query);
        let parser = SqlParser::new(D::get_mapping(), self.semantic_model.clone());
        match parser.transform(query) {
            Ok(sql) => {
                debug!("Transformed query: {}", &sql);
                // Execute the sql and return the result
                self.data_store.execute(&sql).await.map_err(|e| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "SQLSTATE".to_string(),
                        "ERROR".to_string(),
                        e.to_string(),
                    )))
                })
            }
            Err(SqlError::InformationSchemaResult(info)) => {
                // TODO: Return the information schema result
                todo!()
            }
            Err(e) => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "SQLSTATE".to_string(),
                "ERROR".to_string(),
                e.to_string(),
            )))),
        }
    }
}

#[async_trait]
impl<D, S> SimpleQueryHandler for QueryHandler<D, S>
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
        self.handle(query).await
    }
}

pub struct ProcessorFactory<D, S> {
    handler: Arc<QueryHandler<D, S>>,
}

impl<D, S> ProcessorFactory<D, S>
where
    D: DataStoreClient,
    S: SemanticModelStore,
{
    pub fn new(data_store: D, semantic_model: S) -> Self {
        Self {
            handler: Arc::new(QueryHandler::new(data_store, semantic_model)),
        }
    }
}

impl<D, S> PgWireHandlerFactory for ProcessorFactory<D, S>
where
    D: DataStoreClient + Send + Sync,
    S: SemanticModelStore + Send + Sync,
{
    type StartupHandler =
        CleartextPasswordAuthStartupHandler<Authentication, DefaultServerParameterProvider>;
    type SimpleQueryHandler = QueryHandler<D, S>;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(CleartextPasswordAuthStartupHandler::new(
            Authentication::from_env(),
            DefaultServerParameterProvider::default(),
        ))
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }
}
