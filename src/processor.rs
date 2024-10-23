use crate::config::AuthConfig;
use crate::data_store::DataStoreClient;
use crate::semantic_model::SemanticModelStore;
use crate::sql_parser::SqlParser;
use async_trait::async_trait;
use log::debug;
use pgwire::api::auth::cleartext::CleartextPasswordAuthStartupHandler;
use pgwire::api::auth::{AuthSource, DefaultServerParameterProvider, LoginInfo, Password};
use pgwire::api::results::Response;
use pgwire::api::{
    copy::NoopCopyHandler,
    query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler},
    PgWireHandlerFactory,
};
use pgwire::error::PgWireResult;
use pgwire::error::{ErrorInfo, PgWireError};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

pub struct Authentication {
    pairs: HashMap<String, String>,
}

impl Authentication {
    pub fn from_env() -> Self {
        let pairs = AuthConfig::get_pairs();

        Self { pairs }
    }
}

#[derive(Error, Debug)]
pub enum AuthError {
    #[error("Missing username")]
    MissingUsername,
    #[error("Invalid credentials")]
    InvalidCredentials,
    #[error("Internal error: {0}")]
    Internal(String),
}

#[async_trait]
impl AuthSource for Authentication {
    async fn get_password(&self, login_info: &LoginInfo) -> PgWireResult<Password> {
        let username = login_info
            .user()
            .ok_or(AuthError::MissingUsername)
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "SQLSTATE".to_string(),
                    "ERROR".to_string(),
                    e.to_string(),
                )))
            })?
            .to_string();

        let password = self
            .pairs
            .get(&username)
            .ok_or(AuthError::InvalidCredentials)
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "SQLSTATE".to_string(),
                    "ERROR".to_string(),
                    e.to_string(),
                )))
            })?;

        Ok(Password::new(None, password.as_bytes().to_vec()))
    }
}

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
        let sql = SqlParser::new(D::get_mapping(), self.semantic_model.clone())
            .parse(query)
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "SQLSTATE".to_string(),
                    "ERROR".to_string(), // Add this line
                    e.to_string(),
                )))
            })?;
        debug!("Transformed query: {}", sql);

        // Execute the sql and return the result
        let result = self.data_store.execute(&sql).await.map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "SQLSTATE".to_string(),
                "ERROR".to_string(), // Add this line
                e.to_string(),
            )))
        })?;

        Ok(result)
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
