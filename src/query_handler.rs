use crate::data_store::DataStoreClient;
use crate::semantic_model::SemanticModelStore;
use crate::sql_parser::SqlParser;
use pgwire::api::results::Response;
use pgwire::error::PgWireResult;
use pgwire::error::{ErrorInfo, PgWireError};

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
        // Parse the query using the sql parser
        let sql = SqlParser::new(D::get_mapping(), self.semantic_model.clone())
            .parse(query)
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "SQLSTATE".to_string(),
                    "ERROR".to_string(), // Add this line
                    e.to_string(),
                )))
            })?;

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
