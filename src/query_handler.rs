use crate::data_store::DataStore;
use crate::semantic_model::SemanticModelStore;
use crate::sql_parser::SqlParser;
use pgwire::{
    error::{ErrorInfo, PgWireError},
    messages::data::DataRow,
};

pub struct QueryHandler<D, S> {
    data_store: D,
    semantic_model: S,
}

impl<D, S> QueryHandler<D, S>
where
    D: DataStore,
    S: SemanticModelStore,
{
    pub fn new(data_store: D, semantic_model: S) -> Self {
        Self {
            data_store,
            semantic_model,
        }
    }

    pub async fn handle(&self, query: &str) -> Result<Vec<DataRow>, PgWireError> {
        // Parse the query using the sql parser
        let sql = SqlParser::new(self.data_store.clone(), self.semantic_model.clone())
            .parse(query)
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "SQLSTATE".to_string(),
                    "ERROR".to_string(), // Add this line
                    e.to_string(),
                )))
            })?;

        // Execute the sql and return the result
        let result = self.data_store.execute(&sql).map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "SQLSTATE".to_string(),
                "ERROR".to_string(), // Add this line
                e.to_string(),
            )))
        })?;

        Ok(result)
    }
}
