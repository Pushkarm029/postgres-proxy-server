use crate::data_store::{DataStore, Row};
use crate::semantic_model::SemanticModelStore;
use crate::sql_parser::SqlParser;
use pgwire::error::{PgWireError, ErrorInfo};

pub struct QueryHandler<'a> {
    data_store: &'a dyn DataStore,
    semantic_model: &'a dyn SemanticModelStore,
}

impl<'a> QueryHandler<'a> {
    pub fn new(data_store: &'a dyn DataStore, semantic_model: &'a dyn SemanticModelStore) -> Self {
        Self {
            data_store,
            semantic_model,
        }
    }

    pub async fn handle(&self, query: &str) -> Result<Vec<Row>, PgWireError> {
        // Parse the query using the sql parser
        let sql = SqlParser::new(self.data_store, self.semantic_model)
            .parse(query)
            .map_err(|e| PgWireError::UserError(Box::new(ErrorInfo::new(
                "SQLSTATE".to_string(),
                "ERROR".to_string(), // Add this line
                e.to_string(),
            ))))?;

        // Execute the sql and return the result
        let result = self
            .data_store
            .execute(&sql)
            .map_err(|e| PgWireError::UserError(Box::new(ErrorInfo::new(
                "SQLSTATE".to_string(),
                "ERROR".to_string(), // Add this line
                e.to_string(),
            ))))?;
        
        Ok(result)
    }
}
