use crate::data_store::DataStore;
use crate::semantic_model::SemanticModelStore;
use crate::sql_parser::transformations;
use sqlparser::ast::*;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SqlParserError {
    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("SQL parsing error: {0}")]
    SqlParseError(String),

    #[error("SQL generation error: {0}")]
    SqlGenerationError(String),

    #[error("MEASURE function error: {0}")]
    MeasureFunctionError(String),
}

pub struct SqlParser<D, S> {
    data_store: D,
    semantic_model: S,
}

impl<D, S> SqlParser<D, S>
where
    D: DataStore,
    S: SemanticModelStore,
{
    pub fn new(data_store: D, semantic_model: S) -> Self {
        SqlParser {
            data_store,
            semantic_model,
        }
    }

    pub fn parse(&self, query: &str) -> Result<String, SqlParserError> {
        let ast_list = self.parse_query(query)?;
        let transformed_ast_list = self.transform_ast(ast_list)?;

        let output_queries: Result<Vec<String>, SqlParserError> = transformed_ast_list
            .into_iter()
            .map(|ast| self.ast_to_sql(ast))
            .collect();

        Ok(output_queries?.join(";\n"))
    }

    fn parse_query(&self, query: &str) -> Result<Vec<Statement>, SqlParserError> {
        let data_store = PostgreSqlDialect {};
        let statements = Parser::parse_sql(&data_store, query)
            .map_err(|e| SqlParserError::SqlParseError(e.to_string()))?;
        Ok(statements)
    }

    fn transform_ast(&self, ast: Vec<Statement>) -> Result<Vec<Statement>, SqlParserError> {
        ast.into_iter()
            .map(|statement| match statement {
                Statement::Query(mut query) => {
                    transformations::apply_transformations(
                        &mut query,
                        &self.data_store,
                        &self.semantic_model,
                    )?;
                    Ok(Statement::Query(query))
                }
                _ => Err(SqlParserError::PermissionDenied(
                    "Only read-only SQL statements are allowed".to_string(),
                )),
            })
            .collect()
    }

    fn ast_to_sql(&self, ast: Statement) -> Result<String, SqlParserError> {
        Ok(ast.to_string())
    }
}
