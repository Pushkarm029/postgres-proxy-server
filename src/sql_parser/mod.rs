mod transformations;

use crate::data_store::DataStoreMapping;
use crate::semantic_model::SemanticModelStore;
use sqlparser::ast::*;
use sqlparser::parser::Parser;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SqlParserError {
    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("SQL parsing error: {0}")]
    SqlParseError(String),

    #[error("MEASURE function error: {0}")]
    MeasureFunctionError(String),
}

/// Custom error type for SQL transformation errors
#[derive(Error, Debug)]
pub enum SqlTransformError {
    #[error("Invalid MEASURE function: {0}")]
    InvalidMeasureFunction(String),
    #[error("Invalid function argument: {0}")]
    InvalidFunctionArgument(String),
    #[error("Semantic model error: {0}")]
    SemanticModelError(String),
    #[error("SQL parsing error: {0}")]
    SqlParsingError(String),
    #[error("Unsupported SQL construct: {0}")]
    UnsupportedSqlConstruct(String),
}

pub struct SqlParser<M, S> {
    data_store_mapping: M,
    semantic_model: S,
}

impl<M, S> SqlParser<M, S>
where
    M: DataStoreMapping,
    S: SemanticModelStore,
{
    pub fn new(data_store: M, semantic_model: S) -> Self {
        SqlParser {
            data_store_mapping: data_store,
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
        let data_store = self.data_store_mapping.get_dialect();
        let statements = Parser::parse_sql(data_store, query)
            .map_err(|e| SqlParserError::SqlParseError(e.to_string()))?;
        Ok(statements)
    }

    fn transform_ast(&self, ast: Vec<Statement>) -> Result<Vec<Statement>, SqlParserError> {
        ast.into_iter()
            .map(|statement| match statement {
                Statement::Query(mut query) => {
                    transformations::apply_transformations(
                        &mut query,
                        &self.data_store_mapping,
                        &self.semantic_model,
                    )
                    .map_err(|e| SqlParserError::MeasureFunctionError(e.to_string()))?;
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

#[cfg(test)]
mod test {
    use super::SqlParser;
    use crate::data_store::postgres::PostgresMapping;
    use crate::data_store::snowflake::SnowflakeMapping;
    use crate::semantic_model::local_store::LocalSemanticModelStore;
    use rstest::*;

    #[fixture]
    fn sql_parser_fixture() -> SqlParser<PostgresMapping, LocalSemanticModelStore> {
        let mapping = PostgresMapping {};
        let sm = LocalSemanticModelStore::mock();
        SqlParser::new(mapping, sm)
    }

    #[fixture]
    fn snowflake_parser_fixture() -> SqlParser<SnowflakeMapping, LocalSemanticModelStore> {
        let mapping = SnowflakeMapping {};
        let sm = LocalSemanticModelStore::mock();
        SqlParser::new(mapping, sm)
    }

    #[rstest]
    #[case::simple_query(
        "SELECT department_level_1, MEASURE(dm_employees.headcount) FROM dm_employees;",
        "SELECT department_level_1, COUNT(dm_employees.id) AS headcount FROM dm_employees"
    )]
    #[case::simple_query_two(
        "SELECT department_level_1, MEASURE(dm_employees.headcount) AS headcount FROM dm_employees;",
        "SELECT department_level_1, COUNT(dm_employees.id) AS headcount FROM dm_employees"
    )]
    #[case::query_with_cte(
        "WITH cte AS (SELECT department_level_1, MEASURE(dm_employees.headcount) FROM dm_employees) SELECT * FROM cte;",
        "WITH cte AS (SELECT department_level_1, COUNT(dm_employees.id) AS headcount FROM dm_employees) SELECT * FROM cte"
    )]
    #[case::measure_alias_should_be_ignored_first(
        "SELECT department_level_1, MEASURE(dm_employees.headcount) AS 'MEASURE(headcount)' FROM dm_employees;",
        "SELECT department_level_1, COUNT(dm_employees.id) AS 'MEASURE(headcount)' FROM dm_employees"
    )]
    #[case::measure_alias_should_be_ignored_second(
        "WITH cte AS (SELECT department_level_1, MEASURE(dm_employees.headcount) AS 'measure_headcount' FROM dm_employees) SELECT * FROM cte;",
        "WITH cte AS (SELECT department_level_1, COUNT(dm_employees.id) AS 'measure_headcount' FROM dm_employees) SELECT * FROM cte"
    )]
    #[case::test_multiple_tables(
        "SELECT dm_departments.department_level_1_name, MEASURE(dm_employees.headcount) FROM dm_employees LEFT JOIN dm_departments ON dm_employees.department_level_1 = dm_departments.department_level_1;",
        "SELECT dm_departments.department_level_1_name, COUNT(dm_employees.id) AS headcount FROM dm_employees LEFT JOIN dm_departments ON dm_employees.department_level_1 = dm_departments.department_level_1"
    )]
    #[case::test_multiple_measures(
        "SELECT department_level_1, MEASURE(dm_employees.headcount), MEASURE(dm_employees.ending_headcount) FROM dm_employees;",
        "SELECT department_level_1, COUNT(dm_employees.id) AS headcount, COUNT(DISTINCT dm_employees.effective_date) AS ending_headcount FROM dm_employees"
    )]
    #[case::test_union(
        "SELECT department_level_1, MEASURE(dm_employees.headcount), false as is_total FROM dm_employees UNION SELECT null as department_level_1, MEASURE(dm_employees.headcount), true as is_total FROM dm_employees;",
        "SELECT department_level_1, COUNT(dm_employees.id) AS headcount, false AS is_total FROM dm_employees UNION SELECT NULL AS department_level_1, COUNT(dm_employees.id) AS headcount, true AS is_total FROM dm_employees"
    )]
    #[case::test_subquery(
        "SELECT subquery.department_level_1, MEASURE(dm_employees.headcount) FROM (SELECT * FROM dm_employees) AS subquery;",
        "SELECT subquery.department_level_1, COUNT(dm_employees.id) AS headcount FROM (SELECT * FROM dm_employees) AS subquery"
    )]
    #[case::test_case_statement(
        "SELECT CASE WHEN department_level_1 = 'a' THEN 'a' WHEN department_level_1 = 'b' THEN 'b' ELSE 'c' END as case_column FROM dm_employees;",
        "SELECT CASE WHEN department_level_1 = 'a' THEN 'a' WHEN department_level_1 = 'b' THEN 'b' ELSE 'c' END AS case_column FROM dm_employees"
    )]
    #[case::test_distinct_on_snowflake_dialect(
        "SELECT DISTINCT ON (department_level_1) department_level_1, MEASURE(dm_employees.headcount) FROM dm_employees;",
        "SELECT DISTINCT ON (department_level_1) department_level_1, COUNT(dm_employees.id) AS headcount FROM dm_employees"
    )]
    fn test_parser_on_postgres(#[case] initial_query: &str, #[case] expected_query: &str) {
        let sql_parser = sql_parser_fixture();
        let transformed_query = sql_parser.parse(initial_query).unwrap();
        assert_eq!(expected_query, transformed_query);
    }

    #[rstest]
    #[case::test_now_function(
        "SELECT employee_id, name, now() AS now FROM employees WHERE department = 'Sales';",
        "SELECT employee_id, name, CURRENT_TIMESTAMP() AS now FROM employees WHERE department = 'Sales'"
    )]
    fn test_func_parser_on_postgres(#[case] initial_query: &str, #[case] expected_query: &str) {
        let sql_parser = snowflake_parser_fixture();
        let transformed_query = sql_parser.parse(initial_query).unwrap();
        assert_eq!(expected_query, transformed_query.to_string());
    }

    #[rstest]
    #[case::simple_update("UPDATE employees SET salary = 60000 WHERE employee_id = 101;")]
    #[case::multiple_column_update("UPDATE products SET price = 49.99, stock_quantity = stock_quantity - 10 WHERE product_id = 456;")]
    #[case::subquery_update("UPDATE orders SET total_amount = (SELECT SUM(price * quantity) FROM order_items WHERE order_items.order_id = orders.order_id) WHERE order_id = 1234;")]
    #[case::conditional_update("UPDATE users SET status = CASE WHEN last_login IS NULL THEN 'Inactive' WHEN last_login < NOW() - INTERVAL '1 YEAR' THEN 'Inactive' ELSE 'Active' END;")]
    fn test_reject_modify_function(#[case] query: &str) {
        let sql_parser = sql_parser_fixture();
        let transformed_query = sql_parser.parse(query);
        assert!(transformed_query.is_err());
    }
}
