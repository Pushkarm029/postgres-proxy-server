mod transformations;

use crate::data_store::DataStoreMapping;
use crate::semantic_model::SemanticModelStore;
use sqlparser::ast::*;
use sqlparser::parser::Parser;
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum SqlError {
    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("SQL parsing error: {0}")]
    SqlParseError(String),

    #[error("MEASURE function error: {0}")]
    MeasureFunctionError(String),

    #[error("Transformation error: {0}")]
    SqlTransformationError(String),

    #[error("Column not found error: {0} in table {1}")]
    SqlColumnNotFoundError(String, String),

    #[error("Information schema result: {0:?}")]
    InformationSchemaResult(Vec<String>),

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
    pub fn new(data_store_mapping: M, semantic_model: S) -> Self {
        SqlParser {
            data_store_mapping,
            semantic_model,
        }
    }

    pub fn transform(&self, query: &str) -> Result<String, SqlError> {
        let output_queries: Result<Vec<String>, SqlError> = self
            .parse(query)?
            .into_iter()
            .map(|statement| match statement {
                Statement::Query(mut query) => transformations::apply_transformations(
                    &mut query,
                    &self.data_store_mapping,
                    &self.semantic_model,
                )
                .and_then(|_| Ok(Statement::Query(query)))
                .map(|stmt| stmt.to_string()),
                _ => Err(SqlError::PermissionDenied(
                    "Only read-only SQL statements are allowed".to_string(),
                )),
            })
            .collect();

        output_queries.map(|queries| queries.join(";\n"))
    }

    fn parse(&self, query: &str) -> Result<Vec<Statement>, SqlError> {
        let data_store = self.data_store_mapping.get_dialect();
        let statements = Parser::parse_sql(data_store, query)
            .map_err(|e| SqlError::SqlParseError(e.to_string()))?;
        Ok(statements)
    }
}

#[cfg(test)]
mod test {
    use super::{SqlError, SqlParser};
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
    #[case::wildcard(
        "SELECT * FROM dm_employees;",
        "SELECT department_level_1, id, included_in_headcount FROM dm_employees"
    )]
    #[case::simple_query(
        "SELECT department_level_1, MEASURE(dm_employees.headcount) FROM dm_employees;",
        "SELECT department_level_1, COUNT(dm_employees.id) AS headcount FROM dm_employees"
    )]
    #[case::simple_query_two(
        "SELECT department_level_1, MEASURE(dm_employees.headcount) AS headcount FROM dm_employees;",
        "SELECT department_level_1, COUNT(dm_employees.id) AS headcount FROM dm_employees"
    )]
    #[ignore = "Handle locally created table names"]
    #[case::query_with_cte(
        "WITH cte AS (SELECT department_level_1, MEASURE(dm_employees.headcount) FROM dm_employees) SELECT * FROM cte;",
        "WITH cte AS (SELECT department_level_1, COUNT(dm_employees.id) AS headcount FROM dm_employees) SELECT * FROM cte"
    )]
    #[case::measure_alias_should_be_ignored_first(
        "SELECT department_level_1, MEASURE(dm_employees.headcount) AS 'MEASURE(headcount)' FROM dm_employees;",
        "SELECT department_level_1, COUNT(dm_employees.id) AS 'MEASURE(headcount)' FROM dm_employees"
    )]
    #[ignore = "Handle locally created table names"]
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
    #[ignore = "Handle locally created table names"]
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
    #[test_log::test]
    fn test_parser_on_postgres(#[case] initial_query: &str, #[case] expected_query: &str) {
        let sql_parser = sql_parser_fixture();
        let transformed_query = sql_parser.transform(initial_query).unwrap();
        assert_eq!(expected_query, transformed_query);
    }

    #[rstest]
    #[case::test_now_function(
        "SELECT id, now() AS now FROM dm_employees",
        "SELECT id, CURRENT_TIMESTAMP() AS now FROM dm_employees"
    )]
    fn test_func_parser_on_postgres(#[case] initial_query: &str, #[case] expected_query: &str) {
        let sql_parser = snowflake_parser_fixture();
        let transformed_query = sql_parser.transform(initial_query).unwrap();
        assert_eq!(expected_query, transformed_query.to_string());
    }

    #[rstest]
    #[case::simple_update("UPDATE employees SET salary = 60000 WHERE employee_id = 101;")]
    #[case::multiple_column_update("UPDATE products SET price = 49.99, stock_quantity = stock_quantity - 10 WHERE product_id = 456;")]
    #[case::subquery_update("UPDATE orders SET total_amount = (SELECT SUM(price * quantity) FROM order_items WHERE order_items.order_id = orders.order_id) WHERE order_id = 1234;")]
    #[case::conditional_update("UPDATE users SET status = CASE WHEN last_login IS NULL THEN 'Inactive' WHEN last_login < NOW() - INTERVAL '1 YEAR' THEN 'Inactive' ELSE 'Active' END;")]
    fn test_reject_modify_function(#[case] query: &str) {
        let sql_parser = sql_parser_fixture();
        let transformed_query = sql_parser.transform(query);
        assert!(transformed_query.is_err());
    }

    // Special case for information schema result
    #[rstest]
    #[case::query_information_schema_tables(
        "SELECT * FROM information_schema.tables;",
        vec!["dm_departments".to_string(), "dm_employees".to_string()]
    )]
    fn test_information_schema_result_body(
        #[case] query: &str,
        #[case] expected_body: Vec<String>,
    ) {
        let sql_parser = sql_parser_fixture();
        assert_eq!(
            sql_parser.transform(query),
            Err(SqlError::InformationSchemaResult(expected_body))
        );
    }

    #[rstest]
    #[case::column_not_found("SELECT headcount FROM dm_employees;", "headcount", "dm_employees")]
    #[case::column_not_found("SELECT age, health FROM dm_employees;", "age", "dm_employees")]
    fn test_column_not_found_error(
        #[case] query: &str,
        #[case] missing_column: String,
        #[case] table: String,
    ) {
        let sql_parser = sql_parser_fixture();
        assert_eq!(
            sql_parser.transform(query),
            Err(SqlError::SqlColumnNotFoundError(missing_column, table))
        );
    }
}
