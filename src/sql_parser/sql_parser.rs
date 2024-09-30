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
        // println!("ast_list: {:?}\n", ast_list);
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

#[cfg(test)]
mod test {
    use super::SqlParser;
    use crate::data_store::postgres::{PostgresConfig, PostgresDataStore};
    use crate::semantic_model::local_store::LocalSemanticModelStore;
    use rstest::*;

    #[fixture]
    async fn sql_parser_fixture() -> SqlParser<PostgresDataStore, LocalSemanticModelStore> {
        let ds = PostgresDataStore::new(PostgresConfig {
            host: "localhost:5432".to_string(),
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            dbname: "main".to_string(),
        })
        .await
        .unwrap();

        let sm = LocalSemanticModelStore::new();
        SqlParser::new(ds, sm)
    }

    // MEASURE(dm_employees.headcount) FROM dm_employees LEFT JOIN dm_departments ON dm_employees.department_level_1 = dm_departments.department_level_1
    // TODO: fix this case: AS headcount : when AS is not present add measure fetched keyword in AS simple
    #[rstest]
    #[case::simple_query(
        "SELECT department_level_1, MEASURE(dm_employees.headcount) FROM dm_employees;",
        "SELECT department_level_1, COUNT(DISTINCT CASE WHEN dm_employees.included_in_headcount THEN dm_employees.id ELSE NULL END) FROM dm_employees"
    )]
    #[case::query_with_cte(
        "WITH cte AS (SELECT department_level_1, MEASURE(dm_employees.headcount) FROM dm_employees) SELECT * FROM cte;",
        "WITH cte AS (SELECT department_level_1, COUNT(DISTINCT CASE WHEN dm_employees.included_in_headcount THEN dm_employees.id ELSE NULL END) FROM dm_employees) SELECT * FROM cte"
    )]
    #[case::measure_alias_should_be_ignored_first(
        "SELECT department_level_1, MEASURE(dm_employees.headcount) AS 'MEASURE(headcount)' FROM dm_employees;",
        "SELECT department_level_1, COUNT(DISTINCT CASE WHEN dm_employees.included_in_headcount THEN dm_employees.id ELSE NULL END) AS 'MEASURE(headcount)' FROM dm_employees"
    )]
    #[case::measure_alias_should_be_ignored_second(
        "WITH cte AS (SELECT department_level_1, MEASURE(dm_employees.headcount) AS 'measure_headcount' FROM dm_employees) SELECT * FROM cte;",
        "WITH cte AS (SELECT department_level_1, COUNT(DISTINCT CASE WHEN dm_employees.included_in_headcount THEN dm_employees.id ELSE NULL END) AS 'measure_headcount' FROM dm_employees) SELECT * FROM cte"
    )]
    #[case::test_multiple_tables(
        "SELECT dm_departments.department_level_1_name, MEASURE(dm_employees.headcount) FROM dm_employees LEFT JOIN dm_departments ON dm_employees.department_level_1 = dm_departments.department_level_1;",
        "SELECT dm_departments.department_level_1_name, COUNT(DISTINCT CASE WHEN dm_employees.included_in_headcount THEN dm_employees.id ELSE NULL END) FROM dm_employees LEFT JOIN dm_departments ON dm_employees.department_level_1 = dm_departments.department_level_1"
    )]
    // IN THIS CASE, in 2nd measure, we should have distinct, but we got DISTINCT.
    #[case::test_multiple_measures(
        "SELECT department_level_1, MEASURE(dm_employees.headcount), MEASURE(dm_employees.ending_headcount) FROM dm_employees;",
        "SELECT department_level_1, COUNT(DISTINCT CASE WHEN dm_employees.included_in_headcount THEN dm_employees.id ELSE NULL END), count(DISTINCT dm_employees.effective_date) FROM dm_employees"
    )]
    #[case::test_union(
        "SELECT department_level_1, MEASURE(dm_employees.headcount), false as is_total FROM dm_employees UNION SELECT null as department_level_1, MEASURE(dm_employees.headcount), true as is_total FROM dm_employees;",
        "SELECT department_level_1, COUNT(DISTINCT CASE WHEN dm_employees.included_in_headcount THEN dm_employees.id ELSE NULL END), false AS is_total FROM dm_employees UNION SELECT NULL AS department_level_1, COUNT(DISTINCT CASE WHEN dm_employees.included_in_headcount THEN dm_employees.id ELSE NULL END), true AS is_total FROM dm_employees"
    )]
    #[case::test_subquery(
        "SELECT subquery.department_level_1, MEASURE(dm_employees.headcount) FROM (SELECT * FROM dm_employees) AS subquery;",
        "SELECT subquery.department_level_1, COUNT(DISTINCT CASE WHEN dm_employees.included_in_headcount THEN dm_employees.id ELSE NULL END) FROM (SELECT * FROM dm_employees) AS subquery"
    )]
    // #[case::interval_statement_in_dialect(
    //     "SELECT '1 day'::interval as interval_column",
    //     "SELECT INTERVAL '1 day' as interval_column"
    // )]
    // as -> AS, correct or not
    // 11, 10
    #[case::test_case_statement(
        "SELECT CASE WHEN department_level_1 = 'a' THEN 'a' WHEN department_level_1 = 'b' THEN 'b' ELSE 'c' END as case_column FROM dm_employees;",
        "SELECT CASE WHEN department_level_1 = 'a' THEN 'a' WHEN department_level_1 = 'b' THEN 'b' ELSE 'c' END AS case_column FROM dm_employees"
    )]
    #[case::test_distinct_on_snowflake_dialect(
        "SELECT DISTINCT ON (department_level_1) department_level_1, MEASURE(dm_employees.headcount) FROM dm_employees;",
        "SELECT DISTINCT ON (department_level_1) department_level_1, COUNT(DISTINCT CASE WHEN dm_employees.included_in_headcount THEN dm_employees.id ELSE NULL END) FROM dm_employees"
    )]
    #[tokio::test]
    async fn test_parser_on_postgres_dialect(
        #[future] sql_parser_fixture: SqlParser<PostgresDataStore, LocalSemanticModelStore>,
        #[case] initial_query: &str,
        #[case] expected_query: &str,
    ) {
        let sql_parser = sql_parser_fixture.await;
        let transformed_query = sql_parser.parse(initial_query).unwrap();
        assert_eq!(expected_query, transformed_query);
    }

    #[rstest]
    #[case::simple_update("UPDATE employees SET salary = 60000 WHERE employee_id = 101;")]
    #[case::multiple_column_update("UPDATE products SET price = 49.99, stock_quantity = stock_quantity - 10 WHERE product_id = 456;")]
    #[case::subquery_update("UPDATE orders SET total_amount = (SELECT SUM(price * quantity) FROM order_items WHERE order_items.order_id = orders.order_id) WHERE order_id = 1234;")]
    #[case::conditional_update("UPDATE users SET status = CASE WHEN last_login IS NULL THEN 'Inactive' WHEN last_login < NOW() - INTERVAL '1 YEAR' THEN 'Inactive' ELSE 'Active' END;")]
    #[tokio::test]
    async fn test_reject_modify_function(
        #[future] sql_parser_fixture: SqlParser<PostgresDataStore, LocalSemanticModelStore>,
        #[case] query: &str,
    ) {
        let sql_parser = sql_parser_fixture.await;
        let transformed_query = sql_parser.parse(query);
        assert!(transformed_query.is_err());
    }
}
