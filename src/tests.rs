#[cfg(test)]
use super::*;
use schema::replace_measure_with_expression;
use tokio_postgres::NoTls;

// const DATA_DB_CONN_STRING: &str = "postgres://postgres:postgres@localhost:5432";
const SCHEMA_DB_CONN_STRING: &str = "postgres://postgres:postgres@localhost:5432/information_schema";
// const TEST_DB_ADDRESS: &str = "postgres://postgres:postgres@localhost:5432/test_db";
// const PROXY_DB_ADDRESS: &str = "postgres://postgres:postgres@127.0.0.1:5433/main";

#[tokio::test]
async fn test_query_modifier() {
    let (schema_client, connection) = tokio_postgres::connect(SCHEMA_DB_CONN_STRING, NoTls)
        .await
        .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    let initial_query: &str = "SELECT name, MEASURE(head_count) FROM employees GROUP BY name;";
    let expected_final_query: &str = "SELECT name, COUNT(id) FROM employees GROUP BY name";
    let final_query: String = replace_measure_with_expression(&schema_client, initial_query).await;

    assert_eq!(expected_final_query, final_query);
}
