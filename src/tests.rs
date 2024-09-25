#[cfg(test)]
use super::*;
use schema::replace_measure_with_expression;
use tokio_postgres::NoTls;

// const DB_ADDRESS: &str = "postgres://postgres:postgres@localhost:5432";
const SCHEMA_DB_ADDRESS: &str = "postgres://postgres:postgres@localhost:5432/information_schema";
// const TEST_DB_ADDRESS: &str = "postgres://postgres:postgres@localhost:5432/test_db";
// const PROXY_DB_ADDRESS: &str = "postgres://postgres:postgres@127.0.0.1:5433/main";

#[tokio::test]
async fn test_query_modifier() {
    let (client, connection) = tokio_postgres::connect(SCHEMA_DB_ADDRESS, NoTls)
        .await
        .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    let initial_query: &str = "SELECT name, MEASURE(head_count) FROM employees GROUP BY name;";
    let expected_final_query: &str = "SELECT name, COUNT(id) FROM employees GROUP BY name";
    let final_query: String = replace_measure_with_expression(&client, initial_query).await;

    assert_eq!(expected_final_query, final_query);
}
