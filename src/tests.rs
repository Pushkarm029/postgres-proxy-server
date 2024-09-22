// #[cfg(test)]
// use super::*;
// use sqlx::Pool;
// use sqlx::{postgres::PgPoolOptions, PgPool, Postgres};
// use std::sync::Arc;
// use tokio::net::TcpStream;
// use tokio::sync::Mutex;
// use tokio_postgres::{Config, NoTls};

// const DB_ADDRESS: &str = "postgres://postgres:postgres@localhost:5432";
// const SCHEMA_DB_ADDRESS: &str = "postgres://postgres:postgres@localhost:5432/test_schema";
// const TEST_DB_ADDRESS: &str = "postgres://postgres:postgres@localhost:5432/test_db";

// const PROXY_DB_ADDRESS: &str = "postgres://postgres:postgres@127.0.0.1:5433/test_db";

// async fn init_db(pool: &sqlx::PgPool, name: &str) -> sqlx::Result<()> {
//     sqlx::query(format!("CREATE DATABASE {name}").as_str())
//         .execute(pool)
//         .await?;
//     Ok(())
// }

// async fn populate_schema_db(pool: &sqlx::PgPool) -> sqlx::Result<()> {
//     sqlx::query(
//         "CREATE TABLE IF NOT EXISTS measures (
//                 name TEXT PRIMARY KEY,
//                 query TEXT NOT NULL
//             )",
//     )
//     .execute(pool)
//     .await?;

//     sqlx::query(
//         "INSERT INTO measures (name, query) VALUES ($1, $2), ($3, $4), ($5, $6)
//              ON CONFLICT (name) DO UPDATE SET query = EXCLUDED.query",
//     )
//     .bind("head_count")
//     .bind("COUNT(id)")
//     .bind("revenue")
//     .bind("SUM(amount)")
//     .bind("average_salary")
//     .bind("AVG(salary)")
//     .execute(pool)
//     .await?;

//     Ok(())
// }

// async fn setup_main_db(pool: &sqlx::PgPool) -> sqlx::Result<()> {
//     sqlx::query(
//         "CREATE TABLE IF NOT EXISTS users (
//                 id SERIAL PRIMARY KEY,
//                 name VARCHAR(50),
//                 email VARCHAR(50),
//                 salary NUMERIC,
//                 amount NUMERIC
//             )",
//     )
//     .execute(pool)
//     .await?;

//     sqlx::query(
//         "INSERT INTO users (name, email, salary, amount) VALUES
//             ($1, $2, $3, $4), ($5, $6, $7, $8), ($9, $10, $11, $12)",
//     )
//     .bind("John Doe")
//     .bind("john.doe@example.com")
//     .bind(50000)
//     .bind(1000)
//     .bind("Jane Smith")
//     .bind("jane.smith@example.com")
//     .bind(60000)
//     .bind(1500)
//     .bind("Alice Johnson")
//     .bind("alice.johnson@example.com")
//     .bind(55000)
//     .bind(1200)
//     .execute(pool)
//     .await?;

//     Ok(())
// }

// #[sqlx::test]
// async fn e2e_test() {
//     // Setup schema database
//     let schema_creation_pool = PgPoolOptions::new().connect(DB_ADDRESS).await.unwrap();
//     init_db(&schema_creation_pool, "test_schema").await.unwrap();

//     let schema_population_pool = PgPoolOptions::new()
//         .connect(SCHEMA_DB_ADDRESS)
//         .await
//         .unwrap();
//     populate_schema_db(&schema_population_pool).await.unwrap();

//     let main_creation_pool = PgPoolOptions::new().connect(DB_ADDRESS).await.unwrap();
//     init_db(&main_creation_pool, "test_db").await.unwrap();

//     let main_population_pool = PgPoolOptions::new().connect(TEST_DB_ADDRESS).await.unwrap();

//     setup_main_db(&main_population_pool).await.unwrap();

//     // Start the server in a separate task
//     let server = tokio::spawn(async {
//         main();
//     });

//     // Give the server some time to start
//     tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

//     let proxy_pool = PgPoolOptions::new()
//         .connect(PROXY_DB_ADDRESS)
//         .await
//         .unwrap();

//     // let pool: tokio::task::JoinHandle<Pool<Postgres>> = tokio::spawn(async {
//     //     PgPoolOptions::new().connect(PROXY_DB_ADDRESS).await.unwrap()
//     // });

//     // Execute queries with MEASURE function
//     let head_count: (i64,) = sqlx::query_as("SELECT MEASURE(head_count) AS count")
//         .fetch_one(&proxy_pool)
//         .await
//         .unwrap();

//     let revenue: (f64,) = sqlx::query_as("SELECT MEASURE(revenue) AS total_revenue")
//         .fetch_one(&proxy_pool)
//         .await
//         .unwrap();

//     let avg_salary: (f64,) = sqlx::query_as("SELECT MEASURE(average_salary) AS avg_salary")
//         .fetch_one(&proxy_pool)
//         .await
//         .unwrap();

//     // Check the results
//     assert_eq!(head_count.0, 3);
//     assert_eq!(revenue.0, 3700.0);
//     assert_eq!(avg_salary.0, 55000.0);

//     // Test a regular SELECT query
//     let users: Vec<(i32, String, String, f64, f64)> =
//         sqlx::query_as("SELECT * FROM users ORDER BY id")
//             .fetch_all(&proxy_pool)
//             .await
//             .unwrap();

//     assert_eq!(users.len(), 3);
//     assert_eq!(users[0].1, "John Doe");
//     assert_eq!(users[1].1, "Jane Smith");
//     assert_eq!(users[2].1, "Alice Johnson");

//     // Clean up
//     server.abort();
// }
