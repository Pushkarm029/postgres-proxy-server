use std::thread;

#[cfg(test)]
use super::*;
use sqlx::{postgres::PgPoolOptions, query, Postgres};
use tokio_postgres::NoTls;

const DB_ADDRESS: &str = "postgres://postgres:postgres@localhost:5432";
const SCHEMA_DB_ADDRESS: &str = "postgres://postgres:postgres@localhost:5432/test_schema";
const TEST_DB_ADDRESS: &str = "postgres://postgres:postgres@localhost:5432/test_db";

const PROXY_DB_ADDRESS: &str = "postgres://postgres:postgres@127.0.0.1:5433/test_db";

async fn init_db(pool: &sqlx::PgPool, name: &str) -> sqlx::Result<()> {
    sqlx::query(format!("CREATE DATABASE {name}").as_str())
        .execute(pool)
        .await?;
    Ok(())
}

async fn populate_schema_db(pool: &sqlx::PgPool) -> sqlx::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS measures (
                name TEXT PRIMARY KEY,
                query TEXT NOT NULL
            )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "INSERT INTO measures (name, query) VALUES ($1, $2), ($3, $4), ($5, $6)
             ON CONFLICT (name) DO UPDATE SET query = EXCLUDED.query",
    )
    .bind("head_count")
    .bind("COUNT(id)")
    .bind("revenue")
    .bind("SUM(amount)")
    .bind("average_salary")
    .bind("AVG(salary)")
    .execute(pool)
    .await?;

    Ok(())
}

async fn setup_main_db(pool: &sqlx::PgPool) -> sqlx::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50),
                email VARCHAR(50),
                salary NUMERIC,
                amount NUMERIC
            )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "INSERT INTO users (name, email, salary, amount) VALUES
            ($1, $2, $3, $4), ($5, $6, $7, $8), ($9, $10, $11, $12)",
    )
    .bind("John Doe")
    .bind("john.doe@example.com")
    .bind(50000)
    .bind(1000)
    .bind("Jane Smith")
    .bind("jane.smith@example.com")
    .bind(60000)
    .bind(1500)
    .bind("Alice Johnson")
    .bind("alice.johnson@example.com")
    .bind(55000)
    .bind(1200)
    .execute(pool)
    .await?;

    Ok(())
}

async fn cleanup() {
    let schema_cleanup_pool = PgPoolOptions::new().connect(DB_ADDRESS).await.unwrap();
    sqlx::query("DROP DATABASE test_schema WITH (FORCE);")
        .execute(&schema_cleanup_pool)
        .await
        .unwrap();

    let main_cleanup_pool = PgPoolOptions::new().connect(DB_ADDRESS).await.unwrap();
    sqlx::query("DROP DATABASE test_db WITH (FORCE);")
        .execute(&main_cleanup_pool)
        .await
        .unwrap();
}

#[sqlx::test]
async fn e2e_test() {
    // Setup schema database
    // let schema_creation_pool = PgPoolOptions::new().connect(DB_ADDRESS).await.unwrap();
    // init_db(&schema_creation_pool, "test_schema").await.unwrap();

    // let schema_population_pool = PgPoolOptions::new()
    //     .connect(SCHEMA_DB_ADDRESS)
    //     .await
    //     .unwrap();
    // populate_schema_db(&schema_population_pool).await.unwrap();

    // let main_creation_pool = PgPoolOptions::new().connect(DB_ADDRESS).await.unwrap();
    // init_db(&main_creation_pool, "test_db").await.unwrap();

    // let main_population_pool = PgPoolOptions::new().connect(TEST_DB_ADDRESS).await.unwrap();

    // setup_main_db(&main_population_pool).await.unwrap();

    // Start the server in a separate task
    // let server = tokio::spawn(async {
    //     main();
    // });
    // main();

    // Run TCP Server in another runtime
    let handler = thread::spawn(|| main());
    // let _some = thread::spawn(|| {
    //     main();
    // }).join().expect("Thread Panicked");

    // let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    // let server_task = tokio::spawn(async move {
    //     run_server(shutdown_receiver).await.expect("Server error");
    // });

    // Give the server some time to start
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let proxy_pool = PgPoolOptions::new()
        .connect(PROXY_DB_ADDRESS)
        .await
        .unwrap();

    // handler.join();
    // let pool: tokio::task::JoinHandle<Pool<Postgres>> = tokio::spawn(async {
    //     PgPoolOptions::new().connect(PROXY_DB_ADDRESS).await.unwrap()
    // });

    // Execute queries with MEASURE function
    // let head_count: (i64,) = sqlx::query_as("SELECT COUNT(id) FROM employees GROUP BY name;")
    //     .fetch_one(&proxy_pool)
    //     .await
    //     .unwrap();

    // let revenue: (f64,) = sqlx::query_as("SELECT MEASURE(revenue) AS total_revenue")
    //     .fetch_one(&proxy_pool)
    //     .await
    //     .unwrap();

    // let avg_salary: (f64,) = sqlx::query_as("SELECT MEASURE(average_salary) AS avg_salary")
    //     .fetch_one(&proxy_pool)
    //     .await
    //     .unwrap();

    // Check the results
    // assert_eq!(head_count.0, 3);
    // assert_eq!(revenue.0, 3700.0);
    // assert_eq!(avg_salary.0, 55000.0);

    // Test a regular SELECT query

    let sss = query("SELECT * FROM users;")
        .fetch_all(&proxy_pool)
        .await
        .unwrap();
    // let (client, _connection) = tokio_postgres::connect(PROXY_DB_ADDRESS, NoTls)
    // .await
    // .expect("Failed to connect to database");

    // // client.execute(statement, params)
    // let stmt = client.prepare("SELECT * FROM users ORDER BY id;").await.unwrap();
    // let sss = client.query(&stmt, &[]).await.unwrap();

    // println!("{:?}",sss);
    info!("{:?}", sss.len());
    info!("{:?}", sss);

    // let users: Vec<(i32, String, String, f64, f64)> =
    //     sqlx::query_as("SELECT * FROM users ORDER BY id;")
    //         .fetch_all(&proxy_pool)
    //         .await
    //         .unwrap();

    // assert_eq!(users.len(), 3);
    // assert_eq!(users[0].1, "John Doe");
    // assert_eq!(users[1].1, "Jane Smith");
    // assert_eq!(users[2].1, "Alice Johnson");

    // // Clean up
    // shutdown_sender
    //     .send(())
    //     .expect("Failed to send shutdown signal");

    // // Wait for the server to shut down
    // server_task.await.expect("Server task panicked");
    // cleanup().await;
    // server.abort();
}

// not implemented: Extended Query is not implemented on this server.
// note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
// thread 'tests::e2e_test' panicked at src/tests.rs:170:14:
// called `Result::unwrap()` on an `Err` value: Io(Custom { kind: UnexpectedEof, error: "expected to read 5 bytes, got 0 bytes at EOF" })
