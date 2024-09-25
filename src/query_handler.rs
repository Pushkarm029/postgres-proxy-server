use crate::schema::replace_measure_with_expression;
use crate::utils::config::Config;
use crate::utils::encoding::{encode_row_data, row_desc_from_stmt};
use envconfig::Envconfig;
use log::{debug, info, warn};
use pgwire::api::{
    portal::Format,
    results::{QueryResponse, Response, Tag},
};
use pgwire::error::{PgWireError, PgWireResult};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};

pub async fn handle_query(
    client: Arc<Mutex<Client>>,
    initial_query: &str,
) -> PgWireResult<Vec<Response>> {
    info!("Received query: {:?}", initial_query);

    let client = client.lock().await;
    let config = Config::init_from_env().unwrap();

    let (schema_client, schema_connection) =
        tokio_postgres::connect(&config.schema_db_conn, NoTls)
            .await
            .expect("Failed to connect to database");

    tokio::spawn(async move {
        if let Err(e) = schema_connection.await {
            eprintln!("schema connection error: {}", e);
        }
    });

    let prepared_query = replace_measure_with_expression(&schema_client, initial_query).await;

    debug!(
        "OLD Query : {}, NEW Query : {}",
        initial_query, prepared_query
    );

    if prepared_query.to_uppercase().starts_with("SELECT") {
        handle_select_query(&client, &prepared_query).await
    } else {
        handle_other_query(&client, &prepared_query).await
    }
}

async fn handle_select_query<'a>(client: &Client, query: &str) -> PgWireResult<Vec<Response<'a>>> {
    let stmt = client
        .prepare(query)
        .await
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
    let rows = client
        .query(&stmt, &[])
        .await
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

    let field_info = row_desc_from_stmt(&stmt, &Format::UnifiedText)?;
    let field_info_arc = Arc::new(field_info);

    let data_rows = encode_row_data(rows, field_info_arc.clone());

    Ok(vec![Response::Query(QueryResponse::new(
        field_info_arc,
        Box::pin(data_rows),
    ))])
}

async fn handle_other_query<'a>(client: &Client, query: &str) -> PgWireResult<Vec<Response<'a>>> {
    if query.starts_with("UPDATE") || query.starts_with("WRITE") || query.starts_with("INSERT") {
        warn!(
            "{} operation detected! ⚠️ This operation is not allowed.",
            if query.starts_with("UPDATE") {
                "UPDATE"
            } else {
                "WRITE/INSERT"
            }
        );
        return Err(PgWireError::ApiError(Box::new(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "{} query is not Accepted",
                if query.starts_with("UPDATE") {
                    "UPDATE"
                } else {
                    "WRITE or INSERT"
                }
            ),
        ))));
    }

    client
        .execute(query, &[])
        .await
        .map(|affected_rows| {
            vec![Response::Execution(
                Tag::new("OK").with_rows(affected_rows.try_into().unwrap()),
            )]
        })
        .map_err(|e| PgWireError::ApiError(Box::new(e)))
}
