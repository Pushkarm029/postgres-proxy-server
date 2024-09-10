use async_trait::async_trait;
use chrono::Local;
use futures::Stream;
use pgwire::{
    api::{
        auth::noop::NoopStartupHandler,
        copy::NoopCopyHandler,
        portal::Format,
        query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler},
        results::{FieldInfo, QueryResponse, Response, Tag},
        PgWireHandlerFactory,
    },
    error::{PgWireError, PgWireResult},
    messages::data::DataRow,
    tokio::process_socket,
};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls, Row, Statement};

pub struct Processor {
    client: Arc<Mutex<Client>>,
}

const ADDRESS: &str = "postgres://postgres:postgres@localhost:5432";
const DB_ADDRESS: &str = "postgres://postgres:postgres@localhost:5432/new";

#[async_trait]
impl SimpleQueryHandler for Processor {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>> {
        println!(
            "[{} INFO] Received query: {:?}",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            query
        );
        let client = self.client.lock().await;

        if query.to_uppercase().starts_with("SELECT") {
            let stmt = client
                .prepare(query)
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))
                .unwrap();

            // let header = Arc::new(row_desc_from_stmt(&stmt, &Format::UnifiedText).unwrap());
            client
                .query(query, &[])
                .await
                .map(|rows| {
                    vec![Response::Execution(
                        Tag::new("OK").with_rows(rows.len().try_into().unwrap()),
                    )]
                    // let s = encode_row_data(rows, header.clone());
                    // let s =
                    // vec![Response::Query(QueryResponse::new(header, s))]
                })
                .map_err(|e| PgWireError::ApiError(Box::new(e)))
        } else {
            if query.starts_with("UPDATE") {
                println!(
                    "[{} WARNING] UPDATE operation detected! ⚠️ This will modify existing data.",
                    Local::now().format("%Y-%m-%d %H:%M:%S")
                );
            }

            if query.starts_with("WRITE") || query.starts_with("INSERT") {
                println!(
                    "[{} WARNING] WRITE operation detected! ⚠️ Writing new data may impact database integrity if not handled carefully.",
                    Local::now().format("%Y-%m-%d %H:%M:%S")
                );
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
    }
}

impl Processor {
    async fn new() -> Self {
        let (client, connection) = tokio_postgres::connect(DB_ADDRESS, NoTls)
            .await
            .unwrap_or_else(|e| panic!("Failed to connect to database: {}", e));

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Self {
            client: Arc::new(Mutex::new(client)),
        }
    }
}

fn row_desc_from_stmt(stmt: &Statement, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
    stmt.columns()
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            let field_type = col.type_();
            Ok(FieldInfo::new(
                col.name().to_owned(),
                None,
                None,
                field_type.clone(),
                format.format_for(idx),
            ))
        })
        .collect()
}

// fn encode_row_data(
//     mut rows: Vec<Row>,
//     schema: Arc<Vec<FieldInfo>>
// ) -> impl Stream<Item = PgWireResult<DataRow>>{
//     let mut results = Vec::new();
//     let ncols = schema.len();

//     for row in rows.iter_mut() {
//     }
//     // while let Ok(Some(row)) =  {

//     // }
//     // Ok(DataRow::new(row.into_iter(), ncols.try_into().unwrap()))
// }

struct ProcessorFactory {
    handler: Arc<Processor>,
}

impl PgWireHandlerFactory for ProcessorFactory {
    type StartupHandler = NoopStartupHandler;
    type SimpleQueryHandler = Processor;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(NoopStartupHandler)
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }
}

async fn create_db_if_not_exists() {
    let (client, connection) = tokio_postgres::connect(ADDRESS, NoTls).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE DATABASE new", &[]).await.unwrap();

    println!("Database 'new' created successfully!");
}

#[tokio::main]
pub async fn main() {
    create_db_if_not_exists().await;

    let factory = Arc::new(ProcessorFactory {
        handler: Arc::new(Processor::new().await),
    });

    let server_addr = "127.0.0.1:5433";

    println!(
        "[{} INFO] Starting server at {}",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        server_addr
    );

    let listener = TcpListener::bind(server_addr).await.unwrap();

    println!(
        "[{} INFO] Listening for connections on {}",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        server_addr
    );

    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let factory_ref = factory.clone();

        println!(
            "[{} INFO] New connection accepted from: {}",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            incoming_socket.1
        );

        tokio::spawn(async move { process_socket(incoming_socket.0, None, factory_ref).await });
    }
}
