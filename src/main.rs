use async_trait::async_trait;
use bytes::BytesMut;
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
use sqlx::{postgres::PgConnection, Connection};
use std::sync::Arc;
use tokio::{net::TcpListener, sync::Mutex};
use tokio_postgres::{types::Type, Client, NoTls, Row, Statement};
// It also provides a SnowflakeSqlDialect which may be useful for us?
use sqlparser::ast::{Expr, SelectItem};
use sqlparser::{ast::FunctionArguments, dialect::PostgreSqlDialect};
use sqlparser::{
    ast::{FunctionArg, FunctionArgExpr, Ident},
    parser::Parser,
};

pub struct Processor {
    client: Arc<Mutex<Client>>,
}

const DB_ADDRESS: &str = "postgres://postgres:postgres@localhost:5432/new";
const SCHEMA_DB_ADDRESS: &str = "postgres://postgres:postgres@localhost:5432/information_schema";

#[async_trait]
impl SimpleQueryHandler for Processor {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        initial_query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>> {
        println!(
            "[{} INFO] Received query: {:?}",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            initial_query
        );
        let client = self.client.lock().await;

        // parse sql
        let dialect = PostgreSqlDialect {};
        let mut ast = Parser::parse_sql(&dialect, initial_query)
            .unwrap_or_else(|e| panic!("Fails to parse the sql to AST: {}", e));

        replace_measure_with_expression(&mut ast).await;

        let query = ast[0].to_string();
        println!("Old Query -> {}", initial_query);
        println!("New Query -> {}", query);

        // now, unparse the AST
        // pass this query

        if query.to_uppercase().starts_with("SELECT") {
            let stmt = client
                .prepare(&query)
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
        } else {
            if query.starts_with("UPDATE") {
                println!(
                    "[{} WARNING] UPDATE operation detected! ⚠️ This will modify existing data.",
                    Local::now().format("%Y-%m-%d %H:%M:%S")
                );
                return Err(PgWireError::ApiError(Box::new(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "UPDATE query is not Accepted",
                ))));
            }

            if query.starts_with("WRITE") || query.starts_with("INSERT") {
                // TODO: use logger here
                println!(
                    "[{} WARNING] WRITE operation detected! ⚠️ Writing new data may impact database integrity if not handled carefully.",
                    Local::now().format("%Y-%m-%d %H:%M:%S")
                );
                return Err(PgWireError::ApiError(Box::new(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "WRITE or INSERT query is not Accepted",
                ))));
            }

            client
                .execute(&query, &[])
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

async fn replace_measure_with_expression(ast: &mut [sqlparser::ast::Statement]) {
    for statement in ast.iter_mut() {
        if let sqlparser::ast::Statement::Query(query) = statement {
            if let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() {
                for proj in select.projection.iter_mut() {
                    if let SelectItem::UnnamedExpr(Expr::Function(func)) = proj {
                        if func.name.0[0].value == "MEASURE" {
                            if let FunctionArguments::List(list) = &mut func.args {
                                for item in list.args.iter_mut() {
                                    *item = FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                        Expr::Identifier(Ident::new(
                                            get_query_from_schema(item.to_string()).await,
                                        )),
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[derive(sqlx::FromRow, Debug, PartialEq, Eq)]
struct SchemaQuery(String);

async fn get_query_from_schema(old_arg: String) -> String {
    let mut conn = PgConnection::connect(SCHEMA_DB_ADDRESS)
        .await
        .unwrap_or_else(|e| panic!("Failed to connect to database: {}", e));

    let new_arg: SchemaQuery = sqlx::query_as(&format!(
        "SELECT query FROM information_schema.measures WHERE name = '{}';",
        old_arg
    ))
    .fetch_one(&mut conn)
    .await
    .unwrap();

    new_arg.0
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

// Use SQLx
fn encode_row_data(
    rows: Vec<Row>,
    schema: Arc<Vec<FieldInfo>>,
) -> impl Stream<Item = PgWireResult<DataRow>> {
    futures::stream::iter(rows.into_iter().map(move |row| {
        let mut buffer = BytesMut::new();
        for (idx, field) in schema.iter().enumerate() {
            let pg_type = field.datatype();
            match pg_type {
                &Type::INT4 => {
                    let value: Option<i32> = row.get(idx);
                    encode_value(&mut buffer, value.map(|v| v.to_string()));
                }
                &Type::TEXT | &Type::VARCHAR => {
                    let value: Option<String> = row.get(idx);
                    encode_value(&mut buffer, value);
                }
                &Type::BOOL => {
                    let value: Option<bool> = row.get(idx);
                    encode_value(&mut buffer, value.map(|v| v.to_string()));
                }
                &Type::FLOAT4 => {
                    let value: Option<f32> = row.get(idx);
                    encode_value(&mut buffer, value.map(|v| v.to_string()));
                }
                &Type::FLOAT8 => {
                    let value: Option<f64> = row.get(idx);
                    encode_value(&mut buffer, value.map(|v| v.to_string()));
                }
                _ => {
                    encode_value(&mut buffer, None::<String>);
                    println!("Unexpected Type")
                }
            }
        }
        Ok(DataRow::new(buffer, schema.len() as i16))
    }))
}

fn encode_value(buffer: &mut BytesMut, value: Option<String>) {
    match value {
        Some(v) => {
            buffer.extend_from_slice(&(v.len() as i32).to_be_bytes());
            buffer.extend_from_slice(v.as_bytes());
        }
        None => {
            buffer.extend_from_slice(&(-1_i32).to_be_bytes());
        }
    }
}

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

#[tokio::main]
pub async fn main() {
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
