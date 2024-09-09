use async_trait::async_trait;
use chrono::Local;
use pgwire::{
    api::{
        auth::noop::NoopStartupHandler,
        copy::NoopCopyHandler,
        query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler},
        results::{Response, Tag},
        PgWireHandlerFactory,
    },
    error::PgWireResult,
    tokio::process_socket,
};
use std::sync::Arc;
use tokio::net::TcpListener;

pub struct Processor;

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

        Ok(vec![Response::Execution(Tag::new("OK").with_rows(1))])
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
        handler: Arc::new(Processor),
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
