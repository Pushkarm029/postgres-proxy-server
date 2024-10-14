mod data_store;
mod processor;
mod query_handler;
mod semantic_model;
mod server;
mod sql_parser;
mod utils;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    server::main()
}
