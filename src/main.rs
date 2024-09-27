mod data_store;
mod processor;
mod query_handler;
mod semantic_model;
mod server;
mod sql_parser;
mod utils;

fn main() {
    server::main();
}
