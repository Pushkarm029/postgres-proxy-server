use log::{info, warn};
use std::env;

pub const SCHEMA_DB_ADDRESS: &str =
    "postgres://postgres:postgres@localhost:5432/information_schema";
pub const SCHEMA_TABLE_NAME: &str = "measures";
pub const DB_ADDRESS: &str = "postgres://postgres:postgres@localhost:5432/main";
pub const SERVER_ADDR: &str = "127.0.0.1:5433";

pub fn get_schema_table_name() -> String {
    get_env_var("SCHEMA_TABLE_NAME", SCHEMA_TABLE_NAME)
}

pub fn get_schema_db_address() -> String {
    get_env_var("SCHEMA_DB_ADDRESS", SCHEMA_DB_ADDRESS)
}

pub fn get_db_address() -> String {
    get_env_var("DB_ADDRESS", DB_ADDRESS)
}

pub fn get_server_binding_address() -> String {
    get_env_var("SERVER_ADDR", SERVER_ADDR)
}

fn get_env_var(key: &str, default: &str) -> String {
    match env::var(key) {
        Ok(val) => {
            info!("Found Environment Variable for {}: {}", key, val);
            val
        }
        Err(_e) => {
            warn!(
                "Environment Variable {} is not set, using default: {}",
                key, default
            );
            default.to_owned()
        }
    }
}
