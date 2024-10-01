pub mod postgres;
mod snowflake;

// use pgwire::messages::data;
use pgwire::messages::data::DataRow;
use postgres::PostgresDataStore;
// use postgres::PostgresType;
use async_trait::async_trait;
use pgwire::api::results::Response;
use pgwire::error::PgWireResult;
pub use snowflake::SnowflakeConfig;
pub use snowflake::SnowflakeDataStore;
use std::collections::HashMap;
use std::fmt;

pub trait DataStore: DataStoreClient + DataStoreMapping + Clone {}

#[derive(Clone)]
pub enum DataStoreType {
    Postgres(PostgresDataStore),
    Snowflake(SnowflakeDataStore),
}

#[async_trait]
impl DataStore for DataStoreType {}

impl DataStoreMapping for DataStoreType {
    fn get_dialect(&self) -> &dyn sqlparser::dialect::Dialect {
        match self {
            DataStoreType::Postgres(pg_store) => pg_store.get_dialect(),
            DataStoreType::Snowflake(snowflake_store) => snowflake_store.get_dialect(),
        }
    }
    fn map_function(&self, pg_function: &str) -> Option<String> {
        match self {
            DataStoreType::Postgres(pg_store) => pg_store.map_function(pg_function),
            DataStoreType::Snowflake(snowflake_store) => snowflake_store.map_function(pg_function),
        }
    }

    // Implement type mapping if necessary
    // fn map_type(&self, pg_type: &PostgresType) -> Option<String> {
    //     // Example mapping
    // }
}

#[async_trait]
impl DataStoreClient for DataStoreType {
    async fn execute(&self, sql: &str) -> Result<Vec<Response>, DataStoreError> {
        match self {
            DataStoreType::Postgres(pg_store) => pg_store.execute(sql).await,
            DataStoreType::Snowflake(snowflake_store) => snowflake_store.execute(sql).await,
        }
    }
}

/// DataStoreMapping handles the mapping logic for types and functions
/// between different SQL dialects.
pub trait DataStoreMapping {
    /// Dialect supported by the [`DataStoreMapping`]
    fn get_dialect(&self) -> &dyn sqlparser::dialect::Dialect;

    /// Mapping inbuilt Postgres functions to DataStore specific functions.
    ///
    /// For example, Postgres `now()` function for returning the current timestamp
    /// is mapped to `CURRENT_TIMESTAMP()` in Snowflake.
    fn map_function(&self, pg_function: &str) -> Option<String>;

    // You can uncomment or add type mapping functions when necessary
    // /// Mapping Postgres types to DataStore specific types
    // /// TODO: perhaps the input type should be the pgwire representation of types
    // fn map_type(&self, pg_type: &PostgresType) -> Option<String>;
}

/// DataStoreClient is responsible for executing queries and returning
/// results from the DataStore.
#[async_trait]
pub trait DataStoreClient {
    /// Execute the SQL query and return the result as [`DataRow`]s.
    ///
    /// The DataStore must internally map the result data into the
    /// pgwire [`DataRow`] type.
    async fn execute(&self, sql: &str) -> Result<Vec<Response>, DataStoreError>;

    // TODO: Add execute_streaming that returns a stream instead of a vector of data rows
    // async fn execute_streaming(&self, sql: &str) -> Result<Stream<DataRow>, DataStoreError>;
}

pub struct Row {
    pub columns: Vec<String>,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Int(i64),
    Float(f64),
    String(String),
    Array(Vec<Value>),
    Object(HashMap<String, Value>),
}

#[derive(Debug)]
pub enum DataStoreError {
    ConnectionError(String),
    QueryError(String),
    InsufficientPrivileges(String),
    ColumnNotFound(String),
}

impl fmt::Display for DataStoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DataStoreError::ConnectionError(details) => {
                write!(f, "Connection error: {}", details)
            }
            DataStoreError::QueryError(details) => {
                write!(f, "Query error: {}", details)
            }
            DataStoreError::InsufficientPrivileges(details) => {
                write!(f, "Insufficient privileges: {}", details)
            }
            DataStoreError::ColumnNotFound(details) => {
                write!(f, "Column not found: {}", details)
            }
        }
    }
}
