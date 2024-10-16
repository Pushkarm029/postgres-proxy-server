pub mod postgres;
pub mod snowflake;

use async_trait::async_trait;
use pgwire::api::results::Response;
use std::error::Error;
use std::fmt;
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
    type Mapping: DataStoreMapping;

    fn get_mapping() -> Self::Mapping;

    /// Execute the SQL query and return the result as [`DataRow`]s.
    ///
    /// The DataStore must internally map the result data into the
    /// pgwire [`DataRow`] type.
    async fn execute(&self, sql: &str) -> Result<Vec<Response>, DataStoreError>;

    // TODO: Add execute_streaming that returns a stream instead of a vector of data rows
    // async fn execute_streaming(&self, sql: &str) -> Result<Stream<DataRow>, DataStoreError>;
}

#[derive(Debug)]
pub enum DataStoreError {
    ConnectionError(String),
    QueryError(String),
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
            DataStoreError::ColumnNotFound(details) => {
                write!(f, "Column not found: {}", details)
            }
        }
    }
}

impl Error for DataStoreError {}
