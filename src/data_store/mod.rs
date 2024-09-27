mod postgres;
mod snowflake;

use pgwire::messages::data;
use pgwire::messages::data::DataRow;
use postgres::PostgresType;
pub use snowflake::SnowflakeConfig;
pub use snowflake::SnowflakeDataStore;

use std::collections::HashMap;
use std::fmt;

/// DataStore executes and SQL query and returns the data
pub trait DataStore: Clone {
    /// Dialect supported by the [`DataStore`]
    fn get_dialect(&self) -> &dyn sqlparser::dialect::Dialect;
    /// Mapping Postgres types to [`DataStore`] specific types
    ///
    /// TODO: perhaps the input type should be the pgwire representation of types
    fn map_type(&self, pg_type: &PostgresType) -> Option<String>;
    /// Mapping inbuilt Postgres functions to [`DataStore`] specific functions
    ///
    /// For example, Postgres `now()` function for returning current timestamp
    /// is mapped to `CURRENT_TIMESTAMP()` in Snowflake
    fn map_function(&self, pg_function: &str) -> Option<String>;
    /// Execute the query and return the result as [`DataRow`]s
    ///
    /// The data store must internally map the result data into the
    /// pgwire [`DataRow`] type.
    fn execute(&self, sql: &str) -> Result<Vec<DataRow>, DataStoreError>;
    // TODO: add execute_streaming that returns a stream instead of vec
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

pub enum DataStoreError {
    ConnectionError(String),
    QueryError(String),
    InsufficientPrivileges(String),
    ColumnNotFound(String),
}

impl fmt::Display for DataStoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

#[derive(Clone)]
pub struct TodoDummyDataStore;

impl DataStore for TodoDummyDataStore {
    fn get_dialect(&self) -> &dyn sqlparser::dialect::Dialect {
        todo!()
    }

    fn map_type(&self, pg_type: &PostgresType) -> Option<String> {
        todo!()
    }

    fn map_function(&self, pg_function: &str) -> Option<String> {
        todo!()
    }

    fn execute(&self, sql: &str) -> Result<Vec<DataRow>, DataStoreError> {
        todo!()
    }
}
