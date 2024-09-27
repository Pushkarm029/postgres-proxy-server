use crate::data_store::PostgresType;
use std::collections::HashMap;
use std::fmt;
pub trait DataStore {
    fn get_dialect(&self) -> &dyn sqlparser::dialect::Dialect;
    fn map_type(&self, pg_type: &PostgresType) -> Option<String>;
    fn map_function(&self, pg_function: &str) -> Option<String>;
    fn execute(&self, sql: &str) -> Result<Vec<Row>, DataStoreError>;
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
