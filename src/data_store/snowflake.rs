use crate::data_store::{DataStore, DataStoreError, PostgresType, Row};
use odbc::{odbc_safe::AutocommitOn, safe::Odbc3, Connection, Environment};
use pgwire::messages::data::DataRow;

#[derive(Clone)]
pub struct SnowflakeConfig {
    pub account: String,
    pub user: String,
    pub password: String,
    pub warehouse: String,
    pub database: String,
    pub schema: String,
}

pub struct SnowflakeDataStore<'a> {
    config: SnowflakeConfig,
    env: Environment<Odbc3>,
    conn: Connection<'a, AutocommitOn>,
}

impl<'a> Clone for SnowflakeDataStore<'a> {
    fn clone(&self) -> Self {
        todo!("Make the data store/connection cloneable")
    }
}

impl<'a> SnowflakeDataStore<'a> {
    pub fn new(config: SnowflakeConfig) -> Result<Self, DataStoreError> {
        let connection_string = format!(
            "Driver={{SnowflakeDSIIDriver}};Server={}.snowflakecomputing.com;Uid={};Pwd={};Warehouse={};Database={};Schema={};",
            config.account, config.user, config.password, config.warehouse, config.database, config.schema
        );

        let env = Environment::new().map_err(|e| match e {
            Some(e) => DataStoreError::ConnectionError(e.to_string()),
            None => DataStoreError::ConnectionError("TODO: something here".to_string()),
        })?;
        let conn = env
            .connect_with_connection_string(&connection_string)
            .map_err(|e| DataStoreError::ConnectionError(e.to_string()))?;

        todo!("figure out how to store environment so that it can outlive connections")
    }
}

impl<'env> DataStore for SnowflakeDataStore<'env> {
    fn get_dialect(&self) -> &dyn sqlparser::dialect::Dialect {
        &sqlparser::dialect::SnowflakeDialect {}
    }

    fn map_type(&self, pg_type: &PostgresType) -> Option<String> {
        match pg_type {
            PostgresType::Serial => Some("number(6)".to_string()),
            PostgresType::BigSerial => Some("number(11)".to_string()),
            PostgresType::SmallInt => Some("number(6)".to_string()),
            PostgresType::Integer => Some("number(11)".to_string()),
            PostgresType::BigInt => Some("number(20)".to_string()),
            PostgresType::Numeric => Some("number".to_string()),
            PostgresType::Real | PostgresType::DoublePrecision | PostgresType::Money => {
                Some("float".to_string())
            }
            PostgresType::ByteA => Some("binary".to_string()),
            PostgresType::Varchar
            | PostgresType::Char
            | PostgresType::Text
            | PostgresType::Cidr
            | PostgresType::Inet
            | PostgresType::MacAddr
            | PostgresType::MacAddr8
            | PostgresType::Bit
            | PostgresType::Uuid
            | PostgresType::Xml
            | PostgresType::TsVector
            | PostgresType::TsQuery
            | PostgresType::Interval
            | PostgresType::Point
            | PostgresType::Line
            | PostgresType::LSeg
            | PostgresType::Box
            | PostgresType::Path
            | PostgresType::Polygon
            | PostgresType::Circle
            | PostgresType::Array
            | PostgresType::Composite
            | PostgresType::Range
            | PostgresType::PgLsn
            | PostgresType::Name => Some("text".to_string()),
            PostgresType::Json | PostgresType::Jsonb | PostgresType::Geometry => {
                Some("variant".to_string())
            }
            PostgresType::Timestamp | PostgresType::TimestampTz => {
                Some("timestamp_ntz".to_string())
            }
            PostgresType::Date => Some("date".to_string()),
            PostgresType::Time | PostgresType::TimeTz => Some("time".to_string()),
            PostgresType::Boolean => Some("boolean".to_string()),
            PostgresType::Oid => Some("number(11)".to_string()),
            PostgresType::SLTimestamp => Some("timestamp_tz".to_string()),
        }
    }

    fn map_function(&self, pg_function: &str) -> Option<String> {
        match pg_function {
            "now()" => Some("CURRENT_TIMESTAMP()".to_string()),
            // Map other Postgres functions to Snowflake equivalents
            _ => Some(pg_function.to_string()),
        }
    }

    fn execute(&self, query: &str) -> Result<Vec<DataRow>, DataStoreError> {
        todo!()
    }
}
