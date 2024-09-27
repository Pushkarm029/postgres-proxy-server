use crate::data_store::{DataStore, DataStoreError, PostgresType, Row};
use odbc::{odbc_safe::AutocommitOn, Connection, Environment};

pub struct SnowflakeConfig {
    pub account: String,
    pub user: String,
    pub password: String,
    pub warehouse: String,
    pub database: String,
    pub schema: String,
}

pub struct SnowflakeDataStore<'env> {
    config: SnowflakeConfig,
    conn: Connection<'env, AutocommitOn>,
}

impl<'env> SnowflakeDataStore<'env> {
    pub fn new(config: SnowflakeConfig) -> Result<Self, DataStoreError> {
        let connection_string = format!(
            "Driver={{SnowflakeDSIIDriver}};Server={}.snowflakecomputing.com;Uid={};Pwd={};Warehouse={};Database={};Schema={};",
            config.account, config.user, config.password, config.warehouse, config.database, config.schema
        );

        let env = Environment::new().map_err(|e| DataStoreError::ConnectionError(e.to_string()))?;
        let conn = env
            .connect_with_connection_string(&connection_string)
            .map_err(|e| DataStoreError::ConnectionError(e.to_string()))?;

        Ok(Self { config, conn })
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

    fn execute(&self, query: &str) -> Result<Vec<Row>, DataStoreError> {
        let mut stmt = self
            .conn
            .prepare(query)
            .map_err(|e| DataStoreError::QueryError(e.to_string()))?;
        let rows = stmt
            .query()
            .map_err(|e| DataStoreError::QueryError(e.to_string()))?;
        let mut result = Vec::new();

        for row in rows {
            let row_data: Vec<String> = row.iter().map(|cell| cell.to_string()).collect();
            result.push(Row {
                columns: row_data,
                values: Vec::new(),
            });
        }
    }
}
