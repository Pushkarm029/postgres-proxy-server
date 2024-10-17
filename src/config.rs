use envconfig::Envconfig;
use log::debug;

#[derive(Envconfig)]
pub struct Config {
    #[envconfig(from = "SERVER_HOST", default = "127.0.0.1")]
    pub server_host: String,

    #[envconfig(from = "SERVER_PORT", default = "5433")]
    pub server_port: u16,
}

impl Config {
    pub fn new() -> Result<Self, envconfig::Error> {
        let config = Self::init_from_env()?;
        debug!(
            "Config loaded: server_host={}, server_port={}",
            config.server_host, config.server_port
        );
        Ok(config)
    }
}

#[derive(Envconfig, Clone)]
pub struct PostgresConfig {
    #[envconfig(from = "POSTGRES_USER", default = "postgres")]
    pub user: String,
    #[envconfig(from = "POSTGRES_PASSWORD", default = "postgres")]
    pub password: String,
    #[envconfig(from = "POSTGRES_HOST", default = "localhost:5432")]
    pub host: String,
    #[envconfig(from = "POSTGRES_DB", default = "main")]
    pub dbname: String,
}

impl PostgresConfig {
    pub fn new() -> Result<Self, envconfig::Error> {
        let config = Self::init_from_env()?;
        debug!(
            "PostgresConfig loaded: user={}, host={}, dbname={}",
            config.user, config.host, config.dbname
        );
        Ok(config)
    }
}

#[derive(Envconfig, Clone)]
pub struct SnowflakeConfig {
    #[envconfig(from = "SNOWFLAKE_ACCOUNT")]
    pub account: String,

    #[envconfig(from = "SNOWFLAKE_USER")]
    pub user: String,

    #[envconfig(from = "SNOWFLAKE_PASSWORD")]
    pub password: String,

    #[envconfig(from = "SNOWFLAKE_WAREHOUSE")]
    pub warehouse: Option<String>,

    #[envconfig(from = "SNOWFLAKE_DATABASE")]
    pub database: Option<String>,

    #[envconfig(from = "SNOWFLAKE_SCHEMA")]
    pub schema: Option<String>,

    #[envconfig(from = "SNOWFLAKE_ROLE")]
    pub role: Option<String>,

    #[envconfig(from = "SNOWFLAKE_TIMEOUT")]
    pub timeout: Option<u64>,
}

impl SnowflakeConfig {
    pub fn new() -> Result<Self, envconfig::Error> {
        let config = Self::init_from_env()?;
        debug!("SnowflakeConfig loaded: account={}, user={}, warehouse={:?}, database={:?}, schema={:?}, role={:?}, timeout={:?}",
               config.account, config.user, config.warehouse, config.database, config.schema, config.role, config.timeout);
        Ok(config)
    }
}

#[derive(Envconfig, Clone)]
pub struct S3Config {
    #[envconfig(from = "TENANT")]
    pub tenant: String,

    #[envconfig(from = "S3_BUCKET_NAME")]
    pub bucket_name: String,
}

impl S3Config {
    pub fn new() -> Result<Self, envconfig::Error> {
        let config = Self::init_from_env()?;
        debug!(
            "S3Config loaded: tenant={}, bucket_name={}",
            config.tenant, config.bucket_name
        );
        Ok(config)
    }
}

#[derive(Envconfig, Clone)]
pub struct SemanticModelJSONConfig {
    #[envconfig(from = "JSON_PATH")]
    pub json_path: String,
}

impl SemanticModelJSONConfig {
    pub fn new() -> Result<Self, envconfig::Error> {
        let config = Self::init_from_env()?;
        debug!(
            "SemanticModelJSONConfig loaded: json_path={}",
            config.json_path
        );
        Ok(config)
    }
}
