use envconfig::Envconfig;

#[derive(Envconfig)]
pub struct Config {
    #[envconfig(from = "DATA_STORE", default = "postgres")]
    pub data_store: String,

    #[envconfig(from = "SEMANTIC_MODEL_STORE", default = "local")]
    pub semantic_model_store: String,

    #[envconfig(from = "SERVER_HOST", default = "127.0.0.1")]
    pub server_host: String,

    #[envconfig(from = "SERVER_PORT", default = "5433")]
    pub server_port: u16,
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

#[derive(Envconfig, Clone)]
pub struct SnowflakeConfig {
    #[envconfig(from = "SNOWFLAKE_ACCOUNT")]
    pub account: String,

    #[envconfig(from = "SNOWFLAKE_USER")]
    pub user: String,

    #[envconfig(from = "SNOWFLAKE_PASSWORD")]
    pub password: String,

    #[envconfig(from = "SNOWFLAKE_WAREHOUSE")]
    pub warehouse: String,

    #[envconfig(from = "SNOWFLAKE_DATABASE")]
    pub database: String,

    #[envconfig(from = "SNOWFLAKE_SCHEMA")]
    pub schema: String,
}

#[derive(Envconfig, Clone)]
pub struct S3Config {
    #[envconfig(from = "TENANT", default = "tenant1")]
    pub tenant: String,

    #[envconfig(from = "S3_BUCKET_NAME")]
    pub bucket_name: String,
}
