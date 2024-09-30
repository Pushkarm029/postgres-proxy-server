use envconfig::Envconfig;

#[derive(Envconfig)]
pub struct Config {
    #[envconfig(from = "DATA_STORE", default = "postgres")]
    pub data_store: String,

    #[envconfig(from = "SEMANTIC_MODEL_STORE", default = "local")]
    pub semantic_model_store: String,

    #[envconfig(from = "SERVER_HOST")]
    pub server_host: String,

    #[envconfig(from = "SERVER_PORT")]
    pub server_port: u16,
}

#[derive(Envconfig)]
pub struct PostgresDataStoreEnvConfig {
    #[envconfig(from = "POSTGRES_USER", default = "postgres")]
    pub user: String,
    #[envconfig(from = "POSTGRES_PASSWORD", default = "postgres")]
    pub password: String,
    #[envconfig(from = "POSTGRES_HOST", default = "localhost:5432")]
    pub host: String,
    #[envconfig(from = "POSTGRES_DB", default = "main")]
    pub dbname: String,
}

#[derive(Envconfig)]
pub struct SnowflakeDataStoreEnvConfig {
    #[envconfig(from = "SNOWFLAKE_ACCOUNT")]
    pub snowflake_account: String,

    #[envconfig(from = "SNOWFLAKE_USER")]
    pub snowflake_user: String,

    #[envconfig(from = "SNOWFLAKE_PASSWORD")]
    pub snowflake_password: String,

    #[envconfig(from = "SNOWFLAKE_WAREHOUSE")]
    pub snowflake_warehouse: String,

    #[envconfig(from = "SNOWFLAKE_DATABASE")]
    pub snowflake_database: String,

    #[envconfig(from = "SNOWFLAKE_SCHEMA")]
    pub snowflake_schema: String,
}

#[derive(Envconfig)]
pub struct S3SemanticModelEnvConfig {
    #[envconfig(from = "TENANT", default = "tenant1")]
    pub tenant: String,

    #[envconfig(from = "S3_BUCKET_NAME")]
    pub s3_bucket_name: String,
}
