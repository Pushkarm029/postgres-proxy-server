use envconfig::Envconfig;

#[derive(Envconfig)]
pub struct Config {
    #[envconfig(
        from = "DATA_DB_CONN_STRING",
        default = "postgres://postgres:postgres@localhost:5432/information_schema"
    )]
    pub data_db_conn: String,
    #[envconfig(from = "SERVER_ADDR", default = "127.0.0.1:5433")]
    pub server_address: String,
    #[envconfig(
        from = "SCHEMA_DB_CONN_STRING",
        default = "postgres://postgres:postgres@localhost:5432/information_schema"
    )]
    pub schema_db_conn: String,
    #[envconfig(from = "SCHEMA_TABLE_NAME", default = "measures")]
    pub schema_table_name: String,
}
