use crate::utils::config::Config;
use envconfig::Envconfig;
use pgwire::error::PgWireError;
use sqlparser::ast::{Expr, Function, FunctionArguments, Ident, ObjectName, SelectItem, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tokio_postgres::Client;

pub async fn replace_measure_with_expression(client: &Client, initial_query: &str) -> String {
    let dialect = PostgreSqlDialect {};
    let mut ast = Parser::parse_sql(&dialect, initial_query)
        .map_err(|e| PgWireError::ApiError(Box::new(e)))
        .unwrap();

    for statement in ast.iter_mut() {
        if let Statement::Query(query) = statement {
            if let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() {
                for proj in select.projection.iter_mut() {
                    if let SelectItem::UnnamedExpr(Expr::Function(func)) = proj {
                        if func.name.0[0].value == "MEASURE" {
                            let mut new_item = String::new();
                            if let FunctionArguments::List(list) = &mut func.args {
                                for item in list.args.iter_mut() {
                                    new_item =
                                        get_query_from_schema(client, item.to_string()).await;
                                }
                            }
                            *func = Function {
                                name: ObjectName(vec![Ident::new(new_item)]),
                                args: FunctionArguments::None,
                                over: None,
                                parameters: FunctionArguments::None,
                                filter: None,
                                null_treatment: None,
                                within_group: vec![],
                            };
                        }
                    }
                }
            }
        }
    }

    ast[0].to_string()
}

async fn get_query_from_schema(client: &Client, old_arg: String) -> String {
    let config = Config::init_from_env().unwrap();
    let query = format!(
        "SELECT query FROM {} WHERE name = $1;",
        config.schema_table_name
    );

    let new_arg: String = client.query_one(&query, &[&old_arg]).await.unwrap().get(0);
    new_arg
}
