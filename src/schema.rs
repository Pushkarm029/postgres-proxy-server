use sqlparser::ast::{Expr, Function, FunctionArguments, Ident, ObjectName, SelectItem, Statement};
use sqlx::{postgres::PgConnection, Connection};

use crate::utils::config::{get_schema_db_address, get_schema_table_name};

pub async fn replace_measure_with_expression(ast: &mut [Statement]) {
    for statement in ast.iter_mut() {
        if let Statement::Query(query) = statement {
            if let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() {
                for proj in select.projection.iter_mut() {
                    if let SelectItem::UnnamedExpr(Expr::Function(func)) = proj {
                        if func.name.0[0].value == "MEASURE" {
                            let mut new_qq = String::new();
                            if let FunctionArguments::List(list) = &mut func.args {
                                for item in list.args.iter_mut() {
                                    // *item = FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                    //     Expr::Identifier(Ident::new(
                                    //         get_query_from_schema(item.to_string()).await,
                                    //     )),
                                    // ));
                                    new_qq = get_query_from_schema(item.to_string()).await;
                                }
                            }
                            *func = Function {
                                name: ObjectName(vec![Ident::new(new_qq)]),
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
}

#[derive(sqlx::FromRow, Debug, PartialEq, Eq)]
struct SchemaQuery(String);

async fn get_query_from_schema(old_arg: String) -> String {
    let mut conn = PgConnection::connect(&get_schema_db_address())
        .await
        .expect("Failed to connect to database");

    let new_arg: SchemaQuery = sqlx::query_as(&format!(
        "SELECT query FROM {} WHERE name = '{}';",
        get_schema_table_name(),
        old_arg
    ))
    .fetch_one(&mut conn)
    .await
    .expect("Failed to get data from information_schema");

    new_arg.0
}
