use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, SelectItem, Statement,
};
use sqlx::{postgres::PgConnection, Connection};

use crate::utils::config::SCHEMA_DB_ADDRESS;

pub async fn replace_measure_with_expression(ast: &mut [Statement]) {
    for statement in ast.iter_mut() {
        if let Statement::Query(query) = statement {
            if let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() {
                for proj in select.projection.iter_mut() {
                    if let SelectItem::UnnamedExpr(Expr::Function(func)) = proj {
                        if func.name.0[0].value == "MEASURE" {
                            if let FunctionArguments::List(list) = &mut func.args {
                                for item in list.args.iter_mut() {
                                    *item = FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                        Expr::Identifier(Ident::new(
                                            get_query_from_schema(item.to_string()).await,
                                        )),
                                    ));
                                }
                            }
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
    let mut conn = PgConnection::connect(SCHEMA_DB_ADDRESS)
        .await
        .expect("Failed to connect to database");

    let new_arg: SchemaQuery = sqlx::query_as(&format!(
        "SELECT query FROM information_schema.measures WHERE name = '{}';",
        old_arg
    ))
    .fetch_one(&mut conn)
    .await
    .unwrap();

    new_arg.0
}
