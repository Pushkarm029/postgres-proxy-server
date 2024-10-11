use crate::data_store::DataStoreMapping;
use crate::semantic_model::SemanticModelStore;
use crate::sql_parser::SqlParserError;
use sqlparser::ast::*;
use sqlparser::parser::Parser;

pub fn apply_transformations<M, S>(
    query: &mut Query,
    data_store_mapping: &M,
    semantic_model: &S,
) -> Result<(), SqlParserError>
where
    M: DataStoreMapping,
    S: SemanticModelStore,
{
    // First apply the transformations for the body, then each CTE recursively
    apply_set_expression(&mut query.body, data_store_mapping, semantic_model)?;

    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            apply_transformations(&mut cte.query, data_store_mapping, semantic_model)?;
        }
    }

    Ok(())
}

fn apply_set_expression<D, S>(
    set_expr: &mut SetExpr,
    data_store: &D,
    semantic_model: &S,
) -> Result<(), SqlParserError>
where
    D: DataStoreMapping,
    S: SemanticModelStore,
{
    match set_expr {
        SetExpr::Select(select) => {
            for projection in &mut select.projection {
                match projection {
                    SelectItem::ExprWithAlias { expr, .. } => {
                        rewrite_expression(expr, data_store, semantic_model)?;
                    }
                    SelectItem::UnnamedExpr(expr) => {
                        // Rewrite the expression
                        println!("rewritten_expr: {:?}", expr.to_string());
                        let old_expr = expr.clone();
                        let rewritten_expr = rewrite_expression(expr, data_store, semantic_model)?;
                        // Check if the rewritten expression is a function and its name is "MEASURE"
                        if let Expr::Function(func) = &old_expr {
                            let args = match &func.args {
                                FunctionArguments::List(args) => args,
                                _ => {
                                    return Err(SqlParserError::MeasureFunctionError(
                                        "MEASURE function expects a single identifier argument"
                                            .to_string(),
                                    ))
                                }
                            };
                            println!("args: {:?}", args);
                            let ident = match &args.args[0] {
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                    Expr::CompoundIdentifier(ident),
                                )) => ident,
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(
                                    ident,
                                ))) => &vec![ident.clone()],
                                _ => {
                                    return Err(SqlParserError::MeasureFunctionError(
                                        "MEASURE function expects a single identifier argument"
                                            .to_string(),
                                    ))
                                }
                            };

                            // let table_name = ident.get(0).unwrap().value.as_str();
                            // let measure_name = ident.get(1).unwrap().value.as_str();
                            let (_, measure_name) = if ident.len() == 2 {
                                (ident[0].value.as_str(), ident[1].value.as_str())
                            } else if ident.len() == 1 {
                                ("", ident[0].value.as_str())
                            } else {
                                return Err(SqlParserError::MeasureFunctionError(
                                    "Invalid MEASURE function argument".to_string(),
                                ));
                            };

                            if func.name.to_string().to_uppercase() == "MEASURE" {
                                // Set the new projection as an ExprWithAlias
                                *projection = SelectItem::ExprWithAlias {
                                    expr: rewritten_expr.clone(),
                                    alias: Ident::new(measure_name.to_string()), // Use the rewritten expression's string as the alias
                                };
                            } else {
                                // For other cases, just update the projection with the rewritten expression
                                *projection = SelectItem::UnnamedExpr(rewritten_expr);
                            }
                        } else {
                            // For non-function expressions, just update the projection
                            *projection = SelectItem::UnnamedExpr(rewritten_expr);
                        }
                    }
                    _ => (),
                }

                // Handle HAVING clause
                if let Some(having) = &mut select.having {
                    rewrite_expression(having, data_store, semantic_model)?;
                }

                // Handle QUALIFY clause
                if let Some(qualify) = &mut select.qualify {
                    rewrite_expression(qualify, data_store, semantic_model)?;
                }
            }
        }
        SetExpr::Query(query) => {
            apply_transformations(query, data_store, semantic_model)?;
        }
        SetExpr::SetOperation { left, right, .. } => {
            apply_set_expression(&mut *left, data_store, semantic_model)?;
            apply_set_expression(&mut *right, data_store, semantic_model)?;
        }
        _ => (),
    }
    Ok(())
}

fn rewrite_expression<D, S>(
    expr: &mut Expr,
    data_store: &D,
    semantic_model: &S,
) -> Result<Expr, SqlParserError>
where
    D: DataStoreMapping,
    S: SemanticModelStore,
{
    let new_expr = match expr {
        Expr::Function(func) => {
            let mut new_func = func.clone();
            if new_func.name.to_string().to_uppercase() == "MEASURE" {
                rewrite_measure(&mut new_func, data_store, semantic_model)?;
            } else {
                // Apply data_store-specific function mappings
                if let Some(mapped_func) = data_store.map_function(func.to_string().as_str()) {
                    new_func = Function {
                        name: ObjectName(vec![Ident::new(mapped_func)]),
                        args: FunctionArguments::None,
                        over: None,
                        parameters: FunctionArguments::None,
                        filter: None,
                        null_treatment: None,
                        within_group: vec![],
                    };
                }
            }
            // println!("OLD: {:?}", func);
            // println!("NEW: {:?}", new_func);
            Expr::Function(new_func)
        }
        Expr::BinaryOp { left, right, op } => {
            let mut new_left = left.as_ref().clone();
            let mut new_right = right.as_ref().clone();
            rewrite_expression(&mut new_left, data_store, semantic_model)?;
            rewrite_expression(&mut new_right, data_store, semantic_model)?;
            Expr::BinaryOp {
                left: Box::new(new_left),
                right: Box::new(new_right),
                op: op.clone(),
            }
        }
        Expr::Exists { subquery, negated } => {
            let mut new_subquery = subquery.as_ref().clone();
            apply_transformations(&mut new_subquery, data_store, semantic_model)?;
            Expr::Exists {
                subquery: Box::new(new_subquery),
                negated: *negated,
            }
        }
        _ => expr.clone(),
    };

    *expr = new_expr.clone(); // Update the original expression with the new one
    Ok(new_expr) // Return the new expression
}

/// Rewrites the custom `MEASURE` function to the corresponding SQL based on the semantic model.
///
/// The `MEASURE` function is used to reference a measure defined in the semantic model. Its usage
/// looks like `MEASURE(measure_name)`, where `measure_name` is the name of the measure in the
/// semantic model.
///
/// If the `measure_name` does not include a table name (i.e., there is no `.` in the name), we
/// raise an error.
///
/// The function transforms the `MEASURE` call to the SQL defined for the measure in the semantic
/// model.
fn rewrite_measure<D, S>(
    func: &mut Function,
    data_store: &D,
    semantic_model: &S,
) -> Result<Expr, SqlParserError>
where
    D: DataStoreMapping,
    S: SemanticModelStore,
{
    let args = match &func.args {
        FunctionArguments::List(args) => args,
        _ => {
            return Err(SqlParserError::MeasureFunctionError(
                "MEASURE function expects a single identifier argument".to_string(),
            ))
        }
    };
    println!("args: {:?}", args);
    println!("func: {:?}", func.to_string());

    if args.args.len() != 1 {
        return Err(SqlParserError::MeasureFunctionError(
            "MEASURE function expects a single identifier argument".to_string(),
        ));
    }

    println!("ident: {:?}", args);
    let ident = match &args.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(ident))) => ident,
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
            &vec![ident.clone()]
        }
        _ => {
            return Err(SqlParserError::MeasureFunctionError(
                "MEASURE function expects a single identifier argument".to_string(),
            ))
        }
    };

    // if ident.len() != 2 {
    //     return Err(SqlParserError::MeasureFunctionError(
    //         "MEASURE function expects a single identifier argument with a table name".to_string(),
    //     ));
    // }

    // let table_name = ident.get(0).unwrap().value.as_str();
    // let measure_name = ident.get(1).unwrap().value.as_str();
    let (table_name, measure_name) = if ident.len() == 2 {
        (ident[0].value.as_str(), ident[1].value.as_str())
    } else if ident.len() == 1 {
        ("", ident[0].value.as_str())
    } else {
        return Err(SqlParserError::MeasureFunctionError(
            "Invalid MEASURE function argument".to_string(),
        ));
    };
    let measure = semantic_model
        .get_measure(table_name, measure_name)
        .map_err(|e| SqlParserError::MeasureFunctionError(e.to_string()))?;

    let dialect = data_store.get_dialect();
    let statement_sql = format!("SELECT {}", measure.sql);
    let statements = Parser::parse_sql(dialect, &statement_sql)
        .map_err(|e| SqlParserError::MeasureFunctionError(e.to_string()))?;

    let expr = match statements.first().unwrap() {
        Statement::Query(query) => {
            match query.body.as_select().unwrap().projection.first().unwrap() {
                SelectItem::UnnamedExpr(expr) => expr.clone(),
                SelectItem::ExprWithAlias { expr, .. } => expr.clone(),
                _ => {
                    return Err(SqlParserError::MeasureFunctionError(
                        "MEASURE function expects a single function expression".to_string(),
                    ))
                }
            }
        }
        _ => {
            return Err(SqlParserError::MeasureFunctionError(
                "MEASURE function expects a single function expression".to_string(),
            ))
        }
    };

    *func = Function {
        name: ObjectName(vec![Ident::new(expr.to_string())]),
        args: FunctionArguments::None,
        over: None,
        parameters: FunctionArguments::None,
        filter: None,
        null_treatment: None,
        within_group: vec![],
    };

    Ok(expr)
}
