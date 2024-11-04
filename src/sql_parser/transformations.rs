use crate::data_store::DataStoreMapping;
use crate::semantic_model::measure::Renderable;
use crate::semantic_model::SemanticModelStore;
use crate::sql_parser::SqlTransformError;
use sqlparser::ast::*;
use sqlparser::parser::Parser;

/// Applies transformations to a SQL query based on the data store mapping and semantic model.
///
/// This function traverses the entire query structure, including CTEs, and applies the necessary
/// transformations to each part of the query.
pub fn apply_transformations<M: DataStoreMapping, S: SemanticModelStore>(
    query: &mut Query,
    data_store_mapping: &M,
    semantic_model: &S,
) -> Result<(), SqlTransformError> {
    // Transform the main body of the query
    apply_set_expression(&mut query.body, data_store_mapping, semantic_model)?;

    // Transform each CTE if present
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            apply_transformations(&mut cte.query, data_store_mapping, semantic_model)?;
        }
    }

    Ok(())
}

/// Applies transformations to a SQL set expression.
///
/// This function handles different types of set expressions, such as SELECT statements,
/// subqueries, and set operations (UNION, INTERSECT, etc.).
fn apply_set_expression<D: DataStoreMapping, S: SemanticModelStore>(
    set_expr: &mut SetExpr,
    data_store: &D,
    semantic_model: &S,
) -> Result<(), SqlTransformError> {
    match set_expr {
        SetExpr::Select(select) => {
            apply_select_transformations(select, data_store, semantic_model)?
        }
        SetExpr::Query(query) => apply_transformations(query, data_store, semantic_model)?,
        SetExpr::SetOperation { left, right, .. } => {
            apply_set_expression(left, data_store, semantic_model)?;
            apply_set_expression(right, data_store, semantic_model)?;
        }
        _ => {
            return Err(SqlTransformError::UnsupportedSqlConstruct(
                "Unsupported set expression type".to_string(),
            ))
        }
    }
    Ok(())
}

/// Applies transformations to a SELECT statement.
///
/// This function processes each item in the SELECT list, as well as HAVING and QUALIFY clauses
/// if present. It handles both named and unnamed expressions.
fn apply_select_transformations<D: DataStoreMapping, S: SemanticModelStore>(
    select: &mut Select,
    data_store: &D,
    semantic_model: &S,
) -> Result<(), SqlTransformError> {
    for projection in &mut select.projection {
        match projection {
            SelectItem::ExprWithAlias { expr, .. } => {
                rewrite_expression(expr, data_store, semantic_model)?;
            }
            SelectItem::UnnamedExpr(expr) => {
                let old_expr = expr.clone();
                let rewritten_expr = rewrite_expression(expr, data_store, semantic_model)?;
                process_unnamed_expr(projection, &old_expr, rewritten_expr)?;
            }
            _ => (),
        }
    }

    // Transform HAVING clause if present
    if let Some(having) = &mut select.having {
        rewrite_expression(having, data_store, semantic_model)?;
    }

    // Transform QUALIFY clause if present
    if let Some(qualify) = &mut select.qualify {
        rewrite_expression(qualify, data_store, semantic_model)?;
    }

    Ok(())
}

/// Processes an unnamed expression in the SELECT list.
///
/// This function handles the special case of MEASURE functions and determines
/// whether to add an alias to the expression based on its type.
fn process_unnamed_expr(
    projection: &mut SelectItem,
    old_expr: &Expr,
    rewritten_expr: Expr,
) -> Result<(), SqlTransformError> {
    if let Expr::Function(func) = old_expr {
        let args = get_function_args(func)?;
        if !args.args.is_empty() {
            let ident = get_identifier_from_args(args)?;
            let (_, measure_name) = get_measure_info(&ident)?;

            if func.name.to_string().to_uppercase() == "MEASURE" {
                *projection = SelectItem::ExprWithAlias {
                    expr: rewritten_expr.clone(),
                    alias: Ident::new(measure_name.to_string()),
                };
            } else {
                *projection = SelectItem::UnnamedExpr(rewritten_expr.clone());
            }
        } else {
            *projection = SelectItem::UnnamedExpr(rewritten_expr);
        }
    } else {
        *projection = SelectItem::UnnamedExpr(rewritten_expr);
    }
    Ok(())
}

/// Rewrites an expression based on the data store mapping and semantic model.
///
/// This function handles different types of expressions, including functions,
/// binary operations, and EXISTS clauses.
fn rewrite_expression<D: DataStoreMapping, S: SemanticModelStore>(
    expr: &mut Expr,
    data_store: &D,
    semantic_model: &S,
) -> Result<Expr, SqlTransformError> {
    let new_expr = match expr {
        Expr::Function(func) => rewrite_function(func, data_store, semantic_model)?,
        Expr::BinaryOp { left, right, op } => {
            let new_left = rewrite_expression(left, data_store, semantic_model)?;
            let new_right = rewrite_expression(right, data_store, semantic_model)?;
            Expr::BinaryOp {
                left: Box::new(new_left),
                right: Box::new(new_right),
                op: op.clone(),
            }
        }
        Expr::Exists { subquery, negated } => {
            apply_transformations(subquery, data_store, semantic_model)?;
            Expr::Exists {
                subquery: subquery.clone(),
                negated: *negated,
            }
        }
        _ => expr.clone(),
    };

    *expr = new_expr.clone();
    Ok(new_expr)
}

/// Rewrites a function expression, handling MEASURE functions specially.
///
/// For MEASURE functions, it applies semantic model transformations.
/// For other functions, it applies data store specific mappings if available.
fn rewrite_function<D: DataStoreMapping, S: SemanticModelStore>(
    func: &mut Function,
    data_store: &D,
    semantic_model: &S,
) -> Result<Expr, SqlTransformError> {
    if func.name.to_string().to_uppercase() == "MEASURE" {
        rewrite_measure(func, data_store, semantic_model)
    } else if let Some(mapped_func) = data_store.map_function(func.to_string().as_str()) {
        Ok(Expr::Function(Function {
            name: ObjectName(vec![Ident::new(mapped_func)]),
            args: FunctionArguments::None,
            over: None,
            parameters: FunctionArguments::None,
            filter: None,
            null_treatment: None,
            within_group: vec![],
        }))
    } else {
        Ok(Expr::Function(func.clone()))
    }
}

/// Rewrites a MEASURE function based on the semantic model.
///
/// This function extracts the measure name, looks it up in the semantic model,
/// and replaces the MEASURE function with the actual SQL expression for the measure.
fn rewrite_measure<D: DataStoreMapping, S: SemanticModelStore>(
    func: &mut Function,
    data_store: &D,
    semantic_model: &S,
) -> Result<Expr, SqlTransformError> {
    let args = get_function_args(func)?;
    let ident = get_identifier_from_args(args)?;
    let (table_name, measure_name) = get_measure_info(&ident)?;

    let model = semantic_model
        .get_semantic_model(table_name)
        .map_err(|e| SqlTransformError::SemanticModelError(e.to_string()))?;

    let measure = model
        .get_measure(measure_name)
        .map_err(|e| SqlTransformError::SemanticModelError(e.to_string()))?;

    let sql = measure
        .render(&model, true)
        .map_err(|e| SqlTransformError::SemanticModelError(e.to_string()))?;

    let expr = parse_measure_sql(&sql, data_store.get_dialect())?;

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

/// Extracts function arguments from a Function AST node.
fn get_function_args(func: &Function) -> Result<&FunctionArgumentList, SqlTransformError> {
    match &func.args {
        FunctionArguments::List(args) => Ok(args),
        _ => Err(SqlTransformError::InvalidMeasureFunction(
            "MEASURE function expects a list of arguments".to_string(),
        )),
    }
}

/// Extracts the identifier from function arguments.
fn get_identifier_from_args(args: &FunctionArgumentList) -> Result<Vec<Ident>, SqlTransformError> {
    if args.args.is_empty() {
        return Err(SqlTransformError::InvalidFunctionArgument(
            "MEASURE function expects at least one argument".to_string(),
        ));
    }

    match &args.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(ident))) => {
            Ok(ident.clone())
        }
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
            Ok(vec![ident.clone()])
        }
        _ => Err(SqlTransformError::InvalidFunctionArgument(
            "MEASURE function expects a single identifier argument".to_string(),
        )),
    }
}

/// Extracts table name and measure name from an identifier.
fn get_measure_info(ident: &[Ident]) -> Result<(&str, &str), SqlTransformError> {
    match ident.len() {
        2 => Ok((ident[0].value.as_str(), ident[1].value.as_str())),
        1 => Ok(("", ident[0].value.as_str())),
        _ => Err(SqlTransformError::InvalidFunctionArgument(
            "Invalid MEASURE function argument".to_string(),
        )),
    }
}

/// Parses the SQL expression for a measure.
fn parse_measure_sql(
    sql: &str,
    dialect: &dyn sqlparser::dialect::Dialect,
) -> Result<Expr, SqlTransformError> {
    let statement_sql = format!("SELECT {}", sql);
    let statements = Parser::parse_sql(dialect, &statement_sql)
        .map_err(|e| SqlTransformError::SqlParsingError(e.to_string()))?;

    match statements.first() {
        Some(Statement::Query(query)) => {
            match query
                .body
                .as_select()
                .and_then(|select| select.projection.first())
            {
                Some(SelectItem::UnnamedExpr(expr)) => Ok(expr.clone()),
                Some(SelectItem::ExprWithAlias { expr, .. }) => Ok(expr.clone()),
                _ => Err(SqlTransformError::InvalidMeasureFunction(
                    "MEASURE function expects a single expression".to_string(),
                )),
            }
        }
        _ => Err(SqlTransformError::InvalidMeasureFunction(
            "MEASURE function expects a SELECT statement".to_string(),
        )),
    }
}
