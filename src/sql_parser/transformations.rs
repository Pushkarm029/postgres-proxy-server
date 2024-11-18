use crate::data_store::DataStoreMapping;
use crate::semantic_model::measure::Renderable;
use crate::semantic_model::SemanticModelStore;
use sqlparser::ast::*;
use sqlparser::parser::Parser;

use super::SqlError;

/// Applies transformations to a SQL query based on the data store mapping and semantic model.
///
/// This function traverses the entire query structure, including CTEs, and applies the necessary
/// transformations to each part of the query.
pub fn apply_transformations<M: DataStoreMapping, S: SemanticModelStore>(
    query: &mut Query,
    data_store_mapping: &M,
    semantic_model: &S,
) -> Result<(), SqlError> {
    log::trace!("apply_transformations: input query = {}", query.to_string());

    // Transform the main body of the query
    apply_set_expression(&mut query.body, data_store_mapping, semantic_model)?;

    // Transform each CTE if present
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            apply_transformations(&mut cte.query, data_store_mapping, semantic_model)?;
        }
    }

    log::trace!(
        "apply_transformations: transformed query = {}",
        query.to_string()
    );
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
) -> Result<(), SqlError> {
    log::trace!(
        "apply_set_expression: input set_expr = {}",
        set_expr.to_string()
    );

    match set_expr {
        SetExpr::Select(select) => {
            log::trace!("apply_set_expression: processing SELECT");
            apply_select_transformations(select, data_store, semantic_model)?
        }
        SetExpr::Query(query) => {
            log::trace!("apply_set_expression: processing subquery");
            apply_transformations(query, data_store, semantic_model)?
        }
        SetExpr::SetOperation { left, right, .. } => {
            log::trace!("apply_set_expression: processing set operation");
            apply_set_expression(left, data_store, semantic_model)?;
            apply_set_expression(right, data_store, semantic_model)?;
        }
        _ => {
            log::trace!("apply_set_expression: unsupported set expression type");
            return Err(SqlError::UnsupportedSqlConstruct(
                "Unsupported set expression type".to_string(),
            ));
        }
    }

    log::trace!(
        "apply_set_expression: transformed set_expr = {}",
        set_expr.to_string()
    );
    Ok(())
}

/// Applies transformations to a SELECT statement.
///
/// This function processes each item in the SELECT list, as well as HAVING and QUALIFY clauses
/// if present. It handles both named and unnamed expressions.
fn apply_select_transformations<D: DataStoreMapping, S: SemanticModelStore>(
    select: &mut Select,
    data_store: &D,
    model_store: &S,
) -> Result<(), SqlError> {
    log::trace!(
        "apply_select_transformations: input select = {}",
        select.to_string()
    );

    // First, try to get the semantic model for the table being queried
    let model = get_model_from_select(select, model_store);

    // Handle different cases based on the select projection and model availability
    match (model, &mut select.projection[..]) {
        // Information_schema tables query with wildcard
        (Err(e @ SqlError::InformationSchemaResult(_)), [SelectItem::Wildcard(_)]) => {
            log::trace!("apply_select_transformations: handling information_schema.tables");
            return Err(e);
        }

        // Unsupported information schema query
        (Err(SqlError::InformationSchemaResult(_)), _) => {
            log::trace!("apply_select_transformations: Unsupported information schema query");
            return Err(SqlError::SqlTransformationError(
                "Unsupported information.schema query".to_owned(),
            ));
        }

        // Unsupported information schema query
        (Err(e), _) => {
            log::trace!("apply_select_transformations: Error {}", e.to_string());
            return Err(e);
        }

        // We have a semantic model and a single wildcard
        (Ok(model), [SelectItem::Wildcard(_)]) => {
            log::trace!("apply_select_transformations: processing wildcard with model");
            let mut temp_select = select.clone();
            process_wildcard_expr(&mut temp_select, &model)?;
            *select = temp_select;
        }

        // Case 3: We have a semantic model, process each projection
        (Ok(model), _) => {
            for projection in &mut select.projection {
                match projection {
                    SelectItem::ExprWithAlias { expr, .. } => {
                        rewrite_expression(expr, data_store, model_store)?;
                    }
                    SelectItem::UnnamedExpr(expr) => {
                        log::trace!("apply_select_transformations: processing unnamed expression");
                        let old_expr = expr.clone();
                        let new_expr = rewrite_expression(expr, data_store, model_store)?;
                        process_unnamed_expr(projection, &old_expr, new_expr, &model, model_store)?;
                    }
                    SelectItem::QualifiedWildcard(object_name, _) => {
                        if let Some(_table) = object_name.0.last() {
                            // Handle qualified wildcard (table.* or schema.table.*)
                            // Add your logic here
                            todo!("Handle table.* wildcard. Replace with table.column1, table.column2 etc")
                        } else {
                            return Err(SqlError::SqlTransformationError(
                                "Invalid qualified wildcard".to_string(),
                            ));
                        }
                    }
                    SelectItem::Wildcard(_) => {
                        log::trace!("apply_select_transformations: Unsupported query wildcard with multiple projects");
                        return Err(SqlError::SqlTransformationError(
                            "Wildcard with multiple projects is not supported".to_owned(),
                        ));
                    }
                }
            }
        }
    }

    // Transform HAVING clause if present
    if let Some(having) = &mut select.having {
        log::trace!("apply_select_transformations: processing HAVING clause");
        rewrite_expression(having, data_store, model_store)?;
    }

    // Transform QUALIFY clause if present
    if let Some(qualify) = &mut select.qualify {
        log::trace!("apply_select_transformations: processing QUALIFY clause");
        rewrite_expression(qualify, data_store, model_store)?;
    }

    log::trace!(
        "apply_select_transformations: transformed select = {}",
        select.to_string()
    );
    Ok(())
}

// Proccesses a wildcard expression in the SELECT list.
//
// This function handles the conversion of wildcards into the dimensions of the semantic model.
fn process_wildcard_expr(
    select: &mut Select,
    model: &crate::semantic_model::SemanticModel,
) -> Result<(), SqlError> {
    {
        // If a semantic model is provided, proceed to get the dimensions, otherwise skip
        // Get the dimensions from the semantic model
        if model.dimensions.is_empty() {
            return Err(SqlError::SemanticModelError(
                "No dimensions found in semantic model".to_string(),
            ));
        }

        // Create a list of expressions from dimensions
        let expr_list: Vec<SelectItem> = model
            .dimensions
            .iter()
            .map(|dim| SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(dim.name.clone()))))
            .collect();

        // Update the projection with the list of SelectItems (individual columns)
        select.projection = expr_list;
    }
    Ok(())
}

/// Processes an unnamed expression in the SELECT list.
///
/// This function handles the special case of MEASURE functions and determines
/// whether to add an alias to the expression based on its type.
/// Additionally, it checks if any selected columns are not present in the semantic model dimensions.
fn process_unnamed_expr<S: SemanticModelStore>(
    projection: &mut SelectItem,
    old_expr: &Expr,
    rewritten_expr: Expr,
    semantic_model: &crate::semantic_model::SemanticModel,
    model_store: &S,
) -> Result<(), SqlError> {
    log::trace!(
        "process_unnamed_expr: input old_expr = {}",
        old_expr.to_string()
    );

    // Extract model and verify column exists in one step
    match old_expr {
        Expr::Identifier(ident) => {
            if !semantic_model
                .dimensions
                .iter()
                .any(|dim| dim.name == ident.value)
            {
                log::trace!(
                    "process_unnamed_expr: column {} not found in semantic model {}",
                    ident.value,
                    semantic_model.name,
                );
                return Err(SqlError::SqlColumnNotFoundError(
                    ident.value.clone(),
                    semantic_model.name.clone(),
                ));
            }
        }
        Expr::CompoundIdentifier(idents) => {
            if let Some([table, column]) = idents.last_chunk::<2>() {
                let model = if table.value != semantic_model.name {
                    &model_store
                        .get_semantic_model(&table.value)
                        .map_err(|e| SqlError::SemanticModelError(e.to_string()))?
                } else {
                    semantic_model
                };
                if !model.dimensions.iter().any(|dim| dim.name == column.value) {
                    log::trace!(
                        "process_unnamed_expr: column {} not found in semantic model {}",
                        column.value,
                        semantic_model.name,
                    );
                    return Err(SqlError::SqlColumnNotFoundError(
                        column.value.clone(),
                        semantic_model.name.clone(),
                    ));
                }
            } else {
                return Err(SqlError::SqlTransformationError(
                    "Invalid compound identifier".to_string(),
                ));
            }
        }
        _ => (),
    }

    // Handle function expressions (especially MEASURE)
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

    log::trace!(
        "process_unnamed_expr: output projection = {}",
        projection.to_string()
    );
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
) -> Result<Expr, SqlError> {
    log::trace!("rewrite_expression: input expr = {}", expr.to_string());
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
    log::trace!("rewrite_expression: output expr = {}", new_expr.to_string());
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
) -> Result<Expr, SqlError> {
    log::trace!("rewrite_function: input func = {}", func.to_string());
    let result = if func.name.to_string().to_uppercase() == "MEASURE" {
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
    };
    log::trace!("rewrite_function: output = {:?}", result);
    result
}

/// Rewrites a MEASURE function based on the semantic model.
///
/// This function extracts the measure name, looks it up in the semantic model,
/// and replaces the MEASURE function with the actual SQL expression for the measure.
fn rewrite_measure<D: DataStoreMapping, S: SemanticModelStore>(
    func: &mut Function,
    data_store: &D,
    semantic_model: &S,
) -> Result<Expr, SqlError> {
    log::trace!("rewrite_measure: input func = {}", func.to_string());
    let args = get_function_args(func)?;
    let ident = get_identifier_from_args(args)?;
    let (table_name, measure_name) = get_measure_info(&ident)?;

    let model = semantic_model
        .get_semantic_model(table_name)
        .map_err(|e| SqlError::SemanticModelError(e.to_string()))?;

    let measure = model
        .get_measure(measure_name)
        .map_err(|e| SqlError::SemanticModelError(e.to_string()))?;

    let sql = measure
        .render(&model, true)
        .map_err(|e| SqlError::SemanticModelError(e.to_string()))?;

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

    log::trace!("rewrite_measure: output expr = {}", expr.to_string());
    Ok(expr)
}

fn get_model_from_select<S: SemanticModelStore>(
    select: &Select,
    semantic_model: &S,
) -> Result<crate::semantic_model::SemanticModel, SqlError> {
    // Extract table name from the first FROM clause
    let table_name = select
        .from
        .first()
        .and_then(|twj| {
            if let TableFactor::Table {
                name: ObjectName(idents),
                ..
            } = &twj.relation
            {
                // Handle both single identifier and compound (schema.table) cases
                if idents.len() == 2
                    && idents[0].value.to_lowercase() == "information_schema"
                    && idents[1].value.to_lowercase() == "tables"
                {
                    // Return all semantic model names for information_schema.tables
                    return Some("information_schema.tables".to_string());
                }
                Some(idents.last().unwrap().value.clone())
            } else {
                None
            }
        })
        .unwrap_or_default();

    // Special handling for information_schema.tables
    if table_name == "information_schema.tables" {
        let all_models = semantic_model
            .get_all_semantic_models()
            .map_err(|e| SqlError::SemanticModelError(e.to_string()))?;
        return Err(SqlError::InformationSchemaResult(
            all_models.keys().cloned().collect(),
        ));
    }

    // Normal case: try to get the semantic model for the table
    semantic_model.get_semantic_model(&table_name).map_err(|_| {
        SqlError::SemanticModelError(format!("No semantic model found for table: {}", table_name))
    })
}

/// Extracts function arguments from a Function AST node.
fn get_function_args(func: &Function) -> Result<&FunctionArgumentList, SqlError> {
    match &func.args {
        FunctionArguments::List(args) => Ok(args),
        _ => Err(SqlError::InvalidMeasureFunction(
            "MEASURE function expects a list of arguments".to_string(),
        )),
    }
}

/// Extracts the identifier from function arguments.
fn get_identifier_from_args(args: &FunctionArgumentList) -> Result<Vec<Ident>, SqlError> {
    if args.args.is_empty() {
        return Err(SqlError::InvalidFunctionArgument(
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
        _ => Err(SqlError::InvalidFunctionArgument(
            "MEASURE function expects a single identifier argument".to_string(),
        )),
    }
}

/// Extracts table name and measure name from an identifier.
fn get_measure_info(ident: &[Ident]) -> Result<(&str, &str), SqlError> {
    match ident.len() {
        2 => Ok((ident[0].value.as_str(), ident[1].value.as_str())),
        1 => Ok(("", ident[0].value.as_str())),
        _ => Err(SqlError::InvalidFunctionArgument(
            "Invalid MEASURE function argument".to_string(),
        )),
    }
}

/// Parses the SQL expression for a measure.
fn parse_measure_sql(
    sql: &str,
    dialect: &dyn sqlparser::dialect::Dialect,
) -> Result<Expr, SqlError> {
    let statement_sql = format!("SELECT {}", sql);
    let statements = Parser::parse_sql(dialect, &statement_sql)
        .map_err(|e| SqlError::SqlParsingError(e.to_string()))?;

    match statements.first() {
        Some(Statement::Query(query)) => {
            match query
                .body
                .as_select()
                .and_then(|select| select.projection.first())
            {
                Some(SelectItem::UnnamedExpr(expr)) => Ok(expr.clone()),
                Some(SelectItem::ExprWithAlias { expr, .. }) => Ok(expr.clone()),
                _ => Err(SqlError::InvalidMeasureFunction(
                    "MEASURE function expects a single expression".to_string(),
                )),
            }
        }
        _ => Err(SqlError::InvalidMeasureFunction(
            "MEASURE function expects a SELECT statement".to_string(),
        )),
    }
}
