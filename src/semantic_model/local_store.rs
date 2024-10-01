use crate::semantic_model::store::{SemanticModelStore, SemanticModelStoreError};
use crate::semantic_model::Dimension;
use crate::semantic_model::{Measure, SemanticModel};
use std::collections::HashMap;
#[derive(Clone)]
pub struct LocalSemanticModelStore {
    semantic_models: HashMap<String, SemanticModel>,
}

impl LocalSemanticModelStore {
    pub fn new() -> Self {
        let mut semantic_models = HashMap::new();
        let employees_model = SemanticModel {
            name: "dm_employees".to_string(),
            label: "Employees".to_string(),
            description: "Dimensional model for employee data".to_string(),
            measures: vec![
                Measure {
                    name: "headcount".to_string(),
                    description: "Count of distinct employees included in headcount".to_string(),
                    data_type: "INTEGER".to_string(),
                    aggregation: "COUNT_DISTINCT".to_string(),
                    sql: "COUNT(DISTINCT CASE WHEN dm_employees.included_in_headcount THEN dm_employees.id ELSE NULL END)".to_string(),
                },
                Measure {
                    name: "ending_headcount".to_string(),
                    description: "Count of distinct effective dates for employees".to_string(),
                    data_type: "INTEGER".to_string(),
                    aggregation: "COUNT_DISTINCT".to_string(),
                    sql: "count(distinct dm_employees.effective_date)".to_string(),
                },
            ],
            dimensions: vec![
                Dimension {
                    name: "department_level_1".to_string(),
                    description: "Top level department of the employee".to_string(),
                    data_type: "STRING".to_string(),
                    is_primary_key: false,
                },
                Dimension {
                    name: "id".to_string(),
                    description: "Unique identifier for the employee".to_string(),
                    data_type: "INTEGER".to_string(),
                    is_primary_key: true,
                },
                Dimension {
                    name: "included_in_headcount".to_string(),
                    description: "Flag indicating if the employee is included in headcount calculations".to_string(),
                    data_type: "BOOLEAN".to_string(),
                    is_primary_key: false,
                },
                // You can add more dimensions here if needed
            ],
        };

        semantic_models.insert(employees_model.name.clone(), employees_model);

        LocalSemanticModelStore { semantic_models }
    }
}

impl SemanticModelStore for LocalSemanticModelStore {
    fn get_semantic_model(&self, name: &str) -> Result<SemanticModel, SemanticModelStoreError> {
        match self.semantic_models.get(name) {
            Some(model) => Ok(model.clone()),
            None => Err(SemanticModelStoreError::MeasureNotFound),
        }
    }

    fn get_all_semantic_models(
        &self,
    ) -> Result<HashMap<String, SemanticModel>, SemanticModelStoreError> {
        Ok(self.semantic_models.clone())
    }

    fn get_measure(
        &self,
        table_name: &str,
        measure_name: &str,
    ) -> Result<Measure, SemanticModelStoreError> {
        let semantic_model = self.get_semantic_model(table_name)?;

        semantic_model
            .measures
            .iter()
            .find(|measure| measure.name == measure_name)
            .cloned()
            .ok_or(SemanticModelStoreError::MeasureNotFound)
    }
}
