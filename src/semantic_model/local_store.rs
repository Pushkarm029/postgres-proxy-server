use serde::{Deserialize, Serialize};

use super::measure::SimpleMeasure;
use super::{Dimension, Measure, SemanticModel, SemanticModelStore, SemanticModelStoreError};
use crate::config::SemanticModelJSONConfig;
use log::warn;
use log::{debug, error};
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct LocalSemanticModelStore {
    semantic_models: BTreeMap<String, SemanticModel>,
}

impl LocalSemanticModelStore {
    pub fn load_from_json(file_path: &str) -> Result<Self, SemanticModelStoreError> {
        let file = File::open(file_path)
            .map_err(|_| SemanticModelStoreError::FileNotFound(file_path.to_string()))?;
        let reader = BufReader::new(file);
        let store: LocalSemanticModelStore = serde_json::from_reader(reader)
            .map_err(|_| SemanticModelStoreError::InvalidJsonFormat)?;
        Ok(store)
    }

    pub fn new() -> Result<Self, SemanticModelStoreError> {
        match SemanticModelJSONConfig::new() {
            Ok(config) => {
                if Path::new(&config.json_path).exists() {
                    match Self::load_from_json(&config.json_path) {
                        Ok(store) => {
                            debug!(
                                "Loaded semantic models from JSON: {:?}",
                                store.semantic_models.keys()
                            );
                            Ok(store)
                        }
                        Err(e) => {
                            error!(
                                "Failed to load from JSON: {}. Falling back to mock data.",
                                e
                            );
                            Ok(Self::mock())
                        }
                    }
                } else {
                    warn!(
                        "File not found: {}. Falling back to mock data.",
                        config.json_path
                    );
                    Ok(Self::mock())
                }
            }
            Err(e) => {
                warn!("{}. Falling back to mock data.", e);
                Ok(Self::mock())
            }
        }
    }

    pub fn mock() -> Self {
        let mut semantic_models = BTreeMap::new();
        let employees_model = SemanticModel {
            name: "dm_employees".to_string(),
            label: "Employees".to_string(),
            description: "Dimensional model for employee data".to_string(),
            measures: vec![
                Measure::Simple(SimpleMeasure {
                    name: "headcount".to_string(),
                    description: "Count of distinct employees included in headcount".to_string(),
                    data_type: "INTEGER".to_string(),
                    aggregation: "count".to_string(),
                    // sql: "COUNT(DISTINCT CASE WHEN dm_employees.included_in_headcount THEN dm_employees.id ELSE NULL END)".to_string(),
                    // Use simpler re-write for easier to read test cases
                    sql: "dm_employees.id".to_string(),
                }),
                Measure::Simple(SimpleMeasure {
                    name: "ending_headcount".to_string(),
                    description: "Count of distinct effective dates for employees".to_string(),
                    data_type: "INTEGER".to_string(),
                    aggregation: "count_distinct".to_string(),
                    sql: "dm_employees.effective_date".to_string(),
                }),
            ],
            dimensions: vec![
                Dimension {
                    name: "department_level_1".to_string(),
                    description: "Top level department of the employee".to_string(),
                    data_type: "STRING".to_string(),
                },
                Dimension {
                    name: "id".to_string(),
                    description: "Unique identifier for the employee".to_string(),
                    data_type: "INTEGER".to_string(),
                },
                Dimension {
                    name: "included_in_headcount".to_string(),
                    description:
                        "Flag indicating if the employee is included in headcount calculations"
                            .to_string(),
                    data_type: "BOOLEAN".to_string(),
                },
                // You can add more dimensions here if needed
            ],
        };
        let dm_dept_model = SemanticModel {
            name: "dm_departments".to_string(),
            label: "Departments".to_string(),
            description: "Dimensional model for department data".to_string(),
            measures: vec![],
            dimensions: vec![Dimension {
                name: "department_level_1_name".to_string(),
                description: "Top level department of the employee".to_string(),
                data_type: "STRING".to_string(),
            }],
        };

        semantic_models.insert(employees_model.name.clone(), employees_model);
        semantic_models.insert(dm_dept_model.name.clone(), dm_dept_model);

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
    ) -> Result<BTreeMap<String, SemanticModel>, SemanticModelStoreError> {
        Ok(self.semantic_models.clone())
    }

    fn get_measure(
        &self,
        table_name: &str,
        measure_name: &str,
    ) -> Result<Measure, SemanticModelStoreError> {
        let semantic_model = self.get_semantic_model(table_name)?;
        semantic_model.get_measure(measure_name).cloned()
    }
}
