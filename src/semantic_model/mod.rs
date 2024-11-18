pub mod local_store;
pub mod measure;
pub mod s3_store;

use measure::Measure;
use std::collections::{BTreeMap, HashMap};
use thiserror::Error;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SemanticModel {
    pub name: String,
    pub label: String,
    pub description: String,
    pub measures: Vec<Measure>,
    pub dimensions: Vec<Dimension>,
}

impl SemanticModel {
    pub fn get_measure(&self, name: &str) -> Result<&Measure, SemanticModelStoreError> {
        self.measures
            .iter()
            .find(|m| m.name() == name)
            .ok_or(SemanticModelStoreError::MeasureNotFound)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Dimension {
    pub name: String,
    pub description: String,
    pub data_type: String,
}

/// [`SemanticModel`] store
/// TODO: make this an async trait with all functions async
///
/// Since the production semantic model store will be across the network
/// and a local implementation for testing can be made async trivially
pub trait SemanticModelStore: Clone {
    fn get_semantic_model(&self, name: &str) -> Result<SemanticModel, SemanticModelStoreError>;
    fn get_all_semantic_models(
        &self,
    ) -> Result<BTreeMap<String, SemanticModel>, SemanticModelStoreError>;
    fn get_measure(
        &self,
        table_name: &str,
        measure_name: &str,
    ) -> Result<Measure, SemanticModelStoreError>;
}

// TODO: extend semantic model store error to support other error types like from async and serde_json
// for properly supporting s3_store
#[derive(Error, Debug)]
pub enum SemanticModelStoreError {
    #[error("Measure not found")]
    MeasureNotFound,

    #[error("Dimension not found")]
    DimensionNotFound,

    #[error("Model not found")]
    ModelNotFound,

    #[error("File not found: {0}")]
    FileNotFound(String),

    #[error("Invalid JSON format")]
    InvalidJsonFormat,

    #[error("Invalid JSON path")]
    InvalidJsonPath,

    #[error("Env var not set")]
    EnvVarNotSet,
}
