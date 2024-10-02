pub mod local_store;
mod s3_store;

use local_store::LocalSemanticModelStore;
pub use s3_store::S3SemanticModelStore;
use std::collections::HashMap;
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

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Measure {
    pub name: String,
    pub description: String,
    pub data_type: String,
    pub aggregation: String,
    pub sql: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Dimension {
    pub name: String,
    pub description: String,
    pub data_type: String,
    pub is_primary_key: bool,
}

#[derive(Clone)]
pub enum SemanticModelType {
    Local(LocalSemanticModelStore),
    S3(S3SemanticModelStore),
}

impl SemanticModelStore for SemanticModelType {
    fn get_semantic_model(&self, name: &str) -> Result<SemanticModel, SemanticModelStoreError> {
        match self {
            SemanticModelType::Local(local_semantic) => local_semantic.get_semantic_model(name),
            SemanticModelType::S3(s3_semantic) => s3_semantic.get_semantic_model(name),
        }
    }

    fn get_all_semantic_models(
        &self,
    ) -> Result<std::collections::HashMap<String, SemanticModel>, SemanticModelStoreError> {
        match self {
            SemanticModelType::Local(local_semantic) => local_semantic.get_all_semantic_models(),
            SemanticModelType::S3(s3_semantic) => s3_semantic.get_all_semantic_models(),
        }
    }

    fn get_measure(
        &self,
        table_name: &str,
        measure_name: &str,
    ) -> Result<Measure, SemanticModelStoreError> {
        match self {
            SemanticModelType::Local(local_semantic) => {
                local_semantic.get_measure(table_name, measure_name)
            }
            SemanticModelType::S3(s3_semantic) => s3_semantic.get_measure(table_name, measure_name),
        }
    }
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
    ) -> Result<HashMap<String, SemanticModel>, SemanticModelStoreError>;
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
}
