use crate::semantic_model::{Measure, SemanticModel};
use std::collections::HashMap;
use thiserror::Error;

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
