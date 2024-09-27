use crate::semantic_model::model::{Measure, SemanticModel};
use std::collections::HashMap;
use std::error::Error;
use thiserror::Error;
use std::fmt::Display;

pub trait SemanticModelStore {
    fn get_semantic_model(&self, name: &str) -> Result<Option<SemanticModel>, Box<dyn Error>>;
    fn get_all_semantic_models(&self) -> Result<HashMap<String, SemanticModel>, Box<dyn Error>>;
    fn get_measure(
        &self,
        table_name: &str,
        measure_name: &str,
    ) -> Result<Option<Measure>, Box<dyn Error>>;
}

#[derive(Error, Debug)]
pub enum SemanticModelStoreError {
    #[error("Measure not found")]
    MeasureNotFound,
}

impl fmt::Display for SemanticModelStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}
