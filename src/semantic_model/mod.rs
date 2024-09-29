pub mod local_store;
mod s3_store;
mod store;

pub use s3_store::S3SemanticModelStore;
pub use store::SemanticModelStore;

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
