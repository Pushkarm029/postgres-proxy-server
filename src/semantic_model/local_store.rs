use crate::semantic_model::store::{SemanticModelStore, SemanticModelStoreError};
use crate::semantic_model::{Measure, SemanticModel};
use std::collections::HashMap;

#[derive(Clone)]
pub struct LocalSemanticModelStore {
    semantic_models: HashMap<String, SemanticModel>,
}

impl LocalSemanticModelStore {
    pub fn new() -> Self {
        let mut semantic_models = HashMap::new();

        // // Add some dummy semantic models
        // // TODO(ethan): add some real models from the examples file
        // let model1 = SemanticModel {
        //     name: "table1".to_string(),
        //     measures: vec![
        //         Measure {
        //             name: "measure1".to_string(),
        //             // ... other measure fields ...
        //         },
        //         Measure {
        //             name: "measure2".to_string(),
        //             // ... other measure fields ...
        //         },
        //     ],
        //     // ... other semantic model fields ...
        // };

        // let model2 = SemanticModel {
        //     name: "table2".to_string(),
        //     measures: vec![
        //         Measure {
        //             name: "measure3".to_string(),
        //             // ... other measure fields ...
        //         },
        //         Measure {
        //             name: "measure4".to_string(),
        //             // ... other measure fields ...
        //         },
        //     ],
        //     // ... other semantic model fields ...
        // };

        // semantic_models.insert(model1.name.clone(), model1);
        // semantic_models.insert(model2.name.clone(), model2);

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
        // ... implementation similar to S3SemanticModelStore ...
        todo!()
    }
}
