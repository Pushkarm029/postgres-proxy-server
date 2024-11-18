use crate::config::S3Config;

use super::{Measure, SemanticModel, SemanticModelStore, SemanticModelStoreError};
use aws_sdk_s3::{config::BehaviorVersion, Client};
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use tokio::runtime::Runtime;

#[derive(Clone)]
pub struct S3SemanticModelStore {
    tenant: String,
    s3_client: Client,
    bucket_name: String,
}

impl S3SemanticModelStore {
    pub async fn new(config: S3Config) -> Self {
        let S3Config {
            tenant,
            bucket_name,
        } = config;
        let shared_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let s3_client = Client::new(&shared_config);
        S3SemanticModelStore {
            tenant,
            s3_client,
            bucket_name,
        }
    }

    async fn get_object_content(&self, key: &str) -> Result<Option<String>, Box<dyn Error>> {
        let bucket_key = format!("{}/{}", self.tenant, key);
        let result = self
            .s3_client
            .get_object()
            .bucket(self.bucket_name.clone())
            .key(bucket_key)
            .send()
            .await?;

        let body = result.body.collect().await?;

        let content = String::from_utf8(body.to_vec())?;
        Ok(Some(content))
    }

    async fn list_objects(&self) -> Result<Vec<String>, Box<dyn Error>> {
        let bucket_key = format!("{}/", self.tenant);
        let result = self
            .s3_client
            .list_objects_v2()
            .bucket(self.bucket_name.clone())
            .prefix(bucket_key)
            .send()
            .await?;

        let keys = result
            .contents()
            .iter()
            .map(|c| c.key().unwrap().to_string())
            .filter(|k| k.ends_with(".json"))
            .collect();

        Ok(keys)
    }
}

impl SemanticModelStore for S3SemanticModelStore {
    fn get_semantic_model(&self, name: &str) -> Result<SemanticModel, SemanticModelStoreError> {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let key = format!("{}.json", name);
            let content = self.get_object_content(&key).await.unwrap();
            match content {
                Some(json) => {
                    let semantic_model = serde_json::from_str(&json).unwrap();
                    Ok(semantic_model)
                }
                None => Err(SemanticModelStoreError::MeasureNotFound),
            }
        })
    }

    fn get_all_semantic_models(
        &self,
    ) -> Result<BTreeMap<String, SemanticModel>, SemanticModelStoreError> {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let keys = self.list_objects().await.unwrap();
            let mut semantic_models = BTreeMap::new();
            for key in keys {
                if let Some(name) = key.strip_suffix(".json") {
                    let semantic_model = self.get_semantic_model(name)?;
                    semantic_models.insert(name.to_string(), semantic_model);
                }
            }
            Ok(semantic_models)
        })
    }

    fn get_measure(
        &self,
        table_name: &str,
        measure_name: &str,
    ) -> Result<Measure, SemanticModelStoreError> {
        let semantic_model = self.get_semantic_model(table_name)?;
        let measure = semantic_model.get_measure(measure_name)?;
        Ok(measure.clone())
    }
}
