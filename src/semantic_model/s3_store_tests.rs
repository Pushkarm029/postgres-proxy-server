use std::collections::HashMap;
use aws_sdk_s3::{Client, Config, Credentials, Region};
use aws_smithy_http::body::SdkBody;
use aws_smithy_http::result::SdkError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::types::ByteStream;
use http::StatusCode;

// Update the test functions to use aws-sdk-s3 mocking
// ...

#[tokio::test]
async fn test_get_semantic_model() {
    let tenant = "test-tenant".to_string();
    let bucket_name = "test-bucket".to_string();
    let region = Region::UsWest2;

    let mock_response = ListObjectsV2Output {
        contents: Some(vec![
            Object {
                key: Some("test-tenant/model1.json".to_string()),
                ..Default::default()
            },
        ]),
        ..Default::default()
    };

    let mock_dispatcher = MockRequestDispatcher::with_status(200).with_json_body(&mock_response);
    let mock_credentials_provider = MockCredentialsProvider;
    let s3_client = S3Client::new_with(mock_dispatcher, mock_credentials_provider, region);

    let store = S3SemanticModelStore {
        tenant,
        s3_client,
        bucket_name,
    };

    let mock_model_json = r#"
    {
        "name": "model1",
        "label": "Model 1",
        "description": "Test model",
        "measures": [
            {
                "name": "count",
                "description": "Count of records",
                "data_type": "integer",
                "aggregation": "sum"
            }
        ],
        "dimensions": [
            {
                "name": "id",
                "description": "Unique identifier",
                "data_type": "integer",
                "is_primary_key": true
            },
            {
                "name": "label",
                "description": "Label for the record",
                "data_type": "string",
                "is_primary_key": false
            }
        ]
    }
    "#;

    let mock_reader = MockResponseReader::read_response(ReadMockResponse::new(mock_model_json));
    let mock_dispatcher = MockRequestDispatcher::with_status(200).with_body(mock_reader);
    let s3_client = S3Client::new_with(mock_dispatcher, MockCredentialsProvider, region);

    let store = S3SemanticModelStore {
        tenant,
        s3_client,
        bucket_name,
    };

    let semantic_model = store.get_semantic_model("model1").unwrap().unwrap();
    assert_eq!(semantic_model.name, "model1");
    assert_eq!(semantic_model.label, "Model 1");
    assert_eq!(semantic_model.description, "Test model");
    assert_eq!(semantic_model.measures.len(), 1);
    assert_eq!(semantic_model.measures[0].name, "count");
    assert_eq!(semantic_model.dimensions.len(), 2);
    assert_eq!(semantic_model.dimensions[0].name, "id");
    assert_eq!(semantic_model.dimensions[1].name, "label");
}

#[tokio::test]
async fn test_get_all_semantic_models() {
    let tenant = "test-tenant".to_string();
    let bucket_name = "test-bucket".to_string();
    let region = Region::UsWest2;

    let mock_response = ListObjectsV2Output {
        contents: Some(vec![
            Object {
                key: Some("test-tenant/model1.json".to_string()),
                ..Default::default()
            },
            Object {
                key: Some("test-tenant/model2.json".to_string()),
                ..Default::default()
            },
        ]),
        ..Default::default()
    };

    let mock_dispatcher = MockRequestDispatcher::with_status(200).with_json_body(&mock_response);
    let mock_credentials_provider = MockCredentialsProvider;
    let s3_client = S3Client::new_with(mock_dispatcher, mock_credentials_provider, region);

    let store = S3SemanticModelStore {
        tenant,
        s3_client,
        bucket_name,
    };

    let mock_model1_json = r#"
    {
        "name": "model1",
        "label": "Model 1",
        "description": "Test model 1",
        "measures": [
            {
                "name": "count",
                "description": "Count of records",
                "data_type": "integer",
                "aggregation": "sum"
            }
        ],
        "dimensions": [
            {
                "name": "id",
                "description": "Unique identifier",
                "data_type": "integer",
                "is_primary_key": true
            },
            {
                "name": "label",
                "description": "Label for the record",
                "data_type": "string",
                "is_primary_key": false
            }
        ]
    }
    "#;

    let mock_model2_json = r#"
    {
        "name": "model2",
        "label": "Model 2",
        "description": "Test model 2",
        "measures": [
            {
                "name": "count",
                "description": "Count of records",
                "data_type": "integer",
                "aggregation": "sum"
            }
        ],
        "dimensions": [
            {
                "name": "id",
                "description": "Unique identifier",
                "data_type": "integer",
                "is_primary_key": true
            },
            {
                "name": "label",
                "description": "Label for the record",
                "data_type": "string",
                "is_primary_key": false
            }
        ]
    }
    "#;

    let mock_reader1 = MockResponseReader::read_response(ReadMockResponse::new(mock_model1_json));
    let mock_reader2 = MockResponseReader::read_response(ReadMockResponse::new(mock_model2_json));
    let mock_dispatcher1 = MockRequestDispatcher::with_status(200).with_body(mock_reader1);
    let mock_dispatcher2 = MockRequestDispatcher::with_status(200).with_body(mock_reader2);
    let s3_client = S3Client::new_with(mock_dispatcher1.chain(mock_dispatcher2), MockCredentialsProvider, region);

    let store = S3SemanticModelStore {
        tenant,
        s3_client,
        bucket_name,
    };

    let semantic_models = store.get_all_semantic_models().unwrap();
    assert_eq!(semantic_models.len(), 2);
    assert!(semantic_models.contains_key("model1"));
    assert!(semantic_models.contains_key("model2"));
}