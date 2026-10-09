use crate::error::EtlError;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;

/// Shared by `extractor::s3` and `loader::s3` — both just need an
/// `ObjectStore` for a bucket, nothing extractor/loader-specific here.
///
/// Credentials come from the standard AWS environment variables
/// (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`), never
/// the pipeline JSON — an S3 key/secret embedded in a config file that
/// flows through the dashboard/API would be a real credential-leak risk.
pub fn build_store(
    bucket: &str,
    region: Option<&str>,
    endpoint_url: Option<&str>,
) -> Result<Arc<dyn ObjectStore>, EtlError> {
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);

    if let Some(region) = region {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = endpoint_url {
        builder = builder.with_endpoint(endpoint);
        // S3-compatible endpoints used for local/self-hosted testing
        // (e.g. MinIO) are typically plain http://, not https://.
        if endpoint.starts_with("http://") {
            builder = builder.with_allow_http(true);
        }
    }

    let store = builder
        .build()
        .map_err(|e| EtlError::ConfigError(format!("Failed to build S3 client: {}", e)))?;
    Ok(Arc::new(store))
}
