//! An object store implementation for Google Cloud Storage
use crate::{
    path::{Path, DELIMITER},
    util::format_prefix,
    GetResult, ListResult, ObjectMeta, ObjectStore, Result,
};
use async_trait::async_trait;
use bytes::Bytes;
use cloud_storage::{Client, Object};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use snafu::{ResultExt, Snafu};
use std::ops::Range;
use std::{convert::TryFrom, env};

/// A specialized `Error` for Google Cloud Storage object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
enum Error {
    #[snafu(display("Expected streamed data to have length {}, got {}", expected, actual))]
    DataDoesNotMatchLength { expected: usize, actual: usize },

    #[snafu(display(
        "Unable to PUT data. Bucket: {}, Location: {}, Error: {}",
        bucket,
        path,
        source
    ))]
    UnableToPutData {
        source: cloud_storage::Error,
        bucket: String,
        path: String,
    },

    #[snafu(display("Unable to list data. Bucket: {}, Error: {}", bucket, source,))]
    UnableToListData {
        source: cloud_storage::Error,
        bucket: String,
    },

    #[snafu(display("Unable to stream list data. Bucket: {}, Error: {}", bucket, source,))]
    UnableToStreamListData {
        source: cloud_storage::Error,
        bucket: String,
    },

    #[snafu(display(
        "Unable to DELETE data. Bucket: {}, Location: {}, Error: {}",
        bucket,
        path,
        source,
    ))]
    UnableToDeleteData {
        source: cloud_storage::Error,
        bucket: String,
        path: String,
    },

    #[snafu(display(
        "Unable to GET data. Bucket: {}, Location: {}, Error: {}",
        bucket,
        path,
        source,
    ))]
    UnableToGetData {
        source: cloud_storage::Error,
        bucket: String,
        path: String,
    },

    #[snafu(display(
        "Unable to GET data. Bucket: {}, Location: {}, Error: {}",
        bucket,
        path,
        source,
    ))]
    UnableToHeadData {
        source: cloud_storage::Error,
        bucket: String,
        path: String,
    },

    #[snafu(display(
        "Unable to copy object. Bucket: {}, Source: {}, Dest: {}, Error: {}",
        bucket,
        src,
        dest,
        source,
    ))]
    UnableToCopyObject {
        source: cloud_storage::Error,
        bucket: String,
        src: String,
        dest: String,
    },

    #[snafu(display(
        "Unable to rename object. Bucket: {}, Source: {}, Dest: {}, Error: {}",
        bucket,
        src,
        dest,
        source,
    ))]
    UnableToRenameObject {
        source: cloud_storage::Error,
        bucket: String,
        src: String,
        dest: String,
    },

    NotFound {
        path: String,
        source: cloud_storage::Error,
    },

    AlreadyExists {
        path: String,
        source: cloud_storage::Error,
    },
}

impl From<Error> for super::Error {
    fn from(source: Error) -> Self {
        match source {
            Error::NotFound { path, source } => Self::NotFound {
                path,
                source: source.into(),
            },
            _ => Self::Generic {
                store: "GCS",
                source: Box::new(source),
            },
        }
    }
}

/// Configuration for connecting to [Google Cloud Storage](https://cloud.google.com/storage/).
#[derive(Debug)]
pub struct GoogleCloudStorage {
    client: Client,
    bucket_name: String,
}

impl std::fmt::Display for GoogleCloudStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GoogleCloudStorage({})", self.bucket_name)
    }
}

#[async_trait]
impl ObjectStore for GoogleCloudStorage {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let bucket_name = self.bucket_name.clone();

        self.client
            .object()
            .create(
                &bucket_name,
                bytes.to_vec(),
                location.as_ref(),
                "application/octet-stream",
            )
            .await
            .context(UnableToPutDataSnafu {
                bucket: &self.bucket_name,
                path: location.to_string(),
            })?;

        Ok(())
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let bucket_name = self.bucket_name.clone();

        let bytes = self
            .client
            .object()
            .download(&bucket_name, location.as_ref())
            .await
            .map_err(|e| match e {
                cloud_storage::Error::Other(ref text) if text.starts_with("No such object") => {
                    Error::NotFound {
                        path: location.to_string(),
                        source: e,
                    }
                }
                _ => Error::UnableToGetData {
                    bucket: bucket_name.clone(),
                    path: location.to_string(),
                    source: e,
                },
            })?;

        let s = futures::stream::once(async move { Ok(bytes.into()) }).boxed();
        Ok(GetResult::Stream(s))
    }

    async fn get_range(&self, _location: &Path, _range: Range<usize>) -> Result<Bytes> {
        Err(super::Error::NotSupported { source: "cloud_storage_rs does not support range requests - https://github.com/ThouCheese/cloud-storage-rs/pull/111".into() })
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let object = self
            .client
            .object()
            .read(&self.bucket_name, location.as_ref())
            .await
            .map_err(|e| match e {
                cloud_storage::Error::Google(ref error) if error.error.code == 404 => {
                    Error::NotFound {
                        path: location.to_string(),
                        source: e,
                    }
                }
                _ => Error::UnableToHeadData {
                    bucket: self.bucket_name.clone(),
                    path: location.to_string(),
                    source: e,
                },
            })?;

        convert_object_meta(&object)
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let bucket_name = self.bucket_name.clone();

        self.client
            .object()
            .delete(&bucket_name, location.as_ref())
            .await
            .context(UnableToDeleteDataSnafu {
                bucket: &self.bucket_name,
                path: location.to_string(),
            })?;

        Ok(())
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let list_request = cloud_storage::ListRequest {
            prefix: format_prefix(prefix),
            ..Default::default()
        };
        let object_lists = self
            .client
            .object()
            .list(&self.bucket_name, list_request)
            .await
            .context(UnableToListDataSnafu {
                bucket: &self.bucket_name,
            })?;

        let bucket_name = self.bucket_name.clone();
        let objects = object_lists
            .map_ok(move |list| {
                futures::stream::iter(list.items.into_iter().map(|o| convert_object_meta(&o)))
            })
            .map_err(move |source| {
                crate::Error::from(Error::UnableToStreamListData {
                    source,
                    bucket: bucket_name.clone(),
                })
            });

        Ok(objects.try_flatten().boxed())
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let list_request = cloud_storage::ListRequest {
            prefix: format_prefix(prefix),
            delimiter: Some(DELIMITER.to_string()),
            ..Default::default()
        };

        let mut object_lists = Box::pin(
            self.client
                .object()
                .list(&self.bucket_name, list_request)
                .await
                .context(UnableToListDataSnafu {
                    bucket: &self.bucket_name,
                })?,
        );

        let result = match object_lists.next().await {
            None => ListResult {
                objects: vec![],
                common_prefixes: vec![],
                next_token: None,
            },
            Some(list_response) => {
                let list_response = list_response.context(UnableToStreamListDataSnafu {
                    bucket: &self.bucket_name,
                })?;

                let objects = list_response
                    .items
                    .iter()
                    .map(convert_object_meta)
                    .collect::<Result<_>>()?;

                let common_prefixes = list_response
                    .prefixes
                    .iter()
                    .map(Path::parse)
                    .collect::<Result<_, _>>()?;

                ListResult {
                    objects,
                    common_prefixes,
                    next_token: list_response.next_page_token,
                }
            }
        };

        Ok(result)
    }

    async fn copy(&self, source: &Path, dest: &Path) -> Result<()> {
        let source = source.to_raw();
        let dest = dest.to_raw();
        let bucket_name = self.bucket_name.clone();

        let source_obj = self
            .client
            .object()
            .read(&bucket_name, source)
            .await
            .map_err(|e| match e {
                cloud_storage::Error::Google(ref error) if error.error.code == 404 => {
                    Error::NotFound {
                        path: source.to_string(),
                        source: e,
                    }
                }
                _ => Error::UnableToCopyObject {
                    bucket: self.bucket_name.clone(),
                    src: source.to_string(),
                    dest: dest.to_string(),
                    source: e,
                },
            })?;
        // TODO: Handle different buckets?
        self.client
            .object()
            .copy(&source_obj, &bucket_name, dest)
            .await
            .context(UnableToCopyObjectSnafu {
                bucket: self.bucket_name.clone(),
                src: source.to_string(),
                dest: dest.to_string(),
            })?;

        Ok(())
    }

    async fn rename_no_replace(&self, _source: &Path, _dest: &Path) -> Result<()> {
        todo!("cloud-storage crate doesn't yet support rewrite_object with precondition")
    }
}

fn convert_object_meta(object: &Object) -> Result<ObjectMeta> {
    let location = Path::parse(&object.name)?;
    let last_modified = object.updated;
    let size = usize::try_from(object.size).expect("unsupported size on this platform");

    Ok(ObjectMeta {
        location,
        last_modified,
        size,
    })
}

/// Configure a connection to Google Cloud Storage.
pub fn new_gcs(
    service_account_path: impl AsRef<std::ffi::OsStr>,
    bucket_name: impl Into<String>,
) -> Result<GoogleCloudStorage> {
    // The cloud storage crate currently only supports authentication via
    // environment variables. Set the environment variable explicitly so
    // that we can optionally accept command line arguments instead.
    env::set_var("SERVICE_ACCOUNT", service_account_path);
    Ok(GoogleCloudStorage {
        client: Default::default(),
        bucket_name: bucket_name.into(),
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        tests::{
            get_nonexistent_object, list_uses_directories_correctly, list_with_delimiter,
            put_get_delete_list, rename_and_copy,
        },
        Error as ObjectStoreError, ObjectStore,
    };
    use bytes::Bytes;
    use std::env;

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    #[derive(Debug)]
    struct GoogleCloudConfig {
        bucket: String,
        service_account: String,
    }

    // Helper macro to skip tests if TEST_INTEGRATION and the GCP environment variables are not set.
    macro_rules! maybe_skip_integration {
        () => {{
            dotenv::dotenv().ok();

            let required_vars = ["OBJECT_STORE_BUCKET", "GOOGLE_SERVICE_ACCOUNT"];
            let unset_vars: Vec<_> = required_vars
                .iter()
                .filter_map(|&name| match env::var(name) {
                    Ok(_) => None,
                    Err(_) => Some(name),
                })
                .collect();
            let unset_var_names = unset_vars.join(", ");

            let force = std::env::var("TEST_INTEGRATION");

            if force.is_ok() && !unset_var_names.is_empty() {
                panic!(
                    "TEST_INTEGRATION is set, \
                            but variable(s) {} need to be set",
                    unset_var_names
                )
            } else if force.is_err() {
                eprintln!(
                    "skipping Google Cloud integration test - set {}TEST_INTEGRATION to run",
                    if unset_var_names.is_empty() {
                        String::new()
                    } else {
                        format!("{} and ", unset_var_names)
                    }
                );
                return;
            } else {
                GoogleCloudConfig {
                    bucket: env::var("OBJECT_STORE_BUCKET")
                        .expect("already checked OBJECT_STORE_BUCKET"),
                    service_account: env::var("GOOGLE_SERVICE_ACCOUNT")
                        .expect("already checked GOOGLE_SERVICE_ACCOUNT"),
                }
            }
        }};
    }

    #[tokio::test]
    async fn gcs_test() {
        let config = maybe_skip_integration!();
        let integration = new_gcs(config.service_account, config.bucket).unwrap();

        put_get_delete_list(&integration).await.unwrap();
        list_uses_directories_correctly(&integration).await.unwrap();
        list_with_delimiter(&integration).await.unwrap();
        rename_and_copy(&integration).await.unwrap();
    }

    #[tokio::test]
    async fn gcs_test_get_nonexistent_location() {
        let config = maybe_skip_integration!();
        let integration = new_gcs(config.service_account, &config.bucket).unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = integration.get(&location).await.unwrap_err();

        if let ObjectStoreError::NotFound { path, source } = err {
            let source_variant = source.downcast_ref::<cloud_storage::Error>();
            assert!(
                matches!(source_variant, Some(cloud_storage::Error::Other(_))),
                "got: {:?}",
                source_variant
            );
            assert_eq!(path, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type: {:?}", err)
        }
    }

    #[tokio::test]
    async fn gcs_test_get_nonexistent_bucket() {
        let mut config = maybe_skip_integration!();
        config.bucket = NON_EXISTENT_NAME.into();
        let integration = new_gcs(config.service_account, &config.bucket).unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("Unable to GET data"), "{}", err)
    }

    #[tokio::test]
    async fn gcs_test_delete_nonexistent_location() {
        let config = maybe_skip_integration!();
        let integration = new_gcs(config.service_account, &config.bucket).unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = integration.delete(&location).await.unwrap_err().to_string();
        assert!(err.contains("Unable to DELETE data"), "{}", err)
    }

    #[tokio::test]
    async fn gcs_test_delete_nonexistent_bucket() {
        let mut config = maybe_skip_integration!();
        config.bucket = NON_EXISTENT_NAME.into();
        let integration = new_gcs(config.service_account, &config.bucket).unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = integration.delete(&location).await.unwrap_err().to_string();
        assert!(err.contains("Unable to DELETE data"), "{}", err)
    }

    #[tokio::test]
    async fn gcs_test_put_nonexistent_bucket() {
        let mut config = maybe_skip_integration!();
        config.bucket = NON_EXISTENT_NAME.into();
        let integration = new_gcs(config.service_account, &config.bucket).unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);
        let data = Bytes::from("arbitrary data");

        let err = integration
            .put(&location, data)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("Unable to PUT data"), "{}", err)
    }
}
