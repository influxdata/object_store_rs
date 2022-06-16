//! An object store implementation for Azure blob storage
use crate::{
    path::{Path, DELIMITER},
    util::format_prefix,
    GetResult, ListResult, ObjectMeta, ObjectStore, Result,
};
use async_trait::async_trait;
use azure_core::{
    error::{Error as AzureError, ErrorKind as AzureErrorKind},
    prelude::*,
    HttpClient,
};
use azure_storage::core::prelude::{AsStorageClient, StorageAccountClient};
use azure_storage_blobs::blob::Blob;
use azure_storage_blobs::{
    prelude::{AsBlobClient, AsContainerClient, ContainerClient},
    DeleteSnapshotsMethod,
};
use bytes::Bytes;
use futures::{
    stream::{self, BoxStream},
    StreamExt, TryStreamExt,
};
use snafu::{ResultExt, Snafu};
use std::{convert::TryInto, sync::Arc};

/// A specialized `Error` for Azure object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
enum Error {
    #[snafu(display(
        "Unable to DELETE data. Container: {}, Location: {}, Error: {} ({:?})",
        container,
        path,
        source,
        source,
    ))]
    UnableToDeleteData {
        source: AzureError,
        container: String,
        path: String,
    },

    #[snafu(display(
        "Unable to GET data. Container: {}, Location: {}, Error: {} ({:?})",
        container,
        path,
        source,
        source,
    ))]
    UnableToGetData {
        source: AzureError,
        container: String,
        path: String,
    },

    #[snafu(display(
        "Unable to HEAD data. Container: {}, Location: {}, Error: {} ({:?})",
        container,
        path,
        source,
        source,
    ))]
    UnableToHeadData {
        source: AzureError,
        container: String,
        path: String,
    },

    #[snafu(display(
        "Unable to GET part of the data. Container: {}, Location: {}, Error: {} ({:?})",
        container,
        path,
        source,
        source,
    ))]
    UnableToGetPieceOfData {
        source: AzureError,
        container: String,
        path: String,
    },

    #[snafu(display(
        "Unable to PUT data. Bucket: {}, Location: {}, Error: {} ({:?})",
        container,
        path,
        source,
        source,
    ))]
    UnableToPutData {
        source: AzureError,
        container: String,
        path: String,
    },

    #[snafu(display(
        "Unable to list data. Bucket: {}, Error: {} ({:?})",
        container,
        source,
        source,
    ))]
    UnableToListData {
        source: AzureError,
        container: String,
    },

    #[snafu(display(
        "Unable to copy object. Container: {}, From: {}, To: {}, Error: {}",
        container,
        from,
        to,
        source
    ))]
    UnableToCopyFile {
        source: AzureError,
        container: String,
        from: String,
        to: String,
    },

    #[snafu(display("Unable parse source url. Container: {}, Error: {}", container, source))]
    UnableToParseUrl {
        source: url::ParseError,
        container: String,
    },

    NotFound {
        path: String,
        source: AzureError,
    },

    AlreadyExists {
        path: String,
        source: AzureError,
    },

    #[cfg(not(feature = "azure_test"))]
    #[snafu(display(
        "Azurite (azure emulator) support not compiled in, please add `azure_test` feature"
    ))]
    NoEmulatorFeature,
}

impl From<Error> for super::Error {
    fn from(source: Error) -> Self {
        match source {
            Error::NotFound { path, source } => Self::NotFound {
                path,
                source: Box::new(source),
            },
            Error::AlreadyExists { path, source } => Self::AlreadyExists {
                path,
                source: Box::new(source),
            },
            _ => Self::Generic {
                store: "Azure Blob Storage",
                source: Box::new(source),
            },
        }
    }
}

/// Configuration for connecting to [Microsoft Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/).
#[derive(Debug)]
pub struct MicrosoftAzure {
    container_client: Arc<ContainerClient>,
    container_name: String,
    blob_base_url: String,
    is_emulator: bool,
}

impl std::fmt::Display for MicrosoftAzure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.is_emulator {
            true => write!(f, "MicrosoftAzureEmulator({})", self.container_name),
            false => write!(f, "MicrosoftAzure({})", self.container_name),
        }
    }
}

#[allow(clippy::borrowed_box)]
fn check_err_not_found(err: &AzureError) -> bool {
    if let Some(cast_err) = err.downcast_ref::<AzureError>() {
        if let AzureErrorKind::HttpResponse { status, .. } = cast_err.kind() {
            return *status == 404;
        }
    };
    false
}

#[async_trait]
impl ObjectStore for MicrosoftAzure {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let bytes = bytes::BytesMut::from(&*bytes);

        self.container_client
            .as_blob_client(location.as_ref())
            .put_block_blob(bytes)
            .execute()
            .await
            .context(UnableToPutDataSnafu {
                container: &self.container_name,
                path: location.to_owned(),
            })?;

        Ok(())
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let blob = self
            .container_client
            .as_blob_client(location.as_ref())
            .get()
            .execute()
            .await
            .map_err(|err| {
                if check_err_not_found(&err) {
                    return Error::NotFound {
                        source: err,
                        path: location.to_string(),
                    };
                };
                Error::UnableToGetData {
                    source: err,
                    container: self.container_name.clone(),
                    path: location.to_string(),
                }
            })?;

        Ok(GetResult::Stream(
            futures::stream::once(async move { Ok(blob.data) }).boxed(),
        ))
    }

    async fn get_range(&self, location: &Path, range: std::ops::Range<usize>) -> Result<Bytes> {
        let blob = self
            .container_client
            .as_blob_client(location.as_ref())
            .get()
            .range(range)
            .execute()
            .await
            .map_err(|err| {
                if check_err_not_found(&err) {
                    return Error::NotFound {
                        source: err,
                        path: location.to_string(),
                    };
                };
                Error::UnableToGetPieceOfData {
                    source: err,
                    container: self.container_name.clone(),
                    path: location.to_string(),
                }
            })?;

        Ok(blob.data)
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let s = self
            .container_client
            .as_blob_client(location.as_ref())
            .get_properties()
            .execute()
            .await
            .map_err(|err| {
                if check_err_not_found(&err) {
                    return Error::NotFound {
                        source: err,
                        path: location.to_string(),
                    };
                };
                Error::UnableToHeadData {
                    source: err,
                    container: self.container_name.clone(),
                    path: location.to_string(),
                }
            })?;

        convert_object_meta(s.blob)
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.container_client
            .as_blob_client(location.as_ref())
            .delete()
            .delete_snapshots_method(DeleteSnapshotsMethod::Include)
            .execute()
            .await
            .context(UnableToDeleteDataSnafu {
                container: &self.container_name,
                path: location.to_string(),
            })?;

        Ok(())
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        #[derive(Clone)]
        enum ListState {
            Start,
            HasMore(String),
            Done,
        }

        let prefix_raw = format_prefix(prefix);

        Ok(stream::unfold(ListState::Start, move |state| {
            let mut request = self.container_client.list_blobs();

            if let Some(p) = prefix_raw.as_deref() {
                request = request.prefix(p);
            }

            async move {
                match state {
                    ListState::HasMore(ref marker) => {
                        request = request.next_marker(marker as &str);
                    }
                    ListState::Done => {
                        return None;
                    }
                    ListState::Start => {}
                }

                let resp = match request.execute().await.context(UnableToListDataSnafu {
                    container: &self.container_name,
                }) {
                    Ok(resp) => resp,
                    Err(err) => return Some((Err(crate::Error::from(err)), state)),
                };

                let next_state = if let Some(marker) = resp.next_marker {
                    ListState::HasMore(marker.as_str().to_string())
                } else {
                    ListState::Done
                };

                let names = resp
                    .blobs
                    .blobs
                    .into_iter()
                    .map(convert_object_meta)
                    .filter(|it| match it {
                        // This is needed to filter out gen2 directories
                        // https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-known-issues#blob-storage-apis
                        Ok(meta) => meta.size > 0,
                        Err(_) => true,
                    });
                Some((Ok(futures::stream::iter(names)), next_state))
            }
        })
        .try_flatten()
        .boxed())
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let mut request = self.container_client.list_blobs();

        request = request.delimiter(Delimiter::new(DELIMITER));
        if let Some(prefix) = format_prefix(prefix) {
            request = request.prefix(prefix)
        }

        let resp = request.execute().await.context(UnableToListDataSnafu {
            container: &self.container_name,
        })?;

        let next_token = resp.next_marker.as_ref().map(|m| m.as_str().to_string());

        let prefixes = resp.blobs.blob_prefix.unwrap_or_default();

        let common_prefixes = prefixes
            .iter()
            .map(|p| Path::parse(&p.name))
            .collect::<Result<_, _>>()?;

        let objects = resp
            .blobs
            .blobs
            .into_iter()
            .map(convert_object_meta)
            .collect::<Result<_>>()?;

        Ok(ListResult {
            next_token,
            common_prefixes,
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let from_url = self.get_copy_from_url(from)?;
        self.container_client
            .as_blob_client(to.as_ref())
            .copy(&from_url)
            .execute()
            .await
            .context(UnableToCopyFileSnafu {
                container: &self.container_name,
                from: from.as_ref(),
                to: to.as_ref(),
            })?;
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let from_url = self.get_copy_from_url(from)?;
        self.container_client
            .as_blob_client(to.as_ref())
            .copy(&from_url)
            .if_match_condition(IfMatchCondition::NotMatch("*".to_string()))
            .execute()
            .await
            .map_err(|err| {
                if let AzureErrorKind::HttpResponse { status, .. } = err.kind() {
                    if *status == 409 {
                        return Error::AlreadyExists {
                            source: err,
                            path: to.to_string(),
                        };
                    };
                };
                Error::UnableToCopyFile {
                    source: err,
                    container: self.container_name.clone(),
                    from: from.to_string(),
                    to: to.to_string(),
                }
            })?;
        Ok(())
    }
}

impl MicrosoftAzure {
    /// helper function to create a source url for copy function
    fn get_copy_from_url(&self, from: &Path) -> Result<reqwest::Url> {
        Ok(reqwest::Url::parse(&format!(
            "{}/{}/{}",
            &self.blob_base_url, self.container_name, from
        ))
        .context(UnableToParseUrlSnafu {
            container: &self.container_name,
        })?)
    }
}

fn convert_object_meta(blob: Blob) -> Result<ObjectMeta> {
    let location = Path::parse(blob.name)?;
    let last_modified = blob.properties.last_modified;
    let size = blob
        .properties
        .content_length
        .try_into()
        .expect("unsupported size on this platform");

    Ok(ObjectMeta {
        location,
        last_modified,
        size,
    })
}

#[cfg(feature = "azure_test")]
fn check_if_emulator_works() -> Result<()> {
    Ok(())
}

#[cfg(not(feature = "azure_test"))]
fn check_if_emulator_works() -> Result<()> {
    Err(Error::NoEmulatorFeature.into())
}

/// Configure a connection to container with given name on Microsoft Azure
/// Blob store.
///
/// The credentials `account` and `access_key` must provide access to the
/// store.
pub fn new_azure(
    account: impl Into<String>,
    access_key: impl Into<String>,
    container_name: impl Into<String>,
    use_emulator: bool,
) -> Result<MicrosoftAzure> {
    let account = account.into();
    let access_key = access_key.into();
    let http_client: Arc<dyn HttpClient> = Arc::new(reqwest::Client::new());

    let (is_emulator, storage_account_client) = if use_emulator {
        check_if_emulator_works()?;
        (true, StorageAccountClient::new_emulator_default())
    } else {
        (
            false,
            StorageAccountClient::new_access_key(Arc::clone(&http_client), &account, &access_key),
        )
    };

    let storage_client = storage_account_client.as_storage_client();
    let blob_base_url = storage_account_client
        .blob_storage_url()
        .as_ref()
        // make url ending consistent between the emulator and remote storage account
        .trim_end_matches('/')
        .to_string();

    let container_name = container_name.into();

    let container_client = storage_client.as_container_client(&container_name);

    Ok(MicrosoftAzure {
        container_client,
        container_name,
        blob_base_url,
        is_emulator,
    })
}

#[cfg(test)]
mod tests {
    use crate::azure::new_azure;
    use crate::tests::{
        copy_if_not_exists, list_uses_directories_correctly, list_with_delimiter,
        put_get_delete_list, rename_and_copy,
    };
    use std::env;

    #[derive(Debug)]
    struct AzureConfig {
        storage_account: String,
        access_key: String,
        bucket: String,
        use_emulator: bool,
    }

    // Helper macro to skip tests if TEST_INTEGRATION and the Azure environment
    // variables are not set.
    macro_rules! maybe_skip_integration {
        () => {{
            dotenv::dotenv().ok();

            let use_emulator = std::env::var("AZURE_USE_EMULATOR").is_ok();

            let mut required_vars = vec!["OBJECT_STORE_BUCKET"];
            if !use_emulator {
                required_vars.push("AZURE_STORAGE_ACCOUNT");
                required_vars.push("AZURE_STORAGE_ACCESS_KEY");
            }
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
                    "skipping Azure integration test - set {}TEST_INTEGRATION to run",
                    if unset_var_names.is_empty() {
                        String::new()
                    } else {
                        format!("{} and ", unset_var_names)
                    }
                );
                return;
            } else {
                AzureConfig {
                    storage_account: env::var("AZURE_STORAGE_ACCOUNT").unwrap_or_default(),
                    access_key: env::var("AZURE_STORAGE_ACCESS_KEY").unwrap_or_default(),
                    bucket: env::var("OBJECT_STORE_BUCKET")
                        .expect("already checked OBJECT_STORE_BUCKET"),
                    use_emulator,
                }
            }
        }};
    }

    #[tokio::test]
    async fn azure_blob_test() {
        let config = maybe_skip_integration!();
        let integration = new_azure(
            config.storage_account,
            config.access_key,
            config.bucket,
            config.use_emulator,
        )
        .unwrap();

        put_get_delete_list(&integration).await.unwrap();
        list_uses_directories_correctly(&integration).await.unwrap();
        list_with_delimiter(&integration).await.unwrap();
        rename_and_copy(&integration).await.unwrap();
        copy_if_not_exists(&integration).await.unwrap();
    }
}
