//! An object store implementation for Azure Datalake storage Gen2
use crate::{
    azure::convert_object_meta,
    path::{Path, DELIMITER},
    util::format_prefix,
    GetResult, ListResult, ObjectMeta, ObjectStore, Result,
};
use async_trait::async_trait;
use azure_core::{
    error::{Error as AzureError, ErrorKind as AzureErrorKind},
    ClientOptions,
};
use azure_core::{prelude::Delimiter, HttpClient};
use azure_storage::core::prelude::{AsStorageClient, StorageAccountClient};
use azure_storage::core::storage_shared_key_credential::StorageSharedKeyCredential;
use azure_storage_blobs::prelude::{AsBlobClient, AsContainerClient, ContainerClient};
use azure_storage_datalake::prelude::{DataLakeClient, FileSystemClient};
use bytes::Bytes;
use futures::{
    stream::{self, BoxStream},
    StreamExt,
};
use snafu::{ResultExt, Snafu};
use std::sync::Arc;

/// A specialized `Error` for Azure object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
enum Error {
    #[snafu(display(
        "Unable to DELETE data. Filesystem: {}, Location: {}, Error: {} ({:?})",
        file_system,
        path,
        source,
        source,
    ))]
    UnableToDeleteData {
        source: AzureError,
        file_system: String,
        path: String,
    },

    #[snafu(display(
        "Unable to GET data. Filesystem: {}, Location: {}, Error: {} ({:?})",
        file_system,
        path,
        source,
        source,
    ))]
    UnableToGetData {
        source: AzureError,
        file_system: String,
        path: String,
    },

    #[snafu(display(
        "Unable to HEAD data. Filesystem: {}, Location: {}, Error: {} ({:?})",
        file_system,
        path,
        source,
        source,
    ))]
    UnableToHeadData {
        source: AzureError,
        file_system: String,
        path: String,
    },

    #[snafu(display(
        "Unable to GET part of the data. Filesystem: {}, Location: {}, Error: {} ({:?})",
        file_system,
        path,
        source,
        source,
    ))]
    UnableToGetPieceOfData {
        source: AzureError,
        file_system: String,
        path: String,
    },

    #[snafu(display(
        "Unable to PUT data. Bucket: {}, Location: {}, Error: {} ({:?})",
        file_system,
        path,
        source,
        source,
    ))]
    UnableToPutData {
        source: AzureError,
        file_system: String,
        path: String,
    },

    #[snafu(display(
        "Unable to list data. Bucket: {}, Error: {} ({:?})",
        file_system,
        source,
        source,
    ))]
    UnableToListData {
        source: AzureError,
        file_system: String,
    },

    #[snafu(display(
        "Unable to copy object. Filesystem: {}, From: {}, To: {}, Error: {}",
        file_system,
        from,
        to,
        source
    ))]
    UnableToCopyFile {
        source: Box<dyn std::error::Error + Send + Sync>,
        file_system: String,
        from: String,
        to: String,
    },

    #[snafu(display(
        "Unable to rename object. Filesystem: {}, From: {}, To: {}, Error: {}",
        file_system,
        from,
        to,
        source
    ))]
    UnableToRenameFile {
        source: AzureError,
        file_system: String,
        from: String,
        to: String,
    },

    #[snafu(display(
        "Unable parse source url. Filesystem: {}, Error: {}",
        file_system,
        source
    ))]
    UnableToParseUrl {
        source: url::ParseError,
        file_system: String,
    },

    NotFound {
        path: String,
        source: AzureError,
    },

    AlreadyExists {
        path: String,
        source: AzureError,
    },
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
                store: "Azure Data Lake Storage Gen2",
                source: Box::new(source),
            },
        }
    }
}

/// Configuration for connecting to [Microsoft Data Lake Storage Gen2](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction).
#[derive(Debug)]
pub struct MicrosoftAzureAdls2 {
    file_system_client: Arc<FileSystemClient>,
    container_client: Arc<ContainerClient>,
    blob_base_url: String,
    file_system_name: String,
}

impl std::fmt::Display for MicrosoftAzureAdls2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MicrosoftAzureAdls2({})", self.file_system_name)
    }
}

#[async_trait]
impl ObjectStore for MicrosoftAzureAdls2 {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let location = location.as_ref();

        let bytes = bytes::BytesMut::from(&*bytes);
        let length = bytes.len() as i64;

        let file_client = self.file_system_client.get_file_client(location);
        file_client
            .create()
            .into_future()
            .await
            .context(UnableToPutDataSnafu {
                file_system: &self.file_system_name,
                path: location.to_owned(),
            })?;
        file_client
            .append(0, bytes)
            .into_future()
            .await
            .context(UnableToPutDataSnafu {
                file_system: &self.file_system_name,
                path: location.to_owned(),
            })?;
        file_client
            .flush(length)
            .close(true)
            .into_future()
            .await
            .context(UnableToPutDataSnafu {
                file_system: &self.file_system_name,
                path: location.to_owned(),
            })?;

        Ok(())
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let location = location.as_ref();

        let blob = self
            .file_system_client
            .get_file_client(location)
            .read()
            .into_future()
            .await
            .map_err(|err| match err.kind() {
                AzureErrorKind::HttpResponse { status, .. } if *status == 404 => Error::NotFound {
                    source: err,
                    path: location.to_string(),
                },
                _ => Error::UnableToGetData {
                    source: err,
                    file_system: self.file_system_name.clone(),
                    path: location.to_string(),
                },
            })?;

        Ok(GetResult::Stream(
            futures::stream::once(async move { Ok(blob.data) }).boxed(),
        ))
    }

    async fn get_range(&self, location: &Path, range: std::ops::Range<usize>) -> Result<Bytes> {
        let location = location.as_ref();

        let response = self
            .file_system_client
            .get_file_client(location)
            .read()
            .range(range)
            .into_future()
            .await
            .map_err(|err| match err.kind() {
                AzureErrorKind::HttpResponse { status, .. } if *status == 404 => Error::NotFound {
                    source: err,
                    path: location.to_string(),
                },
                _ => Error::UnableToGetPieceOfData {
                    source: err,
                    file_system: self.file_system_name.clone(),
                    path: location.to_string(),
                },
            })?;

        Ok(response.data)
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let raw_location = location.as_ref();

        let properties = self
            .file_system_client
            .get_file_client(raw_location)
            .get_properties()
            .into_future()
            .await
            .map_err(|err| match err.kind() {
                AzureErrorKind::HttpResponse { status, .. } if *status == 404 => Error::NotFound {
                    source: err,
                    path: location.to_string(),
                },
                _ => Error::UnableToHeadData {
                    source: err,
                    file_system: self.file_system_name.clone(),
                    path: location.to_string(),
                },
            })?;

        Ok(ObjectMeta {
            last_modified: properties.last_modified,
            location: location.clone(),
            size: usize::try_from(properties.content_length)
                .expect("unsupported size on this platform"),
        })
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let location = location.as_ref();

        self.file_system_client
            .get_file_client(location)
            .delete()
            .into_future()
            .await
            .context(UnableToDeleteDataSnafu {
                file_system: &self.file_system_name,
                path: location.to_owned(),
            })?;

        Ok(())
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let prefix_raw = format_prefix(prefix);
        let mut request = self.file_system_client.list_paths().recursive(true);
        if let Some(p) = prefix_raw.as_deref() {
            request = request.directory(p);
        }
        Ok(request
            .into_stream()
            .flat_map(|f| match f {
                Ok(lst) => stream::iter(lst.paths.into_iter().filter(|it| !it.is_directory).map(
                    |p| {
                        Ok(ObjectMeta {
                            size: p
                                .content_length
                                .try_into()
                                .expect("unsupported size on this platform"),
                            last_modified: p.last_modified,
                            location: Path::parse(p.name)?,
                        })
                    },
                )),
                // TODO handle error case
                Err(err) => {
                    let err_stream = stream::iter(vec![Err(Error::UnableToListData {
                        source: err,
                        file_system: self.file_system_name.clone(),
                    })]);
                    println!("{:?}", err);
                    err_stream
                }
            })
            .boxed())
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let mut request = self.container_client.list_blobs();

        request = request.delimiter(Delimiter::new(DELIMITER));
        if let Some(prefix) = format_prefix(prefix) {
            request = request.prefix(prefix)
        }

        let resp = request.execute().await.context(UnableToListDataSnafu {
            file_system: &self.file_system_name,
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
        let from_url = reqwest::Url::parse(&format!(
            "{}/{}/{}",
            &self.blob_base_url, self.file_system_name, from
        ))
        .context(UnableToParseUrlSnafu {
            file_system: &self.file_system_name,
        })?;

        self.container_client
            .as_blob_client(to.as_ref())
            .copy(&from_url)
            .execute()
            .await
            .context(UnableToCopyFileSnafu {
                file_system: &self.file_system_name,
                from: from.as_ref(),
                to: to.as_ref(),
            })?;

        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        // the gen2 APIs do not provide a copy method. The blob api however does not check
        // if the target object exists. To achieve the desired behavior, we copy
        // the source to a temporary object and try to rename it using the gen2 api
        let tmp_file = Path::from("");
        self.copy(from, &tmp_file).await?;
        match self.rename_if_not_exists(&tmp_file, to).await {
            Ok(_) => Ok(()),
            Err(err) => {
                self.delete(&tmp_file).await?;
                Err(Error::UnableToCopyFile {
                    source: Box::new(err),
                    file_system: self.file_system_name.clone(),
                    from: from.to_string(),
                    to: to.to_string(),
                })
            }
        }?;
        Ok(())
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let file_client = self.file_system_client.get_file_client(from.as_ref());
        file_client
            .rename_if_not_exists(to.as_ref())
            .into_future()
            .await
            .map_err(|err| match err.kind() {
                // TODO check messages to make sure its the source file that does not exist.
                AzureErrorKind::HttpResponse { status, .. } if *status == 404 => Error::NotFound {
                    source: err,
                    path: from.to_string(),
                },
                AzureErrorKind::HttpResponse { status, .. } if *status == 409 => {
                    Error::AlreadyExists {
                        source: err,
                        path: to.to_string(),
                    }
                }
                _ => Error::UnableToRenameFile {
                    source: err,
                    file_system: self.file_system_name.clone(),
                    from: from.to_string(),
                    to: to.to_string(),
                },
            })?;
        Ok(())
    }
}

/// Configure a connection to container with given name on Microsoft Azure Data Lake Gen2 store.
///
/// The credentials `account` and `access_key` must provide access to the store.
pub fn new_azure(
    account: impl Into<String>,
    access_key: impl Into<String>,
    container_name: impl Into<String>,
) -> Result<MicrosoftAzureAdls2> {
    let account = account.into();
    let access_key = access_key.into();
    let http_client: Arc<dyn HttpClient> = Arc::new(reqwest::Client::new());
    let storage_account_client =
        StorageAccountClient::new_access_key(Arc::clone(&http_client), &account, &access_key);

    let storage_client = storage_account_client.as_storage_client();
    let blob_base_url = storage_account_client
        .blob_storage_url()
        .as_ref()
        // make url ending consistent between the emulator and remote storage account
        .trim_end_matches('/')
        .to_string();

    let file_system_name = container_name.into();
    let container_client = storage_client.as_container_client(&file_system_name);
    let credential = StorageSharedKeyCredential::new(account.into(), access_key.into());
    let file_system_client = Arc::new(
        DataLakeClient::new_with_shared_key(credential, None, ClientOptions::default())
            .into_file_system_client(&file_system_name),
    );

    Ok(MicrosoftAzureAdls2 {
        file_system_client,
        container_client,
        file_system_name,
        blob_base_url,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::azure_adls2::new_azure;
    use crate::test_util::flatten_list_stream;
    use crate::tests::{
        list_uses_directories_correctly, list_with_delimiter, put_get_delete_list, rename_and_copy,
    };
    use azure_storage_datalake::file_system::PathList;
    use std::env;

    #[derive(Debug)]
    struct AzureConfig {
        storage_account: String,
        access_key: String,
        bucket: String,
    }

    // Helper macro to skip tests if TEST_INTEGRATION and the Azure environment
    // variables are not set.
    macro_rules! maybe_skip_integration {
        () => {{
            dotenv::dotenv().ok();

            let required_vars = vec!["OBJECT_STORE_BUCKET"];
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
                }
            }
        }};
    }

    #[tokio::test]
    async fn azure_adls_gen2_test() {
        let config = maybe_skip_integration!();
        let integration =
            new_azure(config.storage_account, config.access_key, config.bucket).unwrap();

        put_get_delete_list(&integration).await.unwrap();
        // list_uses_directories_correctly(&integration).await.unwrap();
        // list_with_delimiter(&integration).await.unwrap();
        // rename_and_copy(&integration).await.unwrap();
    }

    #[tokio::test]
    async fn asd_asd() {
        let config = maybe_skip_integration!();
        let storage = new_azure(config.storage_account, config.access_key, config.bucket).unwrap();

        // let content_list = flatten_list_stream(&storage, None).await.unwrap();
        // assert!(
        //     content_list.is_empty(),
        //     "Expected list to be empty; found: {:?}",
        //     content_list
        // );

        let location = Path::from("test_dir/test_file.json");
        let data = b"{\"paths\":[{\"contentLength\":\"14\",\"isDirectory\":true,\"etag\":\"0x8DA4CAC1DEB8DD7\",\"group\":\"$superuser\",\"lastModified\":\"Sun, 12 Jun 2022 19:45:34 GMT\",\"name\":\"test_dir/test_file.json\",\"owner\":\"$superuser\",\"permissions\":\"rw-r-----\"}]}";
        let parsed = serde_json::from_slice::<PathList>(data);
        println!("PARSED {:?}", parsed);

        let data = Bytes::from("arbitrary data");
        let expected_data = data.clone();
        // storage.put(&location, data).await.unwrap();
        let head = storage.head(&location).await.unwrap();
        println!("{:?}", head);

        let root = Path::from("/");

        // List everything
        let content_list = flatten_list_stream(&storage, Some(&root)).await.unwrap();
        println!("LIST {:?}", content_list);
        assert_eq!(content_list, &[location.clone()]);
    }
}
