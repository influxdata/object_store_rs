//! An object store implementation for S3 using AWS-SDK-RUST
use crate::path::DELIMITER;
use crate::{util::*, BoxStream, GetResult, ListResult, ObjectMeta, ObjectStore, Path, Result};
use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::model::Object;
use aws_sdk_s3::output::ListObjectsV2Output;
use aws_smithy_http::byte_stream::ByteStream;
use aws_smithy_types_convert::date_time::DateTimeExt;
use bytes::Bytes;
use chrono::Utc;
use futures::{stream, TryStreamExt};
use snafu::Snafu;
use std::fmt;
use std::num::NonZeroUsize;
use std::ops::{Deref, Range};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// A specialized `Error` for object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
enum Error {
    #[snafu(display(
        "Unable to DELETE data. Bucket: {}, Location: {}, Error: {} ({:?})",
        bucket,
        path,
        source,
        source,
    ))]
    UnableToDeleteData {
        source: aws_sdk_s3::types::SdkError<aws_sdk_s3::error::DeleteObjectError>,
        bucket: String,
        path: String,
    },

    #[snafu(display(
        "Unable to GET data. Bucket: {}, Location: {}, Error: {} ({:?})",
        bucket,
        path,
        source,
        source,
    ))]
    UnableToGetData {
        source: aws_sdk_s3::types::SdkError<aws_sdk_s3::error::GetObjectError>,
        bucket: String,
        path: String,
    },

    #[snafu(display(
        "Unable to HEAD data. Bucket: {}, Location: {}, Error: {} ({:?})",
        bucket,
        path,
        source,
        source,
    ))]
    UnableToHeadData {
        source: aws_sdk_s3::types::SdkError<aws_sdk_s3::error::HeadObjectError>,
        bucket: String,
        path: String,
    },

    #[snafu(display(
        "Unable to GET part of the data. Bucket: {}, Location: {}, Error: {} ({:?})",
        bucket,
        path,
        source,
        source,
    ))]
    UnableToGetPieceOfData {
        source: std::io::Error,
        bucket: String,
        path: String,
    },

    #[snafu(display(
        "Unable to PUT data. Bucket: {}, Location: {}, Error: {} ({:?})",
        bucket,
        path,
        source,
        source,
    ))]
    UnableToPutData {
        source: aws_sdk_s3::types::SdkError<aws_sdk_s3::error::PutObjectError>,
        bucket: String,
        path: String,
    },

    #[snafu(display(
        "Unable to list data. Bucket: {}, Error: {} ({:?})",
        bucket,
        source,
        source,
    ))]
    UnableToListData {
        source: aws_sdk_s3::types::SdkError<aws_sdk_s3::error::ListObjectsV2Error>,
        bucket: String,
    },

    #[snafu(display(
        "Unable to copy object. Bucket: {}, From: {}, To: {}, Error: {}",
        bucket,
        from,
        to,
        source,
    ))]
    UnableToCopyObject {
        source: aws_sdk_s3::types::SdkError<aws_sdk_s3::error::CopyObjectError>,
        bucket: String,
        from: String,
        to: String,
    },

    #[snafu(display(
        "Unable to parse last modified date. Bucket: {}, Error: {} ({:?})",
        bucket,
        source,
        source,
    ))]
    UnableToParseLastModified {
        source: chrono::ParseError,
        bucket: String,
    },

    NotFound {
        path: String,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

impl From<Error> for super::Error {
    fn from(source: Error) -> Self {
        match source {
            Error::NotFound { path, source } => Self::NotFound { path, source },
            _ => Self::Generic {
                store: "S3",
                source: Box::new(source),
            },
        }
    }
}

/// Configuration for connecting to [Amazon S3](https://aws.amazon.com/s3/).
pub struct AmazonS3 {
    /// S3 client w/o any connection limit.
    ///
    /// You should normally use [`Self::client`] instead.
    client_unrestricted: aws_sdk_s3::Client,

    /// Semaphore that limits the usage of [`client_unrestricted`](Self::client_unrestricted).
    connection_semaphore: Arc<Semaphore>,

    /// Bucket name used by this object store client.
    bucket_name: String,
}

impl fmt::Debug for AmazonS3 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AmazonS3")
            .field("client", &"rusoto_s3::S3Client")
            .field("bucket_name", &self.bucket_name)
            .finish()
    }
}

impl fmt::Display for AmazonS3 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AmazonS3({})", self.bucket_name)
    }
}

#[async_trait]
impl ObjectStore for AmazonS3 {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let key = location.to_string();
        let client = self.client().await.inner;
        let _ = client
            .put_object()
            .bucket(self.bucket_name.clone())
            .key(key.clone())
            .body(ByteStream::from(bytes))
            .send()
            .await
            .map_err(|e| Error::UnableToPutData {
                source: e,
                bucket: self.bucket_name.clone(),
                path: key.clone(),
            })?;
        Ok(())
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let key = location.to_string();
        let bucket_name = self.bucket_name.clone();
        let bytestream = self.get_object(key.clone(), None).await?;
        let stream = bytestream
            .map_err(move |e| Error::UnableToGetPieceOfData {
                source: e.into(),
                bucket: bucket_name.clone(),
                path: key.clone(),
            })
            .err_into();
        Ok(GetResult::Stream(Box::pin(stream)))
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let key = location.to_string();
        let bytestream = self.get_object(key.clone(), Some(range)).await?;
        let data = bytestream
            .collect()
            .await
            .map_err(move |e| Error::UnableToGetPieceOfData {
                source: e.into(),
                bucket: self.bucket_name.clone(),
                path: key,
            })?;
        Ok(data.into_bytes())
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let client = self.client().await.inner;
        let key = location.to_string();
        let head_object_output = client
            .head_object()
            .bucket(self.bucket_name.clone())
            .key(key.clone())
            .send()
            .await
            .map_err(|e| Error::UnableToHeadData {
                source: e,
                bucket: self.bucket_name.clone(),
                path: key.clone(),
            })?;

        let last_modified = head_object_output
            .last_modified
            .map(|sdt| sdt.to_chrono_utc())
            .unwrap_or_else(Utc::now);
        let size = usize::try_from(head_object_output.content_length)
            .expect("unsupported size on this platform");

        Ok(ObjectMeta {
            location: location.to_owned(),
            last_modified,
            size,
        })
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let client = self.client().await.inner;
        let key = location.to_string();
        let _ = client
            .delete_object()
            .bucket(&self.bucket_name)
            .key(key.clone())
            .send()
            .await
            .map_err(|e| Error::UnableToDeleteData {
                source: e,
                bucket: self.bucket_name.clone(),
                path: key.clone(),
            })?;
        Ok(())
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let objects_stream = self.list_objects_v2(prefix, None).await?;
        use futures::StreamExt;
        Ok(objects_stream
            .map_ok(|list_objects_v2_output| {
                stream::iter(
                    list_objects_v2_output
                        .contents
                        .unwrap_or_default()
                        .into_iter()
                        .map(|x| convert_object_meta(&x)),
                )
            })
            .try_flatten()
            .boxed())
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let objects_stream = self
            .list_objects_v2(prefix, Some(DELIMITER.to_string()))
            .await?;

        objects_stream
            .try_fold(
                ListResult {
                    next_token: None,
                    common_prefixes: vec![],
                    objects: vec![],
                },
                |acc, list_objects_v2_result| async move {
                    let mut res = acc;
                    let contents = list_objects_v2_result.contents.unwrap_or_default();
                    let mut objects = contents
                        .iter()
                        .map(convert_object_meta)
                        .collect::<Result<Vec<_>>>()?;

                    res.objects.append(&mut objects);

                    let prefixes = list_objects_v2_result.common_prefixes.unwrap_or_default();
                    res.common_prefixes.reserve(prefixes.len());

                    for p in prefixes {
                        let prefix = p.prefix.expect("can't have a prefix without a value");
                        res.common_prefixes.push(Path::parse(prefix)?);
                    }

                    Ok(res)
                },
            )
            .await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let client = self.client().await.inner;
        let src = from.to_string();
        let key = to.to_string();
        let _ = client
            .copy_object()
            .bucket(self.bucket_name.clone())
            .copy_source(src.clone())
            .key(key.clone())
            .send()
            .await
            .map_err(|e| Error::UnableToCopyObject {
                source: e,
                bucket: self.bucket_name.clone(),
                from: src.clone(),
                to: key.clone(),
            })?;
        Ok(())
    }

    async fn copy_if_not_exists(&self, _: &Path, _: &Path) -> Result<()> {
        Err(crate::Error::NotImplemented)
    }
}

fn convert_object_meta(object: &Object) -> Result<ObjectMeta> {
    let key = &object
        .key
        .clone()
        .expect("object doesn't exist without a key");
    let location = Path::parse(key)?;
    let last_modified = object
        .last_modified
        .map(|sdt| sdt.to_chrono_utc())
        .unwrap_or_else(Utc::now);
    let size = usize::try_from(object.size).expect("unsupported size on this platform");
    Ok(ObjectMeta {
        location,
        last_modified,
        size,
    })
}

/// Configure a connection to Amazon S3 inferred from the environment
/// and then override with the provided settings
#[allow(clippy::too_many_arguments)]
pub async fn new_s3(
    credentials_provider: Option<aws_types::credentials::SharedCredentialsProvider>,
    region: Option<aws_sdk_config::Region>,
    endpoint: Option<aws_sdk_config::Endpoint>,
    retry_config: Option<aws_sdk_config::RetryConfig>,
    sleep: Option<Arc<dyn aws_smithy_async::rt::sleep::AsyncSleep>>,
    timeout_config: Option<aws_config::timeout::Config>,
    bucket_name: String,
    max_connections: NonZeroUsize,
) -> AmazonS3 {
    let config = aws_config::load_from_env().await;

    let region_provider = RegionProviderChain::first_try(region)
        .or_default_provider()
        .or_else(aws_sdk_config::Region::new("eu-central-1"));

    let mut config_builder =
        aws_sdk_s3::config::Builder::from(&config).region(region_provider.region().await);

    if let Some(credentials_provider) = credentials_provider {
        config_builder = config_builder.credentials_provider(credentials_provider);
    }

    if let Some(endpoint) = endpoint {
        config_builder = config_builder.endpoint_resolver(endpoint);
    }

    if let Some(retry_config) = retry_config {
        config_builder = config_builder.retry_config(retry_config);
    }

    if let Some(sleep) = sleep {
        config_builder = config_builder.sleep_impl(sleep);
    }

    if let Some(timeout_config) = timeout_config {
        config_builder = config_builder.timeout_config(timeout_config);
    };

    let config = config_builder.build();

    let client = aws_sdk_s3::Client::from_conf(config);

    AmazonS3 {
        client_unrestricted: client,
        connection_semaphore: Arc::new(Semaphore::new(max_connections.get())),
        bucket_name,
    }
}

/// S3 client bundled w/ a semaphore permit.
#[derive(Clone)]
struct SemaphoreClient {
    /// Permit for this specific use of the client.
    ///
    /// Note that this field is never read and therefore considered "dead code" by rustc.
    #[allow(dead_code)]
    permit: Arc<OwnedSemaphorePermit>,

    inner: aws_sdk_s3::Client,
}

impl Deref for SemaphoreClient {
    type Target = aws_sdk_s3::Client;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl AmazonS3 {
    /// Get a client according to the current connection limit.
    async fn client(&self) -> SemaphoreClient {
        let permit = Arc::clone(&self.connection_semaphore)
            .acquire_owned()
            .await
            .expect("semaphore shouldn't be closed yet");
        SemaphoreClient {
            permit: Arc::new(permit),
            inner: self.client_unrestricted.clone(),
        }
    }

    async fn list_objects_v2(
        &self,
        prefix: Option<&Path>,
        delimiter: Option<String>,
    ) -> Result<BoxStream<'_, Result<ListObjectsV2Output>>> {
        #[derive(Clone)]
        enum ListState {
            Start,
            HasMore(String),
            Done,
        }

        let prefix = format_prefix(prefix);
        let objects = self
            .client()
            .await
            .inner
            .list_objects_v2()
            .bucket(self.bucket_name.clone())
            .set_prefix(prefix)
            .set_delimiter(delimiter);

        let stream = stream::unfold(ListState::Start, move |state| {
            let req = objects.clone();
            async move {
                match state {
                    ListState::Start => {
                        let result = req.set_continuation_token(None).send().await;
                        match result {
                            Ok(list_result) if list_result.is_truncated => {
                                let next_continuation_token = list_result.next_continuation_token.clone().expect("when output is truncated a next continuation token is required");
                                Some((Ok(list_result), ListState::HasMore(next_continuation_token)))
                            }
                            Ok(list_result) if !list_result.is_truncated => {
                                Some((Ok(list_result), ListState::Done))
                            }
                            Err(e) => Some((
                                Err(Error::UnableToListData {
                                    source: e,
                                    bucket: self.bucket_name.clone(),
                                }
                                .into()),
                                state.clone(),
                            )),
                            _ => unreachable!(),
                        }
                    }
                    ListState::HasMore(ref token) => {
                        let result = req.set_continuation_token(Some(token.clone())).send().await;
                        match result {
                            Ok(list_result) if list_result.is_truncated => {
                                let next_continuation_token = list_result.next_continuation_token.clone().expect("when output is truncated a next continuation token is required");
                                Some((Ok(list_result), ListState::HasMore(next_continuation_token)))
                            }
                            Ok(list_result) if !list_result.is_truncated => {
                                Some((Ok(list_result), ListState::Done))
                            }
                            Err(e) => Some((
                                Err(Error::UnableToListData {
                                    source: e,
                                    bucket: self.bucket_name.clone(),
                                }
                                .into()),
                                state.clone(),
                            )),
                            _ => unreachable!(),
                        }
                    }
                    ListState::Done => None,
                }
            }
        });

        Ok(Box::pin(stream))
    }

    async fn get_object(&self, key: String, range: Option<Range<usize>>) -> Result<ByteStream> {
        let client = self.client().await.inner;
        let get_object_output = client
            .get_object()
            .bucket(&self.bucket_name)
            .key(key.clone())
            .set_range(range.map(format_http_range))
            .send()
            .await
            .map_err(|e| Error::UnableToGetData {
                source: e,
                bucket: self.bucket_name.clone(),
                path: key.clone(),
            })?;
        Ok(get_object_output.body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_config::Endpoint;
    use aws_types::credentials::SharedCredentialsProvider;
    use aws_types::Credentials;
    use hyper::Uri;
    use std::env::var;
    use std::io::Read;
    use std::path::PathBuf;
    use stream::StreamExt;
    use tokio::fs::File;

    const ACCESS_KEY_ID: &str = "AKIAIOSFODNN7EXAMPLE";
    const SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    const PROVIDER_NAME: &str = "Static";
    const MINIO_ENDPOINT: &str = "http://localhost:9000";
    const BUCKET_NAME: &str = "simple";

    async fn make_integration() -> AmazonS3 {
        let max_connections = NonZeroUsize::new(10).expect("n:10 is not zero");
        new_s3(
            Some(SharedCredentialsProvider::new(Credentials::new(
                ACCESS_KEY_ID,
                SECRET_ACCESS_KEY,
                None,
                None,
                PROVIDER_NAME,
            ))),
            None,
            Some(Endpoint::immutable(Uri::from_static(MINIO_ENDPOINT))),
            None,
            None,
            None,
            BUCKET_NAME.to_string(),
            max_connections,
        )
        .await
    }

    #[tokio::test]
    async fn test_list_with_delimiter() -> Result<()> {
        let integration = make_integration().await;
        let list_result = integration.list_with_delimiter(None).await?;
        assert_eq!(list_result.next_token, None);
        assert_eq!(
            list_result.common_prefixes,
            vec![Path::parse("empty")?, Path::parse("loremipsum")?]
        );
        assert_eq!(list_result.objects.len(), 0);

        let prefix_nested_items = integration
            .list_with_delimiter(Some(&Path::parse("loremipsum")?))
            .await?;
        assert_eq!(prefix_nested_items.common_prefixes, vec![]);
        assert_eq!(prefix_nested_items.objects.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_list() -> Result<()> {
        let integration = make_integration().await;
        let prefix_none_items = integration
            .list(None)
            .await?
            .collect::<Vec<Result<ObjectMeta>>>()
            .await;
        assert_eq!(prefix_none_items.len(), 1006);

        let prefix_nested_items = integration
            .list(Some(&Path::parse("empty")?))
            .await?
            .collect::<Vec<Result<ObjectMeta>>>()
            .await;
        assert_eq!(prefix_nested_items.len(), 1005);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_range() -> Result<()> {
        let integration = make_integration().await;
        let test_data_file = "loremipsum/loremipsum-5.txt";
        let range = 0..30;
        let get_result = integration
            .get_range(&Path::parse(test_data_file)?, 0..30)
            .await?;
        let test_data_file_contents = read_simple_test_data_file(test_data_file).await?;
        let expected = &test_data_file_contents[range];
        assert_eq!(get_result, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_get() -> Result<()> {
        let integration = make_integration().await;
        let test_data_file = "loremipsum/loremipsum-5.txt";
        let get_result = integration.get(&Path::parse(test_data_file)?).await?;
        let actual = get_result_bytes(get_result).await?;
        let expected = read_simple_test_data_file(test_data_file).await?;
        assert_eq!(actual, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_put() -> Result<()> {
        let integration = make_integration().await;
        let test_data_file = "loremipsum/loremipsum-5.txt";
        let test_data_contents = read_simple_test_data_file(test_data_file).await?;
        let test_data_bytes = Bytes::from(test_data_contents.clone());
        let target_path = Path::parse("test_put/loremipsum.txt")?;
        let _ = integration.delete(&target_path).await;
        let _ = integration.put(&target_path, test_data_bytes).await?;
        let get_result = integration.get(&target_path).await?;
        let written_data_bytes = get_result_bytes(get_result).await?;
        assert_eq!(written_data_bytes, test_data_contents);
        let _ = integration.delete(&target_path).await;
        Ok(())
    }

    async fn get_result_bytes(get_result: GetResult) -> Result<Vec<u8>> {
        match get_result {
            GetResult::File(mut file, _) => {
                let mut bytes = Vec::new();
                let _ = file.read_to_end(&mut bytes);
                Ok(bytes)
            }
            GetResult::Stream(stream) => collect_bytes_and_ignore_errors(stream).await,
        }
    }

    async fn collect_bytes_and_ignore_errors(
        stream: BoxStream<'_, Result<Bytes>>,
    ) -> Result<Vec<u8>> {
        use futures::AsyncReadExt;
        let mut collected_bytes = Vec::new();
        stream
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            .into_async_read()
            .read_to_end(&mut collected_bytes)
            .await
            .map_err(|e| crate::Error::Generic {
                store: "TEST",
                source: Box::new(e),
            })?;
        Ok(collected_bytes)
    }

    /// Returns the simple test data directory, which is by default stored in `testdata/simple`.
    fn simple_test_data() -> String {
        match get_data_dir("SIMPLE_TEST_DATA", "./testdata/simple") {
            Ok(pb) => pb.display().to_string(),
            Err(err) => panic!("failed to get parquet data dir: {}", err),
        }
    }

    async fn read_simple_test_data_file(path_in_simple_test_data: &str) -> Result<Vec<u8>> {
        let mut f = File::open(format!(
            "{}/{}",
            simple_test_data(),
            path_in_simple_test_data
        ))
        .await
        .map_err(|e| crate::Error::Generic {
            store: "TEST",
            source: Box::new(e),
        })?;
        let mut contents = vec![];
        use tokio::io::AsyncReadExt;
        f.read_to_end(&mut contents)
            .await
            .map_err(|e| crate::Error::Generic {
                store: "TEST",
                source: Box::new(e),
            })?;
        Ok(contents)
    }

    /// Returns a directory path for finding test data.
    ///
    /// udf_env: name of an environment variable
    ///
    /// submodule_dir: fallback path (relative to CARGO_MANIFEST_DIR)
    ///
    ///  Returns either:
    /// The path referred to in `udf_env` if that variable is set and refers to a directory
    /// The submodule_data directory relative to CARGO_MANIFEST_PATH
    fn get_data_dir(
        udf_env: &str,
        testdata_sub_dir: &str,
    ) -> std::result::Result<PathBuf, Box<dyn std::error::Error>> {
        // Try user defined env.
        if let Ok(dir) = var(udf_env) {
            let trimmed = dir.trim().to_string();
            if !trimmed.is_empty() {
                let pb = PathBuf::from(trimmed);
                if pb.is_dir() {
                    return Ok(pb);
                } else {
                    return Err(format!(
                        "the data dir `{}` defined by env {} not found",
                        pb.display(),
                        udf_env
                    )
                    .into());
                }
            }
        }

        // The env is undefined or its value is trimmed to empty, let's try default dir.

        // env "CARGO_MANIFEST_DIR" is "the directory containing the manifest of your package",
        // set by `cargo run` or `cargo test`, see:
        // https://doc.rust-lang.org/cargo/reference/environment-variables.html
        let dir = env!("CARGO_MANIFEST_DIR");

        let pb = PathBuf::from(dir).join(testdata_sub_dir);
        if pb.is_dir() {
            Ok(pb)
        } else {
            Err(format!(
                "env `{}` is undefined or has empty value, and the pre-defined data dir `{}` not found\n\
             HINT: try running `git submodule update --init`",
                udf_env,
                pb.display(),
            ).into())
        }
    }
}
