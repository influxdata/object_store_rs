#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

//! # object_store
//!
//! This crate provides APIs for interacting with object storage services.
//!
//! It currently supports PUT, GET, DELETE, HEAD and list for:
//!
//! * [Google Cloud Storage](https://cloud.google.com/storage/)
//! * [Amazon S3](https://aws.amazon.com/s3/)
//! * [Azure Blob Storage](https://azure.microsoft.com/en-gb/services/storage/blobs/#overview)
//! * In-memory
//! * Local file storage
//!

#[cfg(feature = "aws")]
pub mod aws;
#[cfg(feature = "azure")]
pub mod azure;
#[cfg(feature = "gcp")]
pub mod gcp;
pub mod local;
pub mod memory;
mod path;
pub mod throttle;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use snafu::Snafu;
use std::fmt::{Debug, Formatter};

pub use path::*;

/// An alias for a dynamically dispatched object store implementation.
pub type DynObjectStore = dyn ObjectStore;

/// Universal API to multiple object store services.
#[async_trait]
pub trait ObjectStore: std::fmt::Display + Send + Sync + Debug + 'static {
    /// Save the provided bytes to the specified location.
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()>;

    /// Return the bytes that are stored at the specified location.
    async fn get(&self, location: &Path) -> Result<GetResult>;

    /// Return the metadata for the specified location
    async fn head(&self, location: &Path) -> Result<ObjectMeta>;

    /// Delete the object at the specified location.
    async fn delete(&self, location: &Path) -> Result<()>;

    /// List all the objects with the given prefix.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>>;

    /// List objects with the given prefix and an implementation specific
    /// delimiter. Returns common prefixes (directories) in addition to object
    /// metadata.
    ///
    /// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
    /// `foo/bar_baz/x`.
    async fn list_with_delimiter(&self, prefix: &Path) -> Result<ListResult>;
}

/// Result of a list call that includes objects, prefixes (directories) and a
/// token for the next set of results. Individual result sets may be limited to
/// 1,000 objects based on the underlying object storage's limitations.
#[derive(Debug)]
pub struct ListResult {
    /// Token passed to the API for the next page of list results.
    pub next_token: Option<String>,
    /// Prefixes that are common (like directories)
    pub common_prefixes: Vec<Path>,
    /// Object metadata for the listing
    pub objects: Vec<ObjectMeta>,
}

/// The metadata that describes an object.
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectMeta {
    /// The full path to the object
    pub location: Path,
    /// The last modified time
    pub last_modified: DateTime<Utc>,
    /// The size in bytes of the object
    pub size: usize,
}

/// Result for a get request
pub enum GetResult {
    /// A file and its path on the local filesystem
    File(tokio::fs::File, std::path::PathBuf),
    /// An asynchronous stream
    Stream(BoxStream<'static, Result<Bytes>>),
}

impl Debug for GetResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            GetResult::File(_, _) => write!(f, "GetResult(File)"),
            GetResult::Stream(_) => write!(f, "GetResult(Stream)"),
        }
    }
}

impl GetResult {
    /// Collects the data into a [`Vec<u8>`]
    pub async fn bytes(self) -> Result<Vec<u8>> {
        let mut stream = self.into_stream();
        let mut bytes = Vec::new();

        while let Some(next) = stream.next().await {
            bytes.extend_from_slice(next?.as_ref())
        }

        Ok(bytes)
    }

    /// Converts this into a byte stream
    pub fn into_stream(self) -> BoxStream<'static, Result<Bytes>> {
        match self {
            Self::File(file, path) => {
                tokio_util::codec::FramedRead::new(file, tokio_util::codec::BytesCodec::new())
                    .map_ok(|b| b.freeze())
                    .map_err(move |source| {
                        local::Error::UnableToReadBytes {
                            source,
                            path: path.clone(),
                        }
                        .into()
                    })
                    .boxed()
            }
            Self::Stream(s) => s,
        }
    }
}

/// A specialized `Result` for object store-related errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A specialized `Error` for object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Generic {} error: {}", store, source))]
    Generic {
        store: &'static str,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Object at location {} not found: {}", path, source))]
    NotFound {
        path: String,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(
        display("Encountered object with invalid path: {}", source),
        context(false)
    )]
    InvalidPath { source: ParseError },
}

#[cfg(test)]
mod test_util {
    use super::*;

    pub async fn flatten_list_stream(
        storage: &DynObjectStore,
        prefix: Option<&Path>,
    ) -> Result<Vec<Path>> {
        storage
            .list(prefix)
            .await?
            .map_ok(|meta| meta.location)
            .try_collect::<Vec<Path>>()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::flatten_list_stream;

    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = Error> = std::result::Result<T, E>;

    pub(crate) async fn put_get_delete_list(storage: &DynObjectStore) -> Result<()> {
        delete_fixtures(storage).await;

        let content_list = flatten_list_stream(storage, None).await?;
        assert!(
            content_list.is_empty(),
            "Expected list to be empty; found: {:?}",
            content_list
        );

        let location = Path::from("test_dir/test_file.json");

        let data = Bytes::from("arbitrary data");
        let expected_data = data.clone();
        storage.put(&location, data).await?;

        // List everything
        let content_list = flatten_list_stream(storage, None).await?;
        assert_eq!(content_list, &[location.clone()]);

        // List everything starting with a prefix that should return results
        let prefix = Path::from("test_dir");
        let content_list = flatten_list_stream(storage, Some(&prefix)).await?;
        assert_eq!(content_list, &[location.clone()]);

        // List everything starting with a prefix that shouldn't return results
        let prefix = Path::from("something");
        let content_list = flatten_list_stream(storage, Some(&prefix)).await?;
        assert!(content_list.is_empty());

        let read_data = storage.get(&location).await?.bytes().await?;
        assert_eq!(&*read_data, expected_data);

        let head = storage.head(&location).await?;
        assert_eq!(head.size, expected_data.len());

        storage.delete(&location).await?;

        let content_list = flatten_list_stream(storage, None).await?;
        assert!(content_list.is_empty());

        // Azure doesn't report semantic errors
        let is_azure = storage.to_string().starts_with("MicrosoftAzure");

        let err = storage.get(&location).await.unwrap_err();
        assert!(
            matches!(err, crate::Error::NotFound { .. }) || is_azure,
            "{}",
            err
        );

        let err = storage.head(&location).await.unwrap_err();
        assert!(
            matches!(err, crate::Error::NotFound { .. }) || is_azure,
            "{}",
            err
        );

        // Test handling of paths containing an encoded delimiter

        let file_with_delimiter = Path::from_iter(["a", "b/c", "foo.file"]);
        storage
            .put(&file_with_delimiter, Bytes::from("arbitrary"))
            .await
            .unwrap();

        let files = flatten_list_stream(storage, None).await.unwrap();
        assert_eq!(files, vec![file_with_delimiter.clone()]);

        let files = flatten_list_stream(storage, Some(&Path::from("a/b")))
            .await
            .unwrap();
        assert!(files.is_empty());

        let files = storage
            .list_with_delimiter(&Path::from("a/b"))
            .await
            .unwrap();
        assert!(files.common_prefixes.is_empty());
        assert!(files.objects.is_empty());

        let files = storage.list_with_delimiter(&Path::from("a")).await.unwrap();
        assert_eq!(files.common_prefixes, vec![Path::from_iter(["a", "b/c"])]);
        assert!(files.objects.is_empty());

        let files = storage
            .list_with_delimiter(&Path::from_iter(["a", "b/c"]))
            .await
            .unwrap();
        assert!(files.common_prefixes.is_empty());
        assert_eq!(files.objects.len(), 1);
        assert_eq!(files.objects[0].location, file_with_delimiter);

        storage.delete(&file_with_delimiter).await.unwrap();

        // Test handling of paths containing non-ASCII characters, e.g. emoji

        let emoji_prefix = Path::from("🙀");
        let emoji_file = Path::from("🙀/😀.parquet");
        storage
            .put(&emoji_file, Bytes::from("arbitrary"))
            .await
            .unwrap();

        storage.head(&emoji_file).await.unwrap();
        storage
            .get(&emoji_file)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        let files = flatten_list_stream(storage, Some(&emoji_prefix))
            .await
            .unwrap();

        assert_eq!(files, vec![emoji_file.clone()]);

        storage.delete(&emoji_file).await.unwrap();
        let files = flatten_list_stream(storage, Some(&emoji_prefix))
            .await
            .unwrap();
        assert!(files.is_empty());

        Ok(())
    }

    pub(crate) async fn list_uses_directories_correctly(storage: &DynObjectStore) -> Result<()> {
        delete_fixtures(storage).await;

        let content_list = flatten_list_stream(storage, None).await?;
        assert!(
            content_list.is_empty(),
            "Expected list to be empty; found: {:?}",
            content_list
        );

        let location1 = Path::from("foo/x.json");
        let location2 = Path::from("foo.bar/y.json");

        let data = Bytes::from("arbitrary data");
        storage.put(&location1, data.clone()).await?;
        storage.put(&location2, data).await?;

        let prefix = Path::from("foo");
        let content_list = flatten_list_stream(storage, Some(&prefix)).await?;
        assert_eq!(content_list, &[location1.clone()]);

        let prefix = Path::from("foo/x");
        let content_list = flatten_list_stream(storage, Some(&prefix)).await?;
        assert_eq!(content_list, &[]);

        Ok(())
    }

    pub(crate) async fn list_with_delimiter(storage: &DynObjectStore) -> Result<()> {
        delete_fixtures(storage).await;

        // ==================== check: store is empty ====================
        let content_list = flatten_list_stream(storage, None).await?;
        assert!(content_list.is_empty());

        // ==================== do: create files ====================
        let data = Bytes::from("arbitrary data");

        let files: Vec<_> = [
            "test_file",
            "mydb/wb/000/000/000.segment",
            "mydb/wb/000/000/001.segment",
            "mydb/wb/000/000/002.segment",
            "mydb/wb/001/001/000.segment",
            "mydb/wb/foo.json",
            "mydb/wbwbwb/111/222/333.segment",
            "mydb/data/whatevs",
        ]
        .iter()
        .map(|&s| Path::from(s))
        .collect();

        for f in &files {
            let data = data.clone();
            storage.put(f, data).await.unwrap();
        }

        // ==================== check: prefix-list `mydb/wb` (directory) ====================
        let prefix = Path::from("mydb/wb");

        let expected_000 = Path::from("mydb/wb/000");
        let expected_001 = Path::from("mydb/wb/001");
        let expected_location = Path::from("mydb/wb/foo.json");

        let result = storage.list_with_delimiter(&prefix).await.unwrap();

        assert_eq!(result.common_prefixes, vec![expected_000, expected_001]);
        assert_eq!(result.objects.len(), 1);

        let object = &result.objects[0];

        assert_eq!(object.location, expected_location);
        assert_eq!(object.size, data.len());

        // ==================== check: prefix-list `mydb/wb/000/000/001` (partial filename doesn't match) ====================
        let prefix = Path::from("mydb/wb/000/000/001");

        let result = storage.list_with_delimiter(&prefix).await.unwrap();
        assert!(result.common_prefixes.is_empty());
        assert_eq!(result.objects.len(), 0);

        // ==================== check: prefix-list `not_there` (non-existing prefix) ====================
        let prefix = Path::from("not_there");

        let result = storage.list_with_delimiter(&prefix).await.unwrap();
        assert!(result.common_prefixes.is_empty());
        assert!(result.objects.is_empty());

        // ==================== do: remove all files ====================
        for f in &files {
            storage.delete(f).await.unwrap();
        }

        // ==================== check: store is empty ====================
        let content_list = flatten_list_stream(storage, None).await?;
        assert!(content_list.is_empty());

        Ok(())
    }

    pub(crate) async fn get_nonexistent_object(
        storage: &DynObjectStore,
        location: Option<Path>,
    ) -> Result<Vec<u8>> {
        let location = location.unwrap_or_else(|| Path::from("this_file_should_not_exist"));

        let err = storage.head(&location).await.unwrap_err();
        assert!(matches!(err, crate::Error::NotFound { .. }));

        Ok(storage.get(&location).await?.bytes().await?)
    }

    async fn delete_fixtures(storage: &DynObjectStore) {
        let paths = flatten_list_stream(storage, None).await.unwrap();

        for f in &paths {
            let _ = storage.delete(f).await;
        }
    }

    /// Test that the returned stream does not borrow the lifetime of Path
    async fn list_store<'a>(
        store: &'a dyn ObjectStore,
        path_str: &str,
    ) -> super::Result<BoxStream<'a, super::Result<ObjectMeta>>> {
        let path = Path::from(path_str);
        store.list(Some(&path)).await
    }

    #[tokio::test]
    async fn test_list_lifetimes() {
        let store = memory::InMemory::new();
        let stream = list_store(&store, "path").await.unwrap();
        assert_eq!(stream.count().await, 0);
    }

    // Tests TODO:
    // GET nonexisting location (in_memory/file)
    // DELETE nonexisting location
    // PUT overwriting
}
