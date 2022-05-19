//! An object store implementation for a local filesystem
use crate::path::Path;
use crate::{GetResult, ListResult, ObjectMeta, ObjectStore, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    stream::{self, BoxStream},
    StreamExt,
};
use snafu::{OptionExt, ResultExt, Snafu};
use std::{collections::BTreeSet, convert::TryFrom, io};
use tokio::fs;
use url::Url;
use walkdir::{DirEntry, WalkDir};

/// A specialized `Error` for filesystem object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub(crate) enum Error {
    #[snafu(display("File size for {} did not fit in a usize: {}", path, source))]
    FileSizeOverflowedUsize {
        source: std::num::TryFromIntError,
        path: String,
    },

    #[snafu(display("Unable to walk dir: {}", source))]
    UnableToWalkDir {
        source: walkdir::Error,
    },

    #[snafu(display("Unable to access metadata for {}: {}", path, source))]
    UnableToAccessMetadata {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
        path: String,
    },

    #[snafu(display("Unable to copy data to file: {}", source))]
    UnableToCopyDataToFile {
        source: io::Error,
    },

    #[snafu(display("Unable to create dir {}: {}", path.display(), source))]
    UnableToCreateDir {
        source: io::Error,
        path: std::path::PathBuf,
    },

    #[snafu(display("Unable to create file {}: {}", path.display(), err))]
    UnableToCreateFile {
        path: std::path::PathBuf,
        err: io::Error,
    },

    #[snafu(display("Unable to delete file {}: {}", path.display(), source))]
    UnableToDeleteFile {
        source: io::Error,
        path: std::path::PathBuf,
    },

    #[snafu(display("Unable to open file {}: {}", path.display(), source))]
    UnableToOpenFile {
        source: io::Error,
        path: std::path::PathBuf,
    },

    #[snafu(display("Unable to read data from file {}: {}", path.display(), source))]
    UnableToReadBytes {
        source: io::Error,
        path: std::path::PathBuf,
    },

    NotFound {
        path: String,
        source: io::Error,
    },

    #[snafu(display("Unable to canonicalize path {}: {}", path.display(), source))]
    UnableToCanonicalize {
        source: io::Error,
        path: std::path::PathBuf,
    },

    #[snafu(display("Unable to convert path \"{}\" to URL", path.display()))]
    InvalidPath {
        path: std::path::PathBuf,
    },

    #[snafu(display("Unable to convert URL \"{}\" to filesystem path", url))]
    InvalidUrl {
        url: Url,
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
                store: "LocalFileSystem",
                source: Box::new(source),
            },
        }
    }
}

/// Local filesystem storage providing an [`ObjectStore`] interface to files on
/// local disk. Can optionally be created with a directory prefix
///
/// # Path Semantics
///
/// This implementation follows the [file URI] scheme outlined in [RFC 3986]. In
/// particular paths are delimited by `/`
///
/// [file URI]: https://en.wikipedia.org/wiki/File_URI_scheme
/// [RFC 3986]: https://www.rfc-editor.org/rfc/rfc3986
#[derive(Debug)]
pub struct LocalFileSystem {
    root: Url,
}

impl std::fmt::Display for LocalFileSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LocalFileSystem({})", self.root)
    }
}

impl Default for LocalFileSystem {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalFileSystem {
    /// Create new filesystem storage with no prefix
    pub fn new() -> Self {
        Self {
            root: Url::parse("file:///").unwrap(),
        }
    }

    /// Create new filesystem storage with `prefix` applied to all paths
    pub fn new_with_prefix(prefix: impl AsRef<std::path::Path>) -> Result<Self> {
        Ok(Self {
            root: path_to_url(prefix, true)?,
        })
    }

    /// Return filesystem path of the given location
    fn path_to_filesystem(&self, location: &Path) -> Result<std::path::PathBuf> {
        // Workaround https://github.com/servo/rust-url/issues/769
        let mut url = self.root.clone();
        url.set_path(&format!("{}{}", url.path(), location.to_raw()));

        url.to_file_path()
            .map_err(|_| Error::InvalidUrl { url }.into())
    }

    fn filesystem_to_path(&self, location: &std::path::Path) -> Result<Path> {
        let url = path_to_url(location, false)?;
        let relative = self.root.make_relative(&url).expect("relative path");

        // TODO: This will double percent-encode
        Ok(Path::from_raw(relative))
    }

    async fn get_file(&self, location: &Path) -> Result<(fs::File, std::path::PathBuf)> {
        let path = self.path_to_filesystem(location)?;

        let file = fs::File::open(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Error::NotFound {
                    path: location.to_string(),
                    source: e,
                }
            } else {
                Error::UnableToOpenFile {
                    path: path.clone(),
                    source: e,
                }
            }
        })?;
        Ok((file, path))
    }
}

fn path_to_url(path: impl AsRef<std::path::Path>, is_dir: bool) -> Result<Url, Error> {
    // Convert to canonical, i.e. absolute representation
    let canonical = path
        .as_ref()
        .canonicalize()
        .context(UnableToCanonicalizeSnafu {
            path: path.as_ref(),
        })?;

    // Convert to file URL
    let result = match is_dir {
        true => Url::from_directory_path(&canonical),
        false => Url::from_file_path(&canonical),
    };

    result.map_err(|_| Error::InvalidPath { path: canonical })
}

#[async_trait]
impl ObjectStore for LocalFileSystem {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let content = bytes::BytesMut::from(&*bytes);

        let path = self.path_to_filesystem(location)?;

        let mut file = match fs::File::create(&path).await {
            Ok(f) => f,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let parent = path
                    .parent()
                    .context(UnableToCreateFileSnafu { path: &path, err })?;
                fs::create_dir_all(&parent)
                    .await
                    .context(UnableToCreateDirSnafu { path: parent })?;

                match fs::File::create(&path).await {
                    Ok(f) => f,
                    Err(err) => return Err(Error::UnableToCreateFile { path, err }.into()),
                }
            }
            Err(err) => return Err(Error::UnableToCreateFile { path, err }.into()),
        };

        tokio::io::copy(&mut &content[..], &mut file)
            .await
            .context(UnableToCopyDataToFileSnafu)?;

        Ok(())
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let (file, path) = self.get_file(location).await?;
        Ok(GetResult::File(file, path))
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let (file, _) = self.get_file(location).await?;
        let metadata = file
            .metadata()
            .await
            .map_err(|e| Error::UnableToAccessMetadata {
                source: e.into(),
                path: location.to_string(),
            })?;

        convert_metadata(metadata, location.clone())
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let path = self.path_to_filesystem(location)?;
        fs::remove_file(&path)
            .await
            .context(UnableToDeleteFileSnafu { path })?;
        Ok(())
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let root_path = match prefix {
            Some(prefix) => self.path_to_filesystem(prefix)?,
            None => self.root.to_file_path().unwrap(),
        };

        let walkdir = WalkDir::new(&root_path)
            // Don't include the root directory itself
            .min_depth(1);

        let s =
            walkdir.into_iter().flat_map(move |result_dir_entry| {
                match convert_walkdir_result(result_dir_entry) {
                    Err(e) => Some(Err(e)),
                    Ok(None) => None,
                    Ok(entry @ Some(_)) => entry
                        .filter(|dir_entry| dir_entry.file_type().is_file())
                        .map(|entry| {
                            let location = self.filesystem_to_path(entry.path())?;
                            convert_entry(entry, location)
                        }),
                }
            });

        Ok(stream::iter(s).boxed())
    }

    async fn list_with_delimiter(&self, prefix: &Path) -> Result<ListResult> {
        let resolved_prefix = self.path_to_filesystem(prefix)?;

        let walkdir = WalkDir::new(&resolved_prefix).min_depth(1).max_depth(1);

        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();

        for entry_res in walkdir.into_iter().map(convert_walkdir_result) {
            if let Some(entry) = entry_res? {
                let is_directory = entry.file_type().is_dir();
                let entry_location = self.filesystem_to_path(entry.path())?;

                let mut parts = match entry_location.prefix_match(prefix) {
                    Some(parts) => parts,
                    None => continue,
                };

                let common_prefix = match parts.next() {
                    Some(p) => p,
                    None => continue,
                };

                drop(parts);

                if is_directory {
                    common_prefixes.insert(prefix.child(common_prefix));
                } else {
                    objects.push(convert_entry(entry, entry_location)?);
                }
            }
        }

        Ok(ListResult {
            next_token: None,
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }
}

fn convert_entry(entry: DirEntry, location: Path) -> Result<ObjectMeta> {
    let metadata = entry
        .metadata()
        .map_err(|e| Error::UnableToAccessMetadata {
            source: e.into(),
            path: location.to_string(),
        })?;
    convert_metadata(metadata, location)
}

fn convert_metadata(metadata: std::fs::Metadata, location: Path) -> Result<ObjectMeta> {
    let last_modified = metadata
        .modified()
        .expect("Modified file time should be supported on this platform")
        .into();

    let size = usize::try_from(metadata.len()).context(FileSizeOverflowedUsizeSnafu {
        path: location.to_raw(),
    })?;

    Ok(ObjectMeta {
        location,
        last_modified,
        size,
    })
}

/// Convert walkdir results and converts not-found errors into `None`.
fn convert_walkdir_result(
    res: std::result::Result<walkdir::DirEntry, walkdir::Error>,
) -> Result<Option<walkdir::DirEntry>> {
    match res {
        Ok(entry) => Ok(Some(entry)),
        Err(walkdir_err) => match walkdir_err.io_error() {
            Some(io_err) => match io_err.kind() {
                io::ErrorKind::NotFound => Ok(None),
                _ => Err(Error::UnableToWalkDir {
                    source: walkdir_err,
                }
                .into()),
            },
            None => Err(Error::UnableToWalkDir {
                source: walkdir_err,
            }
            .into()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        tests::{
            get_nonexistent_object, list_uses_directories_correctly, list_with_delimiter,
            put_get_delete_list,
        },
        Error as ObjectStoreError, ObjectStore,
    };
    use tempfile::TempDir;

    #[tokio::test]
    async fn file_test() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        put_get_delete_list(&integration).await.unwrap();
        list_uses_directories_correctly(&integration).await.unwrap();
        list_with_delimiter(&integration).await.unwrap();
    }

    #[tokio::test]
    async fn creates_dir_if_not_present() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let location = Path::from_raw("nested/file/test_file");

        let data = Bytes::from("arbitrary data");
        let expected_data = data.clone();

        integration.put(&location, data).await.unwrap();

        let read_data = integration
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&*read_data, expected_data);
    }

    #[tokio::test]
    async fn unknown_length() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let location = Path::from_raw("some_file");

        let data = Bytes::from("arbitrary data");
        let expected_data = data.clone();

        integration.put(&location, data).await.unwrap();

        let read_data = integration
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&*read_data, expected_data);
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn bubble_up_io_errors() {
        use std::{fs::set_permissions, os::unix::prelude::PermissionsExt};

        let root = TempDir::new().unwrap();

        // make non-readable
        let metadata = root.path().metadata().unwrap();
        let mut permissions = metadata.permissions();
        permissions.set_mode(0o000);
        set_permissions(root.path(), permissions).unwrap();

        let store = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        // `list` must fail
        match store.list(None).await {
            Err(_) => {
                // ok, error found
            }
            Ok(mut stream) => {
                let mut any_err = false;
                while let Some(res) = stream.next().await {
                    if res.is_err() {
                        any_err = true;
                    }
                }
                assert!(any_err);
            }
        }

        // `list_with_delimiter
        assert!(store.list_with_delimiter(&Path::default()).await.is_err());
    }

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    #[tokio::test]
    async fn get_nonexistent_location() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let location = Path::from_raw(NON_EXISTENT_NAME);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err();
        if let Some(ObjectStoreError::NotFound { path, source }) =
            err.downcast_ref::<ObjectStoreError>()
        {
            let source_variant = source.downcast_ref::<std::io::Error>();
            assert!(
                matches!(source_variant, Some(std::io::Error { .. }),),
                "got: {:?}",
                source_variant
            );
            assert_eq!(path, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type: {:?}", err);
        }
    }

    #[tokio::test]
    async fn root() {
        let integration = LocalFileSystem::new();

        let canonical = std::path::Path::new("Cargo.toml").canonicalize().unwrap();
        let url = Url::from_directory_path(canonical).unwrap();
        let path = Path::from_raw(url.path());

        integration.head(&path).await.unwrap();
    }

    #[tokio::test]
    async fn test_list_root() {
        let integration = LocalFileSystem::new();
        let result = integration.list_with_delimiter(&Path::from_raw("/")).await;
        if cfg!(target_family = "windows") {
            let r = result.unwrap_err().to_string();
            assert!(
                r.contains("Unable to convert URL \"file:///\" to filesystem path"),
                "{}",
                r
            );
        } else {
            result.unwrap();
        }
    }
}
