//! Path abstraction for Object Storage

use itertools::Itertools;
use snafu::{ensure, ResultExt, Snafu};
use std::fmt::Formatter;

/// The delimiter to separate object namespaces, creating a directory structure.
pub const DELIMITER: &str = "/";

/// The path delimiter as a single byte
pub const DELIMITER_BYTE: u8 = DELIMITER.as_bytes()[0];

mod parts;

pub use parts::{InvalidPart, PathPart};

/// Error returned by [`Path::parse`]
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Path \"{}\" contained empty path segment", path))]
    EmptySegment { path: String },

    #[snafu(display("Error parsing Path \"{}\": \"{}\"", path, source))]
    BadSegment { path: String, source: InvalidPart },
}

/// A parsed path representation that can be safely written to object storage
///
/// # Path Safety
///
/// In theory object stores support any UTF-8 character sequence, however, certain character
/// sequences cause compatibility problems with some applications and protocols. As such the
/// naming guidelines for [S3], [GCS] and [Azure Blob Storage] all recommend sticking to a
/// limited character subset.
///
/// [S3]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
/// [GCS]: https://cloud.google.com/storage/docs/naming-objects
/// [Azure Blob Storage]: https://docs.microsoft.com/en-us/rest/api/storageservices/Naming-and-Referencing-Containers--Blobs--and-Metadata#blob-names
///
/// This presents libraries with two options for consistent path handling:
///
/// 1. Allow constructing unsafe paths, allowing for both reading and writing of data to paths
/// that may not be consistently understood or supported
/// 2. Disallow constructing unsafe paths, ensuring data written can be consistently handled by
/// all other systems, but preventing interaction with objects at unsafe paths
///
/// This library takes the second approach, in particular:
///
/// * Paths are delimited by `/`
/// * Paths do not start with a `/`
/// * Empty path segments are discarded (e.g. `//` is treated as though it were `/`)
/// * Relative path segments, i.e. `.` and `..` are percent encoded
/// * Unsafe characters are percent encoded, as described by [RFC 1738]
/// * All paths are relative to the root of the object store
///
/// In order to provide these guarantees there are two ways to safely construct a [`Path`]
///
/// # Encode
///
/// A string containing potentially illegal path segments can be encoded to a [`Path`]
/// using [`Path::from`] or [`Path::from_iter`].
///
/// ```
/// # use object_store::Path;
/// assert_eq!(Path::from("foo/bar").as_ref(), "foo/bar");
/// assert_eq!(Path::from("foo//bar").as_ref(), "foo/bar");
/// assert_eq!(Path::from("foo/../bar").as_ref(), "foo/%2E%2E/bar");
/// assert_eq!(Path::from_iter(["foo", "foo/bar"]).as_ref(), "foo/foo%2Fbar");
/// ```
///
/// Note: if provided with an already percent encoded string, this will encode it again
///
/// ```
/// # use object_store::Path;
/// assert_eq!(Path::from("foo/foo%2Fbar").as_ref(), "foo/foo%252Fbar");
/// ```
///
/// # Parse
///
/// Alternatively a [`Path`] can be created from an existing string, returning an
/// error if it is invalid. Unlike the encoding methods, this will permit
/// valid percent encoded sequences.
///
/// ```
/// # use object_store::Path;
///
/// assert_eq!(Path::parse("/foo/foo%2Fbar").unwrap().as_ref(), "foo/foo%2Fbar");
/// Path::parse("..").unwrap_err();
/// Path::parse("/foo//").unwrap_err();
/// Path::parse("😀").unwrap_err();
/// Path::parse("%Q").unwrap_err();
/// ```
///
/// [RFC 1738]: https://www.ietf.org/rfc/rfc1738.txt
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct Path {
    /// The raw path with no leading or trailing delimiters
    raw: String,
}

impl Path {
    /// Parse a string as a [`Path`], returning a [`Error`] if invalid,
    /// as defined on the docstring for [`Path`]
    ///
    /// Note: this will strip any leading `/` or trailing `/`
    pub fn parse(path: impl AsRef<str>) -> Result<Self, Error> {
        let path = path.as_ref();

        let stripped = path.strip_prefix(DELIMITER).unwrap_or(path);
        let stripped = stripped.strip_suffix(DELIMITER).unwrap_or(stripped);

        for segment in stripped.split(DELIMITER) {
            ensure!(!segment.is_empty(), EmptySegmentSnafu { path });
            PathPart::parse(segment).context(BadSegmentSnafu { path })?;
        }

        Ok(Self {
            raw: stripped.to_string(),
        })
    }

    /// Returns the [`PathPart`] of this [`Path`]
    pub fn parts(&self) -> impl Iterator<Item = PathPart<'_>> {
        match self.raw.is_empty() {
            true => itertools::Either::Left(std::iter::empty()),
            false => itertools::Either::Right(
                self.raw
                    .split(DELIMITER)
                    .map(|s| PathPart { raw: s.into() }),
            ),
        }
    }

    pub(crate) fn prefix_match(
        &self,
        prefix: &Self,
    ) -> Option<impl Iterator<Item = PathPart<'_>> + '_> {
        let diff = itertools::diff_with(
            self.raw.split(DELIMITER),
            prefix.raw.split(DELIMITER),
            |a, b| a == b,
        );

        match diff {
            // Both were equal
            None => Some(itertools::Either::Left(std::iter::empty())),
            // Mismatch or prefix was longer => None
            Some(itertools::Diff::FirstMismatch(_, _, _) | itertools::Diff::Longer(_, _)) => None,
            // Match with remaining
            Some(itertools::Diff::Shorter(_, back)) => Some(itertools::Either::Right(
                back.map(|s| PathPart { raw: s.into() }),
            )),
        }
    }

    pub(crate) fn prefix_matches(&self, prefix: &Self) -> bool {
        self.prefix_match(prefix).is_some()
    }

    /// Creates a new child of this [`Path`]
    pub fn child<'a>(&self, child: impl Into<PathPart<'a>>) -> Self {
        let raw = match self.raw.is_empty() {
            true => format!("{}", child.into().raw),
            false => format!("{}{}{}", self.raw, DELIMITER, child.into().raw),
        };

        Self { raw }
    }
}

impl AsRef<str> for Path {
    fn as_ref(&self) -> &str {
        &self.raw
    }
}

impl From<&str> for Path {
    fn from(path: &str) -> Self {
        Self::from_iter(path.split(DELIMITER))
    }
}

impl From<String> for Path {
    fn from(path: String) -> Self {
        Self::from_iter(path.split(DELIMITER))
    }
}

impl From<Path> for String {
    fn from(path: Path) -> Self {
        path.raw
    }
}

impl std::fmt::Display for Path {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.raw.fmt(f)
    }
}

impl<'a, I> FromIterator<I> for Path
where
    I: Into<PathPart<'a>>,
{
    fn from_iter<T: IntoIterator<Item = I>>(iter: T) -> Self {
        let raw = T::into_iter(iter)
            .map(|s| s.into())
            .filter(|s| !s.raw.is_empty())
            .map(|s| s.raw)
            .join(DELIMITER);

        Self { raw }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloud_prefix_with_trailing_delimiter() {
        // Use case: files exist in object storage named `foo/bar.json` and
        // `foo_test.json`. A search for the prefix `foo/` should return
        // `foo/bar.json` but not `foo_test.json'.
        let prefix = Path::from_iter(["test"]);
        assert_eq!(prefix.as_ref(), "test");
    }

    #[test]
    fn push_encodes() {
        let location = Path::from_iter(["foo/bar", "baz%2Ftest"]);
        assert_eq!(location.as_ref(), "foo%2Fbar/baz%252Ftest");
    }

    #[test]
    fn convert_raw_before_partial_eq() {
        // dir and file_name
        let cloud = Path::from("test_dir/test_file.json");
        let built = Path::from_iter(["test_dir", "test_file.json"]);

        assert_eq!(built, cloud);

        // dir and file_name w/o dot
        let cloud = Path::from("test_dir/test_file");
        let built = Path::from_iter(["test_dir", "test_file"]);

        assert_eq!(built, cloud);

        // dir, no file
        let cloud = Path::from("test_dir/");
        let built = Path::from_iter(["test_dir"]);
        assert_eq!(built, cloud);

        // file_name, no dir
        let cloud = Path::from("test_file.json");
        let built = Path::from_iter(["test_file.json"]);
        assert_eq!(built, cloud);

        // empty
        let cloud = Path::from("");
        let built = Path::from_iter(["", ""]);

        assert_eq!(built, cloud);
    }

    #[test]
    fn parts_after_prefix_behavior() {
        let existing_path = Path::from("apple/bear/cow/dog/egg.json");

        // Prefix with one directory
        let prefix = Path::from("apple");
        let expected_parts: Vec<PathPart<'_>> = vec!["bear", "cow", "dog", "egg.json"]
            .into_iter()
            .map(Into::into)
            .collect();
        let parts: Vec<_> = existing_path.prefix_match(&prefix).unwrap().collect();
        assert_eq!(parts, expected_parts);

        // Prefix with two directories
        let prefix = Path::from("apple/bear");
        let expected_parts: Vec<PathPart<'_>> = vec!["cow", "dog", "egg.json"]
            .into_iter()
            .map(Into::into)
            .collect();
        let parts: Vec<_> = existing_path.prefix_match(&prefix).unwrap().collect();
        assert_eq!(parts, expected_parts);

        // Not a prefix
        let prefix = Path::from("cow");
        assert!(existing_path.prefix_match(&prefix).is_none());

        // Prefix with a partial directory
        let prefix = Path::from("ap");
        assert!(existing_path.prefix_match(&prefix).is_none());

        // Prefix matches but there aren't any parts after it
        let existing_path = Path::from("apple/bear/cow/dog");

        let prefix = existing_path.clone();
        assert_eq!(existing_path.prefix_match(&prefix).unwrap().count(), 0);
    }

    #[test]
    fn prefix_matches() {
        let haystack = Path::from_iter(["foo/bar", "baz%2Ftest", "something"]);
        let needle = haystack.clone();
        // self starts with self
        assert!(
            haystack.prefix_matches(&haystack),
            "{:?} should have started with {:?}",
            haystack,
            haystack
        );

        // a longer prefix doesn't match
        let needle = needle.child("longer now");
        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} shouldn't have started with {:?}",
            haystack,
            needle
        );

        // one dir prefix matches
        let needle = Path::from_iter(["foo/bar"]);
        assert!(
            haystack.prefix_matches(&needle),
            "{:?} should have started with {:?}",
            haystack,
            needle
        );

        // two dir prefix matches
        let needle = needle.child("baz%2Ftest");
        assert!(
            haystack.prefix_matches(&needle),
            "{:?} should have started with {:?}",
            haystack,
            needle
        );

        // partial dir prefix doesn't match
        let needle = Path::from_iter(["f"]);
        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} should not have started with {:?}",
            haystack,
            needle
        );

        // one dir and one partial dir doesn't match
        let needle = Path::from_iter(["foo/bar", "baz"]);
        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} should not have started with {:?}",
            haystack,
            needle
        );
    }

    #[test]
    fn prefix_matches_with_file_name() {
        let haystack = Path::from_iter(["foo/bar", "baz%2Ftest", "something", "foo.segment"]);

        // All directories match and file name is a prefix
        let needle = Path::from_iter(["foo/bar", "baz%2Ftest", "something", "foo"]);

        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} should not have started with {:?}",
            haystack,
            needle
        );

        // All directories match but file name is not a prefix
        let needle = Path::from_iter(["foo/bar", "baz%2Ftest", "something", "e"]);

        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} should not have started with {:?}",
            haystack,
            needle
        );

        // Not all directories match; file name is a prefix of the next directory; this
        // does not match
        let needle = Path::from_iter(["foo/bar", "baz%2Ftest", "s"]);

        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} should not have started with {:?}",
            haystack,
            needle
        );

        // Not all directories match; file name is NOT a prefix of the next directory;
        // no match
        let needle = Path::from_iter(["foo/bar", "baz%2Ftest", "p"]);

        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} should not have started with {:?}",
            haystack,
            needle
        );
    }
}
