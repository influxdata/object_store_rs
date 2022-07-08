//! A throttling object store wrapper
use parking_lot::Mutex;
use std::io;
use std::ops::Range;
use std::pin::Pin;
use std::task::Poll;
use std::{convert::TryInto, sync::Arc};

use crate::MultipartId;
use crate::{path::Path, GetResult, ListResult, ObjectMeta, ObjectStore, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::Future;
use futures::{stream::BoxStream, StreamExt};
use std::time::Duration;
use tokio::io::AsyncWrite;

/// Configuration settings for throttled store
#[derive(Debug, Default, Clone, Copy)]
pub struct ThrottleConfig {
    /// Sleep duration for every call to [`delete`](ThrottledStore::delete).
    ///
    /// Sleeping is done before the underlying store is called and independently of the success of
    /// the operation.
    pub wait_delete_per_call: Duration,

    /// Sleep duration for every byte received during [`get`](ThrottledStore::get).
    ///
    /// Sleeping is performed after the underlying store returned and only for successful gets. The
    /// sleep duration is additive to [`wait_get_per_call`](Self::wait_get_per_call).
    ///
    /// Note that the per-byte sleep only happens as the user consumes the output bytes. Should
    /// there be an intermediate failure (i.e. after partly consuming the output bytes), the
    /// resulting sleep time will be partial as well.
    pub wait_get_per_byte: Duration,

    /// Sleep duration for every call to [`get`](ThrottledStore::get).
    ///
    /// Sleeping is done before the underlying store is called and independently of the success of
    /// the operation. The sleep duration is additive to
    /// [`wait_get_per_byte`](Self::wait_get_per_byte).
    pub wait_get_per_call: Duration,

    /// Sleep duration for every call to [`list`](ThrottledStore::list).
    ///
    /// Sleeping is done before the underlying store is called and independently of the success of
    /// the operation. The sleep duration is additive to
    /// [`wait_list_per_entry`](Self::wait_list_per_entry).
    pub wait_list_per_call: Duration,

    /// Sleep duration for every entry received during [`list`](ThrottledStore::list).
    ///
    /// Sleeping is performed after the underlying store returned and only for successful lists.
    /// The sleep duration is additive to [`wait_list_per_call`](Self::wait_list_per_call).
    ///
    /// Note that the per-entry sleep only happens as the user consumes the output entries. Should
    /// there be an intermediate failure (i.e. after partly consuming the output entries), the
    /// resulting sleep time will be partial as well.
    pub wait_list_per_entry: Duration,

    /// Sleep duration for every call to
    /// [`list_with_delimiter`](ThrottledStore::list_with_delimiter).
    ///
    /// Sleeping is done before the underlying store is called and independently of the success of
    /// the operation. The sleep duration is additive to
    /// [`wait_list_with_delimiter_per_entry`](Self::wait_list_with_delimiter_per_entry).
    pub wait_list_with_delimiter_per_call: Duration,

    /// Sleep duration for every entry received during
    /// [`list_with_delimiter`](ThrottledStore::list_with_delimiter).
    ///
    /// Sleeping is performed after the underlying store returned and only for successful gets. The
    /// sleep duration is additive to
    /// [`wait_list_with_delimiter_per_call`](Self::wait_list_with_delimiter_per_call).
    pub wait_list_with_delimiter_per_entry: Duration,

    /// Sleep duration for every call to [`put`](ThrottledStore::put).
    ///
    /// Sleeping is done before the underlying store is called and independently of the success of
    /// the operation.
    pub wait_put_per_call: Duration,
}

/// Sleep only if non-zero duration
async fn sleep(duration: Duration) {
    if !duration.is_zero() {
        tokio::time::sleep(duration).await
    }
}

/// Store wrapper that wraps an inner store with some `sleep` calls.
///
/// This can be used for performance testing.
///
/// **Note that the behavior of the wrapper is deterministic and might not reflect real-world
/// conditions!**
#[derive(Debug)]
pub struct ThrottledStore<T: ObjectStore> {
    inner: T,
    config: Arc<Mutex<ThrottleConfig>>,
}

impl<T: ObjectStore> ThrottledStore<T> {
    /// Create new wrapper with zero waiting times.
    pub fn new(inner: T, config: ThrottleConfig) -> Self {
        Self {
            inner,
            config: Arc::new(Mutex::new(config)),
        }
    }

    /// Mutate config.
    pub fn config_mut<F>(&self, f: F)
    where
        F: Fn(&mut ThrottleConfig),
    {
        let mut guard = self.config.lock();
        f(&mut guard)
    }

    /// Return copy of current config.
    pub fn config(&self) -> ThrottleConfig {
        *self.config.lock()
    }
}

impl<T: ObjectStore> std::fmt::Display for ThrottledStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ThrottledStore({})", self.inner)
    }
}

#[async_trait]
impl<T: ObjectStore> ObjectStore for ThrottledStore<T> {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        sleep(self.config().wait_put_per_call).await;

        self.inner.put(location, bytes).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        let (upload_id, inner) = self.inner.put_multipart(location).await?;
        Ok((
            upload_id,
            Box::new(ThrottledUpload {
                inner,
                config: Arc::clone(&self.config),
                state: ThrottledUploadState::Idle,
            }),
        ))
    }

    async fn cleanup_multipart(&self, location: &Path, upload_id: &MultipartId) -> Result<()> {
        self.inner.cleanup_multipart(location, upload_id).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        sleep(self.config().wait_get_per_call).await;

        // need to copy to avoid moving / referencing `self`
        let wait_get_per_byte = self.config().wait_get_per_byte;

        self.inner.get(location).await.map(|result| {
            let s = match result {
                GetResult::Stream(s) => s,
                GetResult::File(_, _) => unimplemented!(),
            };

            GetResult::Stream(
                s.then(move |bytes_result| async move {
                    match bytes_result {
                        Ok(bytes) => {
                            let bytes_len: u32 = usize_to_u32_saturate(bytes.len());
                            sleep(wait_get_per_byte * bytes_len).await;
                            Ok(bytes)
                        }
                        Err(err) => Err(err),
                    }
                })
                .boxed(),
            )
        })
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let config = self.config();

        let sleep_duration = config.wait_delete_per_call
            + config.wait_get_per_byte * (range.end - range.start) as u32;

        sleep(sleep_duration).await;

        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        sleep(self.config().wait_put_per_call).await;
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        sleep(self.config().wait_delete_per_call).await;

        self.inner.delete(location).await
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        sleep(self.config().wait_list_per_call).await;

        // need to copy to avoid moving / referencing `self`
        let wait_list_per_entry = self.config().wait_list_per_entry;

        self.inner.list(prefix).await.map(|stream| {
            stream
                .then(move |result| async move {
                    match result {
                        Ok(entry) => {
                            sleep(wait_list_per_entry).await;
                            Ok(entry)
                        }
                        Err(err) => Err(err),
                    }
                })
                .boxed()
        })
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        sleep(self.config().wait_list_with_delimiter_per_call).await;

        match self.inner.list_with_delimiter(prefix).await {
            Ok(list_result) => {
                let entries_len = usize_to_u32_saturate(list_result.objects.len());
                sleep(self.config().wait_list_with_delimiter_per_entry * entries_len).await;
                Ok(list_result)
            }
            Err(err) => Err(err),
        }
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        sleep(self.config().wait_put_per_call).await;

        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        sleep(self.config().wait_put_per_call).await;

        self.inner.copy_if_not_exists(from, to).await
    }
}

/// Saturated `usize` to `u32` cast.
fn usize_to_u32_saturate(x: usize) -> u32 {
    x.try_into().unwrap_or(u32::MAX)
}

enum ThrottledUploadState {
    Idle,
    Sleeping(Pin<Box<dyn Future<Output = ()> + Send>>),
    Waiting,
}

struct ThrottledUpload {
    inner: Box<dyn AsyncWrite + Unpin + Send>,
    config: Arc<Mutex<ThrottleConfig>>,
    state: ThrottledUploadState,
}

impl ThrottledUpload {
    fn config(&self) -> ThrottleConfig {
        *self.config.lock()
    }

    fn poll_waiting<F>(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        inner_call: F,
    ) -> Poll<Result<(), io::Error>>
    where
        F: Fn(
            &mut Box<dyn AsyncWrite + Unpin + Send>,
            &mut std::task::Context<'_>,
        ) -> Poll<Result<(), io::Error>>,
    {
        loop {
            match &mut self.state {
                ThrottledUploadState::Idle => {
                    // If idle, begin sleeping
                    let wait = self.config().wait_put_per_call;
                    self.state = ThrottledUploadState::Sleeping(Box::pin(sleep(wait)))
                }
                ThrottledUploadState::Sleeping(fut) => {
                    // If sleep is done, move to waiting for inner
                    match Pin::new(fut).poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(_) => {
                            self.state = ThrottledUploadState::Waiting;
                        }
                    }
                }
                ThrottledUploadState::Waiting => {
                    return inner_call(&mut self.inner, cx);
                }
            }
        }
    }
}

impl AsyncWrite for ThrottledUpload {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, io::Error>> {
        self.poll_waiting(cx, |inner, cx| {
            Pin::new(inner).poll_write(cx, buf).map_ok(|_| ())
        })
        .map_ok(|_| buf.len())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        self.poll_waiting(cx, |inner, cx| Pin::new(inner).poll_flush(cx))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        self.poll_waiting(cx, |inner, cx| Pin::new(inner).poll_shutdown(cx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        memory::InMemory,
        tests::{
            copy_if_not_exists, list_uses_directories_correctly, list_with_delimiter,
            put_get_delete_list, rename_and_copy, stream_get,
        },
    };
    use bytes::Bytes;
    use futures::TryStreamExt;
    use tokio::time::Duration;
    use tokio::time::Instant;

    const WAIT_TIME: Duration = Duration::from_millis(100);
    const ZERO: Duration = Duration::from_millis(0); // Duration::default isn't constant

    macro_rules! assert_bounds {
        ($d:expr, $lower:expr) => {
            assert_bounds!($d, $lower, $lower + 1);
        };
        ($d:expr, $lower:expr, $upper:expr) => {
            let d = $d;
            let lower = $lower * WAIT_TIME;
            let upper = $upper * WAIT_TIME;
            assert!(d >= lower, "{:?} must be >= than {:?}", d, lower);
            assert!(d < upper, "{:?} must be < than {:?}", d, upper);
        };
    }

    #[tokio::test]
    async fn throttle_test() {
        let inner = InMemory::new();
        let store = ThrottledStore::new(inner, ThrottleConfig::default());

        put_get_delete_list(&store).await.unwrap();
        list_uses_directories_correctly(&store).await.unwrap();
        list_with_delimiter(&store).await.unwrap();
        rename_and_copy(&store).await.unwrap();
        copy_if_not_exists(&store).await.unwrap();
        stream_get(&store).await.unwrap();
    }

    #[tokio::test]
    async fn delete_test() {
        let inner = InMemory::new();
        let store = ThrottledStore::new(inner, ThrottleConfig::default());

        assert_bounds!(measure_delete(&store, None).await, 0);
        assert_bounds!(measure_delete(&store, Some(0)).await, 0);
        assert_bounds!(measure_delete(&store, Some(10)).await, 0);

        store.config_mut(|cfg| cfg.wait_delete_per_call = WAIT_TIME);
        assert_bounds!(measure_delete(&store, None).await, 1);
        assert_bounds!(measure_delete(&store, Some(0)).await, 1);
        assert_bounds!(measure_delete(&store, Some(10)).await, 1);
    }

    #[tokio::test]
    async fn get_test() {
        let inner = InMemory::new();
        let store = ThrottledStore::new(inner, ThrottleConfig::default());

        assert_bounds!(measure_get(&store, None).await, 0);
        assert_bounds!(measure_get(&store, Some(0)).await, 0);
        assert_bounds!(measure_get(&store, Some(10)).await, 0);

        store.config_mut(|cfg| cfg.wait_get_per_call = WAIT_TIME);
        assert_bounds!(measure_get(&store, None).await, 1);
        assert_bounds!(measure_get(&store, Some(0)).await, 1);
        assert_bounds!(measure_get(&store, Some(10)).await, 1);

        store.config_mut(|cfg| {
            cfg.wait_get_per_call = ZERO;
            cfg.wait_get_per_byte = WAIT_TIME;
        });
        assert_bounds!(measure_get(&store, Some(2)).await, 2);

        store.config_mut(|cfg| {
            cfg.wait_get_per_call = WAIT_TIME;
            cfg.wait_get_per_byte = WAIT_TIME;
        });
        assert_bounds!(measure_get(&store, Some(2)).await, 3);
    }

    #[tokio::test]
    async fn list_test() {
        let inner = InMemory::new();
        let store = ThrottledStore::new(inner, ThrottleConfig::default());

        assert_bounds!(measure_list(&store, 0).await, 0);
        assert_bounds!(measure_list(&store, 10).await, 0);

        store.config_mut(|cfg| cfg.wait_list_per_call = WAIT_TIME);
        assert_bounds!(measure_list(&store, 0).await, 1);
        assert_bounds!(measure_list(&store, 10).await, 1);

        store.config_mut(|cfg| {
            cfg.wait_list_per_call = ZERO;
            cfg.wait_list_per_entry = WAIT_TIME;
        });
        assert_bounds!(measure_list(&store, 2).await, 2);

        store.config_mut(|cfg| {
            cfg.wait_list_per_call = WAIT_TIME;
            cfg.wait_list_per_entry = WAIT_TIME;
        });
        assert_bounds!(measure_list(&store, 2).await, 3);
    }

    #[tokio::test]
    async fn list_with_delimiter_test() {
        let inner = InMemory::new();
        let store = ThrottledStore::new(inner, ThrottleConfig::default());

        assert_bounds!(measure_list_with_delimiter(&store, 0).await, 0);
        assert_bounds!(measure_list_with_delimiter(&store, 10).await, 0);

        store.config_mut(|cfg| cfg.wait_list_with_delimiter_per_call = WAIT_TIME);
        assert_bounds!(measure_list_with_delimiter(&store, 0).await, 1);
        assert_bounds!(measure_list_with_delimiter(&store, 10).await, 1);

        store.config_mut(|cfg| {
            cfg.wait_list_with_delimiter_per_call = ZERO;
            cfg.wait_list_with_delimiter_per_entry = WAIT_TIME;
        });
        assert_bounds!(measure_list_with_delimiter(&store, 2).await, 2);

        store.config_mut(|cfg| {
            cfg.wait_list_with_delimiter_per_call = WAIT_TIME;
            cfg.wait_list_with_delimiter_per_entry = WAIT_TIME;
        });
        assert_bounds!(measure_list_with_delimiter(&store, 2).await, 3);
    }

    #[tokio::test]
    async fn put_test() {
        let inner = InMemory::new();
        let store = ThrottledStore::new(inner, ThrottleConfig::default());

        assert_bounds!(measure_put(&store, 0).await, 0);
        assert_bounds!(measure_put(&store, 10).await, 0);

        store.config_mut(|cfg| cfg.wait_put_per_call = WAIT_TIME);
        assert_bounds!(measure_put(&store, 0).await, 1);
        assert_bounds!(measure_put(&store, 10).await, 1);

        store.config_mut(|cfg| cfg.wait_put_per_call = ZERO);
        assert_bounds!(measure_put(&store, 0).await, 0);
    }

    async fn place_test_object(store: &ThrottledStore<InMemory>, n_bytes: Option<usize>) -> Path {
        let path = Path::from("foo");

        if let Some(n_bytes) = n_bytes {
            let data: Vec<_> = std::iter::repeat(1u8).take(n_bytes).collect();
            let bytes = Bytes::from(data);
            store.put(&path, bytes).await.unwrap();
        } else {
            // ensure object is absent
            store.delete(&path).await.unwrap();
        }

        path
    }

    async fn place_test_objects(store: &ThrottledStore<InMemory>, n_entries: usize) -> Path {
        let prefix = Path::from("foo");

        // clean up store
        let entries: Vec<_> = store
            .list(Some(&prefix))
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        for entry in entries {
            store.delete(&entry.location).await.unwrap();
        }

        // create new entries
        for i in 0..n_entries {
            let path = prefix.child(i.to_string().as_str());

            let data = Bytes::from("bar");
            store.put(&path, data).await.unwrap();
        }

        prefix
    }

    async fn measure_delete(store: &ThrottledStore<InMemory>, n_bytes: Option<usize>) -> Duration {
        let path = place_test_object(store, n_bytes).await;

        let t0 = Instant::now();
        store.delete(&path).await.unwrap();

        t0.elapsed()
    }

    async fn measure_get(store: &ThrottledStore<InMemory>, n_bytes: Option<usize>) -> Duration {
        let path = place_test_object(store, n_bytes).await;

        let t0 = Instant::now();
        let res = store.get(&path).await;
        if n_bytes.is_some() {
            // need to consume bytes to provoke sleep times
            let s = match res.unwrap() {
                GetResult::Stream(s) => s,
                GetResult::File(_, _) => unimplemented!(),
            };

            s.map_ok(|b| bytes::BytesMut::from(&b[..]))
                .try_concat()
                .await
                .unwrap();
        } else {
            assert!(res.is_err());
        }

        t0.elapsed()
    }

    async fn measure_list(store: &ThrottledStore<InMemory>, n_entries: usize) -> Duration {
        let prefix = place_test_objects(store, n_entries).await;

        let t0 = Instant::now();
        store
            .list(Some(&prefix))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        t0.elapsed()
    }

    async fn measure_list_with_delimiter(
        store: &ThrottledStore<InMemory>,
        n_entries: usize,
    ) -> Duration {
        let prefix = place_test_objects(store, n_entries).await;

        let t0 = Instant::now();
        store.list_with_delimiter(Some(&prefix)).await.unwrap();

        t0.elapsed()
    }

    async fn measure_put(store: &ThrottledStore<InMemory>, n_bytes: usize) -> Duration {
        let data: Vec<_> = std::iter::repeat(1u8).take(n_bytes).collect();
        let bytes = Bytes::from(data);

        let t0 = Instant::now();
        store.put(&Path::from("foo"), bytes).await.unwrap();

        t0.elapsed()
    }
}
