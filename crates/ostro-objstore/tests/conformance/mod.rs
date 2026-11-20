//! Backend-agnostic conformance suite (08-03). Every check takes `Arc<dyn ObjectStore>` (so
//! object safety is exercised on every call) and a namespace `ns` that prefixes every key, so
//! a shared bucket (CI SeaweedFS) can be reused across runs.

#![allow(dead_code)] // each test binary uses a subset

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::stream::BoxStream;
use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use ostro_objstore::{Conditional, Error, ObjectMeta, ObjectStore, PutOutcome, Result};

/// Object-safety compile checks (B1).
fn _assert_object_safe(_: Arc<dyn ObjectStore>) {}

/// Run every check that all backends must pass. `multipart_len` is the size of the
/// `put_multipart` round trip (above the backend's multipart threshold where it has one).
pub async fn run_all(store: Arc<dyn ObjectStore>, ns: &str, multipart_len: usize) {
    put_get_head(store.clone(), ns).await;
    ranges(store.clone(), ns).await;
    delete_idempotent(store.clone(), ns).await;
    conditional_puts(store.clone(), ns).await;
    multipart_dyn(store.clone(), ns, multipart_len).await;
    multipart_default_impl(store.clone(), ns).await;
    list_paginates_in_key_order(store.clone(), ns).await;
    key_grammar(store.clone()).await;
}

pub async fn put_get_head(store: Arc<dyn ObjectStore>, ns: &str) {
    let key = format!("{ns}/basic/a");
    let put = store
        .put(&key, Bytes::from_static(b"hello world"))
        .await
        .expect("put");
    let (body, meta) = store.get(&key).await.expect("get").expect("present");
    assert_eq!(&body[..], b"hello world");
    assert_eq!(meta.key, key);
    assert_eq!(meta.size, 11);
    assert!(meta.e_tag.is_some(), "get reports an etag");
    if put.e_tag.is_some() {
        assert_eq!(put.e_tag, meta.e_tag, "put and get etags agree");
    }
    let head = store.head(&key).await.expect("head").expect("present");
    assert_eq!(head.size, 11);
    assert_eq!(head.e_tag, meta.e_tag);
    assert!(head.last_modified > jiff::Timestamp::UNIX_EPOCH);

    // Overwrite replaces.
    store
        .put(&key, Bytes::from_static(b"second"))
        .await
        .expect("overwrite");
    let (body, _) = store.get(&key).await.unwrap().unwrap();
    assert_eq!(&body[..], b"second");

    let missing = format!("{ns}/basic/missing");
    assert!(store.head(&missing).await.expect("head missing").is_none());
    assert!(store.get(&missing).await.expect("get missing").is_none());
}

pub async fn ranges(store: Arc<dyn ObjectStore>, ns: &str) {
    let key = format!("{ns}/range/r");
    let data: Vec<u8> = (0u8..100).collect();
    store.put(&key, Bytes::from(data.clone())).await.unwrap();
    assert_eq!(
        &store.get_range(&key, 10..20).await.unwrap()[..],
        &data[10..20]
    );
    assert_eq!(&store.get_range(&key, 0..1).await.unwrap()[..], &data[0..1]);
    // Open-ended: end clamps to the size.
    assert_eq!(
        &store.get_range(&key, 90..u64::MAX).await.unwrap()[..],
        &data[90..]
    );
    assert_eq!(
        &store.get_range(&key, 95..1000).await.unwrap()[..],
        &data[95..]
    );
    assert!(store.get_range(&key, 50..50).await.unwrap().is_empty());
    assert!(matches!(
        store.get_range(&key, 100..110).await,
        Err(Error::InvalidRange { .. })
    ));
    assert!(matches!(
        store
            .get_range(&key, std::ops::Range { start: 20, end: 10 })
            .await,
        Err(Error::InvalidRange { .. })
    ));
    let missing = format!("{ns}/range/missing");
    assert!(matches!(
        store.get_range(&missing, 0..10).await,
        Err(Error::NotFound { .. })
    ));
}

pub async fn delete_idempotent(store: Arc<dyn ObjectStore>, ns: &str) {
    let key = format!("{ns}/del/x");
    store.put(&key, Bytes::from_static(b"x")).await.unwrap();
    store.delete(&key).await.expect("delete");
    assert!(store.head(&key).await.unwrap().is_none());
    store.delete(&key).await.expect("second delete is a no-op");
    store
        .delete(&format!("{ns}/del/never"))
        .await
        .expect("deleting a never-written key succeeds");

    let keys: Vec<String> = (0..5).map(|i| format!("{ns}/del/m{i}")).collect();
    for k in &keys {
        store.put(k, Bytes::from_static(b"m")).await.unwrap();
    }
    let mut with_missing = keys.clone();
    with_missing.push(format!("{ns}/del/never2"));
    store.delete_many(&with_missing).await.expect("delete_many");
    for k in &keys {
        assert!(store.head(k).await.unwrap().is_none(), "{k} survived");
    }
    store.delete_many(&keys).await.expect("delete_many again");
    store
        .delete_many(&[])
        .await
        .expect("delete_many of nothing");
}

pub async fn conditional_puts(store: Arc<dyn ObjectStore>, ns: &str) {
    let key = format!("{ns}/cond/LOCK");
    let first = store
        .put_if_absent(&key, Bytes::from_static(b"one"))
        .await
        .expect("put_if_absent");
    let Conditional::Ok(PutOutcome { e_tag: put_tag }) = first else {
        panic!("first put_if_absent must succeed");
    };
    assert_eq!(
        store
            .put_if_absent(&key, Bytes::from_static(b"two"))
            .await
            .unwrap(),
        Conditional::PreconditionFailed
    );
    let (body, meta) = store.get(&key).await.unwrap().unwrap();
    assert_eq!(&body[..], b"one", "the losing put_if_absent wrote nothing");
    let tag1 = meta.e_tag.expect("etag");
    if let Some(t) = put_tag {
        assert_eq!(t, tag1);
    }

    // Stale / bogus etag -> 412, nothing written.
    assert_eq!(
        store
            .put_if_match(
                &key,
                Bytes::from_static(b"x"),
                "\"0123456789abcdef0123456789abcdef\""
            )
            .await
            .unwrap(),
        Conditional::PreconditionFailed
    );
    assert_eq!(&store.get(&key).await.unwrap().unwrap().0[..], b"one");

    // Current etag -> replaced, new etag.
    let ok = store
        .put_if_match(&key, Bytes::from_static(b"three"), &tag1)
        .await
        .unwrap();
    let Conditional::Ok(_) = ok else {
        panic!("put_if_match with the current etag must succeed");
    };
    let (body, meta) = store.get(&key).await.unwrap().unwrap();
    assert_eq!(&body[..], b"three");
    let tag2 = meta.e_tag.expect("etag");
    assert_ne!(tag1, tag2);
    // The old etag is now stale.
    assert!(
        store
            .put_if_match(&key, Bytes::from_static(b"four"), &tag1)
            .await
            .unwrap()
            .is_precondition_failed()
    );
    // A missing object never matches.
    assert!(
        store
            .put_if_match(&format!("{ns}/cond/none"), Bytes::from_static(b"x"), &tag2)
            .await
            .unwrap()
            .is_precondition_failed()
    );
    store.delete(&key).await.unwrap();
}

/// N concurrent `put_if_absent` on one key: exactly one wins. Not run against s3s-fs (its
/// existence check is not atomic; it is a test server).
pub async fn put_if_absent_race(store: Arc<dyn ObjectStore>, ns: &str) {
    let key = format!("{ns}/race/LOCK");
    let tasks: Vec<_> = (0..16u8)
        .map(|i| {
            let store = store.clone();
            let key = key.clone();
            tokio::spawn(async move { store.put_if_absent(&key, Bytes::from(vec![i; 8])).await })
        })
        .collect();
    let mut winners = 0;
    for t in tasks {
        if let Conditional::Ok(_) = t.await.unwrap().expect("put_if_absent") {
            winners += 1;
        }
    }
    assert_eq!(winners, 1, "exactly one concurrent put_if_absent wins");
    store.delete(&key).await.unwrap();
}

fn pattern(len: usize) -> impl Iterator<Item = Bytes> {
    const CHUNK: usize = 1 << 20;
    (0..len.div_ceil(CHUNK)).map(move |i| {
        let n = CHUNK.min(len - i * CHUNK);
        Bytes::from(
            (0..n)
                .map(|j| ((i * 31 + j * 7) % 251) as u8)
                .collect::<Vec<_>>(),
        )
    })
}

fn pattern_stream(len: usize) -> BoxStream<'static, Result<Bytes>> {
    stream::iter(pattern(len).map(Ok)).boxed()
}

async fn assert_pattern(store: &Arc<dyn ObjectStore>, key: &str, len: usize) {
    let (body, meta) = store
        .get(key)
        .await
        .unwrap()
        .expect("multipart object present");
    assert_eq!(meta.size as usize, len);
    assert_eq!(body.len(), len);
    let want: Vec<u8> = pattern(len).flat_map(|b| b.to_vec()).collect();
    assert!(body[..] == want[..], "multipart body differs");
}

pub async fn multipart_dyn(store: Arc<dyn ObjectStore>, ns: &str, len: usize) {
    let key = format!("{ns}/mp/big");
    let out = store
        .put_multipart(&key, pattern_stream(len))
        .await
        .expect("put_multipart");
    assert_pattern(&store, &key, len).await;
    if let Some(t) = out.e_tag {
        assert_eq!(Some(t), store.head(&key).await.unwrap().unwrap().e_tag);
    }
    // A small body through the same entry point (below any threshold).
    let small = format!("{ns}/mp/small");
    store
        .put_multipart(&small, pattern_stream(1000))
        .await
        .unwrap();
    assert_pattern(&store, &small, 1000).await;
    // An erroring stream fails the write and leaves nothing behind.
    let bad = format!("{ns}/mp/bad");
    let s: BoxStream<'static, Result<Bytes>> = stream::iter(vec![
        Ok(Bytes::from_static(b"partial")),
        Err(Error::Body("boom".into())),
    ])
    .boxed();
    assert!(store.put_multipart(&bad, s).await.is_err());
    assert!(store.head(&bad).await.unwrap().is_none());
    store.delete_many(&[key, small]).await.unwrap();
}

/// A backend that does not override `put_multipart`, so the trait's default impl runs.
pub struct NoMultipart(pub Arc<dyn ObjectStore>);

#[async_trait]
impl ObjectStore for NoMultipart {
    async fn put(&self, key: &str, body: Bytes) -> Result<PutOutcome> {
        self.0.put(key, body).await
    }
    async fn put_if_absent(&self, key: &str, body: Bytes) -> Result<Conditional<PutOutcome>> {
        self.0.put_if_absent(key, body).await
    }
    async fn put_if_match(
        &self,
        key: &str,
        body: Bytes,
        e_tag: &str,
    ) -> Result<Conditional<PutOutcome>> {
        self.0.put_if_match(key, body, e_tag).await
    }
    async fn get(&self, key: &str) -> Result<Option<(Bytes, ObjectMeta)>> {
        self.0.get(key).await
    }
    async fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> Result<Bytes> {
        self.0.get_range(key, range).await
    }
    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        self.0.head(key).await
    }
    async fn list(&self, prefix: &str, start_after: Option<&str>) -> Result<Vec<ObjectMeta>> {
        self.0.list(prefix, start_after).await
    }
    async fn delete(&self, key: &str) -> Result<()> {
        self.0.delete(key).await
    }
    async fn delete_many(&self, keys: &[String]) -> Result<()> {
        self.0.delete_many(keys).await
    }
    fn supports_conditional_put(&self) -> bool {
        self.0.supports_conditional_put()
    }
}

pub async fn multipart_default_impl(store: Arc<dyn ObjectStore>, ns: &str) {
    let wrapped: Arc<dyn ObjectStore> = Arc::new(NoMultipart(store));
    let key = format!("{ns}/mpdefault/obj");
    let len = 3 * (1 << 20) + 17;
    wrapped
        .put_multipart(&key, pattern_stream(len))
        .await
        .expect("default put_multipart");
    assert_pattern(&wrapped, &key, len).await;
    wrapped.delete(&key).await.unwrap();
}

pub async fn list_paginates_in_key_order(store: Arc<dyn ObjectStore>, ns: &str) {
    const N: usize = 1005; // > 1000: crosses the S3 page size and the DeleteObjects batch
    let dir = format!("{ns}/list/");
    let keys: Vec<String> = (0..N).map(|i| format!("{dir}k{i:05}")).collect();
    stream::iter(keys.clone())
        .map(|k| {
            let store = store.clone();
            async move { store.put(&k, Bytes::from_static(b"v")).await }
        })
        .buffer_unordered(32)
        .try_collect::<Vec<_>>()
        .await
        .expect("bulk put");
    // A sibling that shares the string prefix but not the directory.
    let sibling = format!("{ns}/list2/x");
    store.put(&sibling, Bytes::from_static(b"s")).await.unwrap();

    let all = store.list(&dir, None).await.expect("list");
    let got: Vec<&str> = all.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(got.len(), N);
    assert!(
        got.iter().zip(&keys).all(|(a, b)| *a == b.as_str()),
        "key order"
    );
    assert!(all.iter().all(|m| m.size == 1));

    let after = store
        .list(&dir, Some(&format!("{dir}k00499")))
        .await
        .expect("list start_after");
    assert_eq!(after.len(), N - 500);
    assert_eq!(after[0].key, format!("{dir}k00500"));

    let narrow = store.list(&format!("{dir}k0100"), None).await.unwrap();
    let narrow: Vec<String> = narrow.into_iter().map(|m| m.key).collect();
    assert_eq!(
        narrow,
        (1000..1005)
            .map(|i| format!("{dir}k{i:05}"))
            .collect::<Vec<_>>()
    );

    // String prefix (not a directory boundary) spans both "list/" and "list2/".
    let spans = store.list(&format!("{ns}/list"), None).await.unwrap();
    assert_eq!(spans.len(), N + 1);
    assert_eq!(spans.last().unwrap().key, sibling);

    assert!(
        store
            .list(&format!("{ns}/nothing-here/"), None)
            .await
            .unwrap()
            .is_empty()
    );

    let mut doomed = keys.clone();
    doomed.push(sibling);
    store
        .delete_many(&doomed)
        .await
        .expect("delete_many > 1000");
    assert!(
        store
            .list(&format!("{ns}/list"), None)
            .await
            .unwrap()
            .is_empty()
    );
}

pub async fn key_grammar(store: Arc<dyn ObjectStore>) {
    for bad in ["", "/abs", "a/../b", "..", "a//b", "nul\0key"] {
        assert!(
            matches!(
                store.put(bad, Bytes::new()).await,
                Err(Error::InvalidKey { .. })
            ),
            "{bad:?} accepted"
        );
        assert!(matches!(
            store.head(bad).await,
            Err(Error::InvalidKey { .. })
        ));
    }
}
