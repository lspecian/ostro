//! `open_url`: `file:///abs/path` -> [`LocalFs`], `s3://bucket/prefix` -> [`S3Store`].

use std::sync::Arc;

use crate::{Error, LocalFs, ObjectStore, Result, S3Store, StoreOptions};

/// Open a store from a URL.
///
/// - `file:///abs/path` (host empty or `localhost`): [`LocalFs`] rooted at the path (created if
///   missing). Of `cfg`, only `conditional_put` applies.
/// - `s3://bucket/prefix`: [`S3Store`] on `bucket`, with `prefix/` prepended to every key.
///   Credentials never go in the URL (userinfo is rejected); use `cfg.credentials`.
///
/// Anything else is [`Error::Config`].
pub fn open_url(url: &str, cfg: &StoreOptions) -> Result<Arc<dyn ObjectStore>> {
    let u =
        ::url::Url::parse(url).map_err(|e| Error::Config(format!("bad object store URL: {e}")))?;
    if !u.username().is_empty() || u.password().is_some() {
        return Err(Error::Config(
            "credentials do not belong in the object store URL".into(),
        ));
    }
    if u.query().is_some() || u.fragment().is_some() {
        return Err(Error::Config(
            "object store URL must not have a query or fragment".into(),
        ));
    }
    match u.scheme() {
        "file" => {
            if u.host_str()
                .is_some_and(|h| !h.is_empty() && h != "localhost")
            {
                return Err(Error::Config(format!(
                    "file:// URL must not name a remote host: {url}"
                )));
            }
            let path = u.to_file_path().map_err(|()| {
                Error::Config(format!("file:// URL is not an absolute path: {url}"))
            })?;
            Ok(Arc::new(
                LocalFs::new(path)?.with_conditional_put(cfg.conditional_put),
            ))
        }
        "s3" => {
            let bucket = u
                .host_str()
                .filter(|h| !h.is_empty())
                .ok_or_else(|| Error::Config(format!("s3:// URL must name a bucket: {url}")))?;
            Ok(Arc::new(S3Store::new(bucket, u.path(), cfg)?))
        }
        other => Err(Error::Config(format!(
            "unsupported object store URL scheme {other:?} (expected file:// or s3://)"
        ))),
    }
}
