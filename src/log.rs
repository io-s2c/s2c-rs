use crate::config::S2cOptions;
use crate::object_read_write::ObjectReadWrite;
use crate::paths::KeysResolver;
use bytes::Bytes;

use moka::future::Cache;
use moka::policy::EvictionPolicy;
use object_store::path::Path;
use object_store::{Error, PutResult};
use std::sync::Arc;
use tokio_util::bytes;

pub struct S2cLog {
    object_read_write: ObjectReadWrite,
    key_resolver: KeysResolver,
    cache: Cache<u64, Bytes>,
}

impl S2cLog {
    pub fn new(
        object_read_write: ObjectReadWrite,
        group_id: String,
        s2c_options: &'static S2cOptions,
    ) -> Self {
        Self {
            object_read_write,
            key_resolver: KeysResolver::new(group_id),
            cache: Cache::builder()
                .eviction_policy(EvictionPolicy::lru())
                .max_capacity(s2c_options.log_lru_cache_size as u64)
                .build(),
        }
    }

    pub async fn append(&self, index: u64, payload: Bytes) -> Result<PutResult, Error> {
        let path = Path::from(self.key_resolver.log_entry(KeysResolver::invert(index)));
        let result = self
            .object_read_write
            .put_if_absent(path, payload.clone())
            .await?;
        self.cache.insert(index, payload).await;
        Ok(result)
    }

    pub async fn get_head(&self) -> Result<Option<u64>, Error> {
        let prefix = Path::from(self.key_resolver.log_prefix());
        Ok(self
            .object_read_write
            .first_under_prefix(prefix)
            .await?
            .and_then(|location| location.filename().map(String::from))
            .map(|name| name.parse().expect("Not a valid index")))
    }

    pub async fn get_at(&self, index: u64) -> Result<Option<Bytes>, Arc<Error>> {
        match self.cache.try_get_with(index, self._get_at(index)).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) => match *e {
                Error::NotFound { .. } => Ok(None),
                _ => Err(e),
            },
        }
    }

    pub async fn delete_range(&self, start: u64, end: u64) -> Result<(), Error> {
        let paths: Vec<Path> = (start..end).map(|n: u64| n.to_string().into()).collect();
        self.object_read_write.delete(paths).await
    }

    async fn _get_at(&self, index: u64) -> Result<Bytes, Error> {
        let path = Path::from(self.key_resolver.log_entry(KeysResolver::invert(index)));
        self.object_read_write.read(path).await
    }
}
