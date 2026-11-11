use crate::backoff::BackoffCounter;
use crate::config::{S2cOptions, S2cRetryOptions};
use crate::error::is_transient;
use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    Attributes, Error, Extensions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
    PutResult, TagSet, UpdateVersion,
};
use std::collections::HashSet;
use std::sync::Arc;
use std::vec;
use tokio_util::bytes;

pub struct ObjectReadWrite {
    object_store: Arc<dyn ObjectStore>,
    unlimited_retry: S2cRetryOptions,
}

impl ObjectReadWrite {
    pub fn new(object_store: Arc<dyn ObjectStore>, s2c_options: &'static S2cOptions) -> Self {
        Self {
            object_store,
            unlimited_retry: s2c_options.retry.to_unlimited(),
        }
    }

    async fn put(
        &self,
        path: Path,
        payload: Bytes,
        put_options: PutOptions,
    ) -> Result<PutResult, Error> {
        let mut backoff_counter = BackoffCounter::new(&self.unlimited_retry);
        loop {
            match self
                .object_store
                .put_opts(
                    &path,
                    PutPayload::from_bytes(payload.clone()),
                    put_options.clone(),
                )
                .await
            {
                Ok(re) => return Ok(re),
                Err(e) => {
                    if is_transient(&e) {
                        backoff_counter.await_attempt().await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }

    pub async fn put_if_absent(&self, path: Path, payload: Bytes) -> Result<PutResult, Error> {
        self.put(
            path,
            payload,
            PutOptions {
                mode: PutMode::Create,
                tags: TagSet::default(),
                attributes: Attributes::default(),
                extensions: Extensions::default(),
            },
        )
        .await
    }

    pub async fn put_if_match(
        &self,
        path: Path,
        payload: Bytes,
        e_tag: String,
    ) -> Result<PutResult, Error> {
        self.put(
            path,
            payload,
            PutOptions {
                mode: PutMode::Update(UpdateVersion {
                    e_tag: Some(e_tag),
                    version: None,
                }),
                tags: TagSet::default(),
                attributes: Attributes::default(),
                extensions: Extensions::default(),
            },
        )
        .await
    }

    pub async fn read(&self, path: Path) -> Result<Bytes, Error> {
        let mut backoff_counter = BackoffCounter::new(&self.unlimited_retry);
        loop {
            let result = match self.object_store.get(&path).await {
                Ok(re) => re.bytes().await,
                Err(e) => Err(e),
            };
            match result {
                Ok(bytes) => return Ok(bytes),
                Err(e) => {
                    if is_transient(&e) {
                        backoff_counter.await_attempt().await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }

    pub async fn first_under_prefix(&self, prefix: Path) -> Result<Option<Path>, Error> {
        let mut backoff_counter = BackoffCounter::new(&self.unlimited_retry);
        loop {
            let mut stream = self.object_store.list(Some(&prefix));
            match stream.next().await {
                None => return Ok(None),
                Some(Ok(obj)) => return Ok(Some(obj.location)),
                Some(Err(e)) => {
                    if is_transient(&e) {
                        backoff_counter.await_attempt().await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }

    pub async fn delete(&self, mut paths: Vec<Path>) -> Result<(), Error> {
        let mut deleted: HashSet<Path> = HashSet::new();

        let mut backoff = BackoffCounter::new(&self.unlimited_retry);

        let all_len = paths.len();

        let mut last_err: Option<Error> = None;

        while backoff.can_attempt() {
            loop {
                let stream = tokio_stream::iter(paths.clone()).map(Ok).boxed();

                let mut result_stream = self.object_store.delete_stream(stream);
                while let Some(next) = result_stream.next().await {
                    match next {
                        Ok(p) => {
                            deleted.insert(p);
                        }
                        Err(Error::NotFound { .. }) => {
                            // Ignore
                        }
                        Err(e) => {
                            if !is_transient(&e) {
                                return Err(e);
                            }
                            last_err = Some(e);
                        }
                    }
                }
                if deleted.len().eq(&all_len) {
                    return Ok(());
                } else {
                    paths.retain(|e| !deleted.contains(e));
                    backoff.await_attempt().await;
                    if let Some(err) = last_err {
                        return Err(err);
                    }
                }
            }
        }
        Ok(())
    }

}
