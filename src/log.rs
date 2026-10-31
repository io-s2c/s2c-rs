use crate::backoff::BackoffCounter;
use crate::config::S2cOptions;
use crate::error::is_transient;
use crate::paths::KeysResolver;
use bytes::Bytes;

use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    Attributes, Extensions, ObjectStore, ObjectStoreExt, PutMode, PutOptions,
    PutPayload, PutResult, TagSet,
};
use std::sync::Arc;
use tokio_util::bytes;

pub struct S2cLog {
    object_store: Arc<dyn ObjectStore>,
    key_resolver: KeysResolver,
    s2c_options: &'static S2cOptions,
}

impl S2cLog {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        group_id: String,
        s2c_options: &'static S2cOptions,
    ) -> Self {
        Self {
            object_store,
            key_resolver: KeysResolver::new(group_id),
            s2c_options,
        }
    }

    pub async fn append(
        &self,
        index: u64,
        payload: Bytes,
    ) -> Result<PutResult, object_store::Error> {
        let mut backoff_counter = BackoffCounter::new(&self.s2c_options.retry);
        loop {
            match self
                .object_store
                .put_opts(
                    &Path::from(self.key_resolver.log_entry(KeysResolver::invert(index))),
                    PutPayload::from_bytes(payload.clone()),
                    PutOptions {
                        mode: PutMode::Create,
                        tags: TagSet::default(),
                        attributes: Attributes::default(),
                        extensions: Extensions::default(),
                    },
                )
                .await
            {
                Ok(re) => return Ok(re),
                Err(e @ object_store::Error::AlreadyExists { .. }) => {
                    return Err(e);
                }
                Err(e @ object_store::Error::Generic { .. }) => {
                    if (is_transient(&e)) {
                        if (backoff_counter.can_attempt()) {
                            backoff_counter.await_attempt().await;
                        } else {
                            return Err(e);
                        }
                    }
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }

    pub async fn get_head(&self) -> Result<Option<u64>, object_store::Error> {
        let mut backoff_counter = BackoffCounter::new(&self.s2c_options.retry);
        let prefix = Path::from(self.key_resolver.log_prefix());
        let mut stream = self.object_store.list(Some(&prefix));
        loop {
            match stream.next().await {
                None => return Ok(None),
                Some(re) => match re {
                    Err(e) => {
                        if is_transient(&e) {
                            if backoff_counter.can_attempt() {
                                backoff_counter.await_attempt().await;
                            }
                        } else {
                            return Err(e);
                        }
                    }
                    Ok(obj) => {
                        return Ok(obj
                            .location
                            .filename()
                            .map(String::from)
                            .map(|name| name.parse().expect("Not a valid index")));
                    }
                },
            }
        }
    }

    pub async fn get_at(&self, index: u64) -> Result<Bytes, object_store::Error> {
        let path = self.key_resolver.log_entry(KeysResolver::invert(index));
        let p = path.as_str();
        let mut backoff_counter = BackoffCounter::new(&self.s2c_options.retry);
        loop {
            match self.object_store.get(&Path::from(p)).await {
                Ok(re) => {
                    backoff_counter.reset();
                        match re.bytes().await {
                            Ok(bytes) => return Ok(bytes),
                            Err(e) => {
                                if is_transient(&e) && backoff_counter.can_attempt() {
                                    backoff_counter.await_attempt().await;
                                } else {
                                    return Err(e);
                                }
                            }
                        }
                }
                Err(e) => {
                    if is_transient(&e) && backoff_counter.can_attempt() {
                        backoff_counter.await_attempt().await;
                    } else {
                        return Err(e)
                    }
                }
            }
        }
    }
}