use crate::network::error::ClientError;
use crate::proto::io::S2cMessage;
use std::collections::HashMap;
use tokio::sync::{oneshot, RwLock};

#[derive(Default)]
pub struct InflightRequests {
    requests: RwLock<HashMap<String, oneshot::Sender<Result<S2cMessage, ClientError>>>>,
}

impl InflightRequests {
    fn new() -> Self {
        Self::default()
    }

    pub async fn add(
        &self,
        correlation_id: String,
    ) -> oneshot::Receiver<Result<S2cMessage, ClientError>> {
        let (tx, rx) = oneshot::channel();
        self.requests.write().await.insert(correlation_id, tx);
        rx
    }

    pub async fn respond(&self, message: S2cMessage) {
        let correlation_id = message.correlation_id.as_str();
        if let Some(sender) = self.requests.write().await.remove(correlation_id) {
            let _ = sender.send(Ok(message));
        }
    }

    pub async fn discard(&self, correlation_id: String) {
        self.requests.write().await.remove(&correlation_id);
    }

    pub async fn fail_all(&self, err: &ClientError) {
        self.requests.write().await.drain().for_each(|(_, tx)| {
            let _ = tx.send(Err(err.clone()));
        });
    }

    pub async fn clear(&self) {
        self.requests.write().await.clear();
    }

    pub async fn size(&self) -> usize {
        self.requests.read().await.len()
    }
}
