use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::proto::job::Data;
// Loader store the information of schema, dataset and filter
const CAP: usize = 1024;
#[derive(Debug)]
pub struct Job {
    id: u64,
    sender: Sender<Arc<Vec<Data>>>,
    pending: AtomicUsize
}

impl Job {
    pub fn new(id: u64) -> (Arc<Self>, Receiver<Arc<Vec<Data>>>) {
        let (s, r) = channel::<Arc<Vec<Data>>>(CAP);
        (
            Arc::new(Job {
                id,
                sender: s,
                pending: AtomicUsize::new(0)
            }),
            r,
        )
    }

    pub fn get_id(&self) -> u64 {
        self.id
    }

    pub fn is_full(&self) -> bool {
        self.sender.capacity() == 0
    }

    pub async fn push(&self, v: Arc<Vec<Data>>) {
        log::debug!("{} push- data with pending {:?} capacity {}", self.id, self.pending.load(Ordering::SeqCst), self.sender.capacity());
        self.sender.send(v).await.unwrap();
        self.pending.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn can_push(&self) -> bool {
        log::debug!("{} try push data with pending {:?} capacity: {}", self.id, self.pending.load(Ordering::SeqCst), self.sender.capacity());
        self.pending.load(Ordering::SeqCst) < self.sender.capacity()
    }

    pub fn add_pending(&self) {
        self.pending.fetch_add(1, Ordering::SeqCst);
    }

    pub fn capacity(&self) -> usize {
        self.sender.capacity()
    }
}
