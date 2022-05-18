use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
#[derive(Debug, Clone)]
pub struct IdGenerator {
    dataset_id: Arc<AtomicU64>,
    job_id: Arc<AtomicU64>,
}

impl IdGenerator {
    pub fn get_dataset_id(&self) -> u64 {
        self.dataset_id.fetch_add(1, Ordering::SeqCst);
        self.dataset_id.load(Ordering::SeqCst)
    }

    pub fn get_job_id(&self) -> u64 {
        self.job_id.fetch_add(1, Ordering::SeqCst);
        self.job_id.load(Ordering::SeqCst)
    }

    pub fn new() -> Self {
        Self {
            dataset_id: Arc::new(AtomicU64::new(0)),
            job_id: Arc::new(AtomicU64::new(0)),
        }
    }
}
