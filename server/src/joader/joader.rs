use crate::cache::cache::Cache;
use crate::dataset::DatasetRef;
use crate::job::Job;
use crate::proto::job::Condition;
use crate::sampler::isa_sampler_tree::SamplerTree;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
#[derive(Debug)]
pub struct Joader {
    dataset: DatasetRef,
    sampler_tree: Arc<Mutex<SamplerTree>>,
    // map loader id to loader
    job_table: HashMap<u64, Arc<Job>>,
    ref_table: HashMap<u32, usize>,
    size: usize,
}

async fn read(
    idx: u32,
    ref_cnt: usize,
    cache: Arc<Mutex<Cache>>,
    dataset: DatasetRef,
    job_set: Vec<Arc<Job>>,
) {
    let data = dataset.read(idx);
    let key = dataset.get_id().to_string() + &idx.to_string();
    let mut cache_lock = cache.lock().await;
    cache_lock.set(&key, data.clone(), ref_cnt);
    for job in job_set {
        job.push(data.clone()).await;
    }
}

impl Joader {
    fn get_ref_cnt(&mut self, idx: u32, count: usize) -> usize {
        *self.ref_table.get_mut(&idx).unwrap() -= count;
        self.size -= count;
        self.ref_table[&idx]
    }

    pub fn new(dataset: DatasetRef) -> Joader {
        let mut ref_table = HashMap::new();
        for i in dataset.get_indices(None) {
            ref_table.insert(i, 0);
        }
        let sampler_tree = Arc::new(Mutex::new(SamplerTree::new()));
        let joader = Joader {
            dataset,
            sampler_tree: sampler_tree.clone(),
            job_table: HashMap::new(),
            ref_table,
            size: 0,
        };
        joader
    }

    pub async fn atomic_next(&mut self, cache: Arc<Mutex<Cache>>) {
        // shadown the job
        let mask = HashSet::new();
        let mut can_push = true;
        for (_, job) in self.job_table.iter() {
            // if all job read in the same order, then we stop it when a buffer is full
            can_push &= job.can_push();
        }
        if !can_push {
            return;
        }
        for (_, job) in self.job_table.iter() {
            // if all job read in the same order, then we stop it when a buffer is full
            job.add_pending();
        }
        let sample_res = {
            let mut sampler_tree_lock = self.sampler_tree.lock().await;
            sampler_tree_lock.sample(&mask)
        };
        log::debug!(
            "sampling result (data_set, job_set){:?} with mask {:?}",
            sample_res,
            mask
        );
        for (data_idx, job_id_set) in sample_res {
            let ref_cnt = self.get_ref_cnt(data_idx, job_id_set.len());
            let dataset = self.dataset.clone();
            let clone_cache = cache.clone();
            let mut job_set = Vec::new();
            for job_id in job_id_set {
                job_set.push(self.job_table[&job_id].clone());
            }
            tokio::spawn(async move {
                read(data_idx, ref_cnt, clone_cache, dataset, job_set).await;
            });
        }
    }

    pub async fn next(&mut self, cache: Arc<Mutex<Cache>>) {
        // shadown the job
        let mut mask = HashSet::new();
        for (id, job) in self.job_table.iter() {
            // if all job read in the same order, then we stop it when a buffer is full
            if job.can_push() {
                // return;
                job.add_pending();
            } else {
                mask.insert(*id);
            }
        }
        let sample_res = {
            let mut sampler_tree_lock = self.sampler_tree.lock().await;
            sampler_tree_lock.sample_with_buffer(&mask)
        };
        log::debug!(
            "sampling result (data_set, job_set){:?} with mask {:?}",
            sample_res,
            mask
        );
        if sample_res.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        for (data_idx, job_id_set) in sample_res {
            let ref_cnt = self.get_ref_cnt(data_idx, job_id_set.len());
            let dataset = self.dataset.clone();
            let clone_cache = cache.clone();
            let mut job_set = Vec::new();
            for job_id in job_id_set {
                job_set.push(self.job_table[&job_id].clone());
            }
            tokio::spawn(async move {
                read(data_idx, ref_cnt, clone_cache, dataset, job_set).await;
            });
        }
    }

    pub async fn del_job(&mut self, id: u64) {
        log::debug!("Del job {}", id);
        let mut sampler_tree = self.sampler_tree.lock().await;
        let valuse = sampler_tree.get_job_values(id);
        sampler_tree.delete(id);
        // Todo(xj): clear cache
        for v in valuse.iter() {
            self.size -= 1;
            *self.ref_table.get_mut(v).unwrap() -= 1;
        }
        self.job_table.remove(&id);
    }

    pub async fn add_job(&mut self, job: Arc<Job>, condition: Option<Condition>) -> usize {
        let indices  = self.dataset.get_indices(condition);
        log::debug!("Add a loader {} at {}: {:?}", job.get_id(), self.dataset.get_id(), indices);
        let len = indices.len();
        self.sampler_tree
            .lock()
            .await
            .insert(indices, job.get_id());
        let job_id = job.get_id();
        self.job_table.insert(job_id, job);
        for (_, cnt) in self.ref_table.iter_mut() {
            *cnt += 1;
            self.size += 1;
        }
        len
    }

    pub fn get_id(&self) -> u64 {
        self.dataset.get_id()
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn len(&self) -> usize {
        self.dataset.len()
    }
}
