use crate::dataset::{DatasetRef, POOL_SIZE};
use crate::joader::condition::Cond;
use crate::loader::{DataSender, Loader};
use crate::sampler::sampler_tree::SamplerTree;
use crate::{cache::cache::Cache, loader::IdxSender};
use crossbeam::channel::{Receiver, Sender};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
#[derive(Debug)]
pub struct Joader {
    dataset: DatasetRef,
    empty: bool,
    sampler_tree: Arc<Mutex<SamplerTree>>,
    // map loader id to loader
    loader_table: HashMap<u64, Loader>,
    ref_table: HashMap<u32, usize>,
    key: u32,
    sampler_recv: Receiver<HashMap<u32, HashSet<u64>>>,
    cond: Arc<Cond>,
}

fn sample(
    sampler_tree: Arc<Mutex<SamplerTree>>,
    sender: Sender<HashMap<u32, HashSet<u64>>>,
    cond: Arc<Cond>,
) {
    cond.wait();
    loop {
        let res = {
            // println!("sample try lock sampler tree....");
            let mut sampler_tree_lock = sampler_tree.lock().unwrap();
            let res = sampler_tree_lock.sample();
            // println!("sample lock sampler tree....");
            res
        };
        if res.is_empty() {
            sender.send(res).unwrap();
            cond.wait();
        } else {
            sender.send(res).unwrap();
        }
    }
}

impl Joader {
    fn get_ref_cnt(&mut self, idx: u32, count: usize) -> usize {
        *self.ref_table.get_mut(&idx).unwrap() -= count;
        self.ref_table[&idx]
    }

    pub fn contains(&self, id: u64) -> bool {
        self.loader_table.contains_key(&id)
    }

    pub fn set_hash_key(&mut self, key: u32) {
        self.key = key;
    }

    // pub fn get_mut(&mut self, id: u64) -> &mut Loader {
    //     self.loader_table.get_mut(&id).unwrap()
    // }

    pub fn new(dataset: DatasetRef) -> Joader {
        let mut ref_table = HashMap::new();
        for i in dataset.get_indices() {
            ref_table.insert(i, 0);
        }
        let (s, r) = crossbeam::channel::bounded(POOL_SIZE*8);
        let sampler_tree = Arc::new(Mutex::new(SamplerTree::new()));
        let cond = Arc::new(Cond::new());
        let joader = Joader {
            dataset,
            empty: true,
            sampler_tree: sampler_tree.clone(),
            loader_table: HashMap::new(),
            ref_table,
            key: 1,
            sampler_recv: r,
            cond: cond.clone(),
        };
        thread::spawn(move || {
            sample(sampler_tree, s, cond);
        });
        joader
    }

    #[inline]
    fn get_hash_host(&self, idx: u32) -> u32 {
        idx % self.key
    }

    #[inline]
    fn choose_local_host(&self, host_id: u32) -> bool {
        host_id == self.key - 1
    }

    async fn distributed(&mut self, data_idx: u32, loader_ids: &mut HashSet<u64>) {
        let host_id = self.get_hash_host(data_idx);
        if !self.choose_local_host(host_id) {
            let loader_id_cloned = loader_ids.clone();
            for loader_id in loader_id_cloned {
                if self
                    .loader_table
                    .get_mut(&loader_id)
                    .unwrap()
                    .send_idx(data_idx, host_id as u64)
                    .await
                {
                    // we need distributed the idx to other hosts
                    log::debug!(
                        "Joader distribted data {:} to loader {:?} in host {:?}",
                        data_idx,
                        loader_id,
                        host_id
                    );
                    loader_ids.remove(&loader_id);
                }
            }
        }
    }

    pub async fn remote_read_batch(
        &mut self,
        sampler_res: &HashMap<u32, HashSet<u64>>,
        cache: Arc<Mutex<Cache>>,
    ) {
        let mut batch_data = HashMap::new();
        let mut loader_table = HashMap::new();
        for (data_idx, loader_ids) in sampler_res {
            let loader_cnt = loader_ids.len();
            // Todo(xj): supported remote ref cnt
            batch_data.insert(*data_idx, (0, loader_cnt));
            loader_table.insert(data_idx, loader_ids);
        }
        let addr = self.dataset.read_decode_batch(
            cache.clone(),
            batch_data,
        );
        for (data_idx, addr) in &addr {
            for (idx, id) in loader_table[data_idx].iter().enumerate() {
                log::debug!(
                    "Joader load data {:} at {:?} to loader {:?}",
                    data_idx,
                    addr,
                    id
                );
                self.loader_table[id].send_data(*addr, idx).await;
            }
        }
    }

    pub async fn next_batch(&mut self, cache: Arc<Mutex<Cache>>, batch_size: usize) {
        // let now = SystemTime::now();
        let mut batch_data: HashMap<u32, (usize, usize)> = HashMap::new();
        let mut loader_table: HashMap<u32, HashSet<u64>> = HashMap::new();
        while batch_data.len() < batch_size {
            let data_table = self.sampler_recv.recv().unwrap();
            if data_table.is_empty() {
                self.empty = true;
                break;
            }
            for (data_idx, mut loader_ids) in data_table {
                let ref_cnt = self.get_ref_cnt(data_idx, loader_ids.len());
                self.distributed(data_idx, &mut loader_ids).await;

                if !loader_ids.is_empty() {
                    if let Some(set) = loader_table.get_mut(&data_idx) {
                        loader_ids.iter().for_each(|x| {
                            set.insert(*x);
                        });
                    } else {
                        loader_table.insert(data_idx, loader_ids.clone());
                    }
                    let loader_cnt = loader_ids.len();
                    assert!(loader_cnt != 0);
                    batch_data.insert(data_idx, (ref_cnt, loader_cnt));
                }
            }
        }
        // let time1 = SystemTime::now().duration_since(now).unwrap().as_secs_f32();
        // let ret = self.dataset.read_batch(cache.clone(), batch_data);
        let ret = self.dataset.read_decode_batch(cache.clone(), batch_data);
        for (data_idx, addr) in &ret {
            for (idx, id) in loader_table[data_idx].iter().enumerate() {
                log::debug!("Joader load data {:} at {:?} to {:?}", data_idx, addr, id);
                if self.loader_table.contains_key(&id) {
                    // Todo(xj): clean cache
                    self.loader_table[id].send_data(*addr, idx).await;
                }
            }
        }
        // let time2 = SystemTime::now().duration_since(now).unwrap().as_secs_f32();
        // println!("{} {}", time1, (time2 - time1) / (batch_size as f32));
    }

    pub fn del_loader(&mut self, id: u64) {
        log::debug!("Del loader {}", id);
        let mut sampler_tree = self.sampler_tree.lock().unwrap();
        let valuse = sampler_tree.get_loader_values(id);
        sampler_tree.delete(id);
        // Todo(xj): clear cache
        for v in valuse.iter() {
            *self.ref_table.get_mut(v).unwrap() -= 1;
        }
        self.loader_table.remove(&id);
    }

    pub fn add_idx_sender(&mut self, loader_id: u64, idx_sender: IdxSender, host_id: u64) {
        log::debug!("Add a idxsender {}", loader_id);
        let loader = self.loader_table.get_mut(&loader_id).unwrap();
        loader.add_idx_sender(idx_sender, host_id);
        if loader.ready() {
            log::debug!("loader id {} ready", loader_id);
            // println!("idx try lock sampler tree....");
            self.sampler_tree
                .lock()
                .unwrap()
                .insert(self.dataset.get_indices(), loader_id);
            // println!("idx lock sampler tree....");
            self.cond.notify();
            self.empty = false;
        }
    }

    pub fn add_data_sender(&mut self, loader_id: u64, data_sender: DataSender) {
        log::debug!(
            "Add a datasender {} at {}",
            loader_id,
            self.dataset.get_id()
        );
        let loader = self.loader_table.get_mut(&loader_id).unwrap();
        loader.add_data_sender(data_sender);
        if loader.ready() {
            log::debug!("loader id {} ready", loader_id);
            self.sampler_tree
                .lock()
                .unwrap()
                .insert(self.dataset.get_indices(), loader_id);
            self.cond.notify();
            self.empty = false;
        }
    }

    pub fn reset_dataloader(&mut self, loader_id: u64) {
        log::debug!(
            "Reset a datasender {} at {}",
            loader_id,
            self.dataset.get_id()
        );
        let loader = self.loader_table.get_mut(&loader_id).unwrap();
        if loader.ready() {
            log::debug!("loader id {} ready", loader_id);
            self.sampler_tree
                .lock()
                .unwrap()
                .insert(self.dataset.get_indices(), loader_id);
            self.cond.notify();
            self.empty = false;
        }
    }

    pub fn del_idx_sender(&mut self, loader_id: u64, host_id: u64) {
        let loader = self.loader_table.get_mut(&loader_id).unwrap();
        loader.del_idx_sender(host_id);
    }

    pub fn del_data_sender(&mut self, loader_id: u64) {
        log::debug!(
            "Del a datasender {} at {}",
            loader_id,
            self.dataset.get_id()
        );
        let loader = self.loader_table.get_mut(&loader_id).unwrap();
        loader.del_data_sender();
    }

    pub fn is_loader_empty(&self, loader_id: u64) -> bool {
        self.loader_table[&loader_id].is_empty()
    }

    pub fn add_loader(&mut self, loader_id: u64, nums: u32) {
        log::debug!("Add a loader {} at {}", loader_id, self.dataset.get_id());
        self.loader_table
            .insert(loader_id, Loader::new(loader_id, nums));
        for (_, cnt) in self.ref_table.iter_mut() {
            *cnt += 1;
        }
    }

    pub fn get_mut_loader(&mut self, id: u64) -> &mut Loader {
        self.loader_table.get_mut(&id).unwrap()
    }

    pub fn get_id(&self) -> u32 {
        self.dataset.get_id()
    }

    pub fn is_empty(&self) -> bool {
        self.empty
    }

    pub fn len(&self) -> u64 {
        self.dataset.len()
    }
}
