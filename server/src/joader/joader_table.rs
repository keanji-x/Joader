use std::{
    collections::{HashMap, HashSet},
    sync::Arc
};
// casue aysnc trait has not been supported, we use thread pool
use crate::{cache::cache::Cache, proto::distributed::SampleResult, service::GlobalID, dataset::POOL_SIZE};
use std::sync::Mutex;
use super::joader::Joader;

#[derive(Debug)]
pub struct JoaderTable {
    // Joader is hash by the name of dataset
    joader_table: HashMap<u32, Joader>,
    cache: Arc<Mutex<Cache>>,
    hash_key: u32,
    shm_path: String,
}

impl JoaderTable {
    pub fn new(cache: Arc<Mutex<Cache>>, shm_path: String) -> JoaderTable {
        JoaderTable {
            joader_table: HashMap::new(),
            cache,
            hash_key: 1,
            shm_path,
        }
    }

    pub fn add_joader(&mut self, mut joader: Joader) {
        log::debug!("Add Joader {:?}", joader.get_id());
        joader.set_hash_key(self.hash_key);
        let id = joader.get_id();
        self.joader_table.insert(id, joader);
    }

    pub fn del_joader(&mut self, id: u32) {
        log::debug!("Del joader {:?}", id);
        self.joader_table.remove(&id);
    }

    pub fn get_mut(&mut self, id: u32) -> &mut Joader {
        log::debug!("Get joader {:?}", id);
        self.joader_table.get_mut(&id).unwrap()
    }

    pub fn get_shm_path(&self) -> String {
        self.shm_path.clone()
    }

    pub fn is_empty(&self) -> bool {
        let mut empty = true;
        for (_, joader) in self.joader_table.iter() {
            empty &= joader.is_empty();
        }
        empty
    }

    pub async fn next(&mut self) {
        for (_, joader) in self.joader_table.iter_mut() {
            if !joader.is_empty() {
                joader.next_batch(self.cache.clone(), POOL_SIZE*8).await;
            }
        }
    }

    pub fn set_hash_key(&mut self, num: u32) {
        self.hash_key = num + 1;
    }

    pub async fn remote_read(&mut self, sample_res: &Vec<SampleResult>) {
        let mut res = HashMap::<u32, HashMap<u32, HashSet<u64>>>::new();
        for s in sample_res {
            let loader_id = s.loader_id;
            let dataset_id = GlobalID::parse_dataset_id(loader_id);
            if !res.contains_key(&dataset_id) {
                res.insert(dataset_id, HashMap::new());
            }
            for idx in &s.indices {
                let idx_map = res.get_mut(&dataset_id).unwrap();
                if !idx_map.contains_key(idx) {
                    idx_map.insert(*idx, HashSet::new());
                }
                idx_map.get_mut(idx).unwrap().insert(loader_id);
            }
        }

        for (dataset_id, s) in res {
            self.joader_table
                .get_mut(&dataset_id)
                .unwrap()
                .remote_read_batch(&s, self.cache.clone())
                .await;
        }
    }

    pub fn contains_dataset(&self, id: u32) -> bool {
        self.joader_table.contains_key(&id)
    }
}
