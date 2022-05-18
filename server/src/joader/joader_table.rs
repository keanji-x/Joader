use std::{collections::HashMap, sync::Arc};
// casue aysnc trait has not been supported, we use thread pool
use super::joader::Joader;
use crate::cache::cache::Cache;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct JoaderTable {
    // Joader is hash by the name of dataset
    joader_table: HashMap<u64, Joader>,
    cache: Arc<Mutex<Cache>>
}

impl JoaderTable {
    pub fn new(cache: Arc<Mutex<Cache>>) -> JoaderTable {
        JoaderTable {
            joader_table: HashMap::new(),
            cache
        }
    }

    pub fn add_joader(&mut self, joader: Joader) {
        log::debug!("Add Joader {:?}", joader.get_id());
        let id = joader.get_id();
        self.joader_table.insert(id, joader);
    }

    pub fn del_joader(&mut self, id: u64) {
        log::debug!("Del joader {:?}", id);
        self.joader_table.remove(&id);
    }

    pub fn get_mut(&mut self, id: u64) -> &mut Joader {
        log::debug!("Get joader {:?}", id);
        self.joader_table.get_mut(&id).unwrap()
    }

    pub fn is_empty(&self) -> bool {
        let mut empty = true;
        for (_, joader) in self.joader_table.iter() {
            empty &= joader.is_empty();
        }
        empty
    }

    pub async fn next(&mut self) -> i32 {
        let mut cnt = 0;
        for (_, joader) in self.joader_table.iter_mut() {
            if !joader.is_empty() {
                joader.next( self.cache.clone()).await;
                // joader.atomic_next(self.cache.clone()).await;
                cnt += 1;
            }
        }
        cnt
    }

    pub fn contains_dataset(&self, id: u64) -> bool {
        self.joader_table.contains_key(&id)
    }
}
