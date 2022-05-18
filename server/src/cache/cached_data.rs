use std::collections::HashMap;

#[derive(Debug)]
pub struct CachedData {
    data2head: HashMap<u64, usize>,
    head2data: HashMap<usize, u64>,
}

impl CachedData {
    pub fn new() -> Self {
        CachedData {
            data2head: HashMap::new(),
            head2data: HashMap::new(),
        }
    }

    pub fn add(&mut self, head: usize, data_id: u64) {
        log::debug!("Cache data {:?} in {:?}", data_id, head);
        self.data2head.insert(data_id, head);
        self.head2data.insert(head, data_id);
    }

    pub fn remove(&mut self, head: usize) {
        if let Some(data) = self.head2data.remove(&head) {
            let head = self.data2head.remove(&data);
            log::debug!("Delete data head: {:?} name:{:?} in cache", head, data);
        }
    }

    pub fn contains(&self, data_id: u64) -> Option<usize> {
        self.data2head.get(&data_id).map(|x| *x)
    }
}
