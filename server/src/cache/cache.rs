// 1. Set key value
// 2. Remove value according reference

use cached::{Cached, UnboundCache};
use std::{collections::HashMap, sync::Arc};

use crate::proto::job::Data;

use super::policy::{Policy, RefCnt};

#[derive(Debug)]
pub struct Cache {
    refs: HashMap<String, usize>,
    cache: UnboundCache<String, Arc<Vec<Data>>>,
    capacity: usize,
    size: usize,
    policy: RefCnt,
}

impl Cache {
    pub fn new() -> Self {
        Cache {
            refs: HashMap::new(),
            cache: UnboundCache::new(),
            capacity: usize::MAX,
            size: 0,
            policy: RefCnt::new(64),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Cache {
            refs: HashMap::new(),
            cache: UnboundCache::new(),
            capacity: cap,
            size: 0,
            policy: RefCnt::new(64),
        }
    }

    pub fn set(&mut self, key: &str, value: Arc<Vec<Data>>, ref_cnt: usize) {
        for data in value.iter() {
            self.size += data.bs.len();
        }
        while self.size > self.capacity {
            self.evict();
        }
        self.policy.set(key.to_string(), ref_cnt);
        self.cache.cache_set(key.to_string(), value);
        self.refs.insert(key.to_string(), ref_cnt);

        log::debug!(
            "Cache(size: {:}, cap: {:}) set {:?} with ref_cnt {:} ",
            self.size,
            self.capacity,
            key,
            ref_cnt
        );
        
    }

    pub fn get(&mut self, key: &String) -> Option<&Arc<Vec<Data>>> {
        self.cache.cache_get(key)
    }

    fn evict(&mut self) {
        let evict_keys = self.policy.evict().unwrap();
        log::debug!(
            "Cache (size: {:}) evict {:?}",
            self.size,
            evict_keys,
        );
        for key in evict_keys.iter() {
            let v = self.cache.cache_remove(key);
            for data in v.unwrap().iter() {
                self.size -= data.bs.len();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::job::data::DataType;

    fn construct_data(len: usize) -> Data {
        Data {
            bs: (0..len).map(|x| (x % 256) as u8).collect::<Vec<_>>(),
            ty: DataType::Image as i32,
        }
    }
    #[test]
    fn test_cache_simple() {
        let mut cache = Cache::with_capacity(100);
        let data = Arc::new(vec![construct_data(100)]);
        cache.set("1", data.clone(), 0);
        assert_eq!(*cache.get(&"1".to_string()).unwrap(), data);
        let data = Arc::new(vec![construct_data(100)]);
        cache.set("2", data.clone(), 0);
        assert_eq!(cache.get(&"1".to_string()), None);
        assert_eq!(*cache.get(&"2".to_string()).unwrap(), data);
    }

    #[test]
    fn test_cache() {
        let mut cache = Cache::with_capacity(500);
        for i in 1..10 {
            let data = Arc::new(vec![construct_data(100)]);
            cache.set(&i.to_string(), data, i);
        }
    }
}
