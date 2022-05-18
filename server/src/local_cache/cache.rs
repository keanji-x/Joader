// 1. Set key value
// 2. Remove value according reference

use std::{collections::HashMap, sync::Arc};
use cached::{UnboundCache, Cached};

use crate::proto::job::Data;

use super::policy::{RefCnt, Policy};

#[derive(Debug)]
pub struct Cache {
    refs: HashMap::<String, usize>,
    cache: UnboundCache<String, Arc<Vec<Data>>>,
    capacity: usize,
    size: usize,
    policy: RefCnt,
}

impl Cache {
    pub fn new() -> Self{
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
        while !self.check_valid(value.len()) {
            self.evict()
        }
        self.policy.set(key.to_string(), ref_cnt);
        self.size += 1;
        self.cache.cache_set(key.to_string(), value);
        self.refs.insert(key.to_string(), ref_cnt);
    }

    pub fn get(&mut self, key: &String)-> Option<&Arc<Vec<Data>>> {
        self.cache.cache_get(key)
    }
    fn check_valid(&self, l: usize)-> bool {
        self.size + l < self.capacity
    }

    fn evict(&mut self) {
        let evict_keys = self.policy.evict().unwrap();
        
        for key in evict_keys.iter() {
            let v = self.cache.cache_remove(key);
            self.size -= v.unwrap().len();
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     #[test]
//     fn test_cache() {
//         let mut cache = Cache::new();
//         cache.set("1", vec![1], 0);
//         cache.set("2", vec![1,2], 0);
//         cache.set("3", vec![1,2,3], 0);
//         assert_eq!(cache.get(&"1".to_string()).unwrap(), &[1]);
//     }


//     #[test]
//     fn test_ref_cnt() {
//         let mut cache = Cache::with_capacity(5);
//         for i in 0..10 {
//             cache.set(&i.to_string(), vec![i], i as usize);
//         }
//         for i in 0..5u32 {
//             assert_eq!(cache.get(&i.to_string()), None);
//         }
//     }
// }

