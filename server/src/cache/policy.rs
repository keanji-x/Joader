use std::collections::{HashMap, HashSet};

pub trait Policy {
    fn set(&mut self, key: String, ref_cnt: usize);
    fn evict(&mut self) -> Option<HashSet<String>>;
}

#[derive(Debug)]
pub struct RefCnt {
    key_table: HashMap<String, usize>,
    ref_table: Vec<HashSet<String>>,
}

impl RefCnt {
    pub fn new(level: usize) -> Self {
        let mut ref_table = Vec::new();
        for _ in 0..level {
            ref_table.push(HashSet::new());
        }
        RefCnt {
            key_table: HashMap::new(),
            ref_table,
        }
    }
}


impl Policy for RefCnt {
    fn set(&mut self, key: String, ref_cnt: usize) {
        if self.key_table.contains_key(&key) {
            let old_ref = self.key_table[&key];
            self.ref_table[old_ref].remove(&key);
        }
        self.key_table.insert(key.clone(), ref_cnt);
        self.ref_table[ref_cnt].insert(key);
    }

    fn evict(&mut self) -> Option<HashSet<String>> {
        let mut res = None;
        for node in self.ref_table.iter_mut() {
            if node.is_empty() {
                continue;
            }
            res = Some(node.clone());
            node.clear();
            break;
        }
        res
    }
}
