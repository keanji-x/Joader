use std::{
    collections::{HashMap, LinkedList},
    sync::Arc,
};

#[derive(Debug)]
struct Zone {
    start: u64,
    end: u64,
}

impl Zone {

    fn len(&self) -> u64 {
        self.end - self.start
    }
}

#[derive(Debug)]
pub struct FreeList {
    // the list store the start and the end
    free_list: LinkedList<Arc<Zone>>,
    start_hash: HashMap<u64, Arc<Zone>>,
    end_hash: HashMap<u64, Arc<Zone>>,
}

impl FreeList {
    pub fn new() -> FreeList {
        FreeList {
            free_list: LinkedList::new(),
            start_hash: HashMap::new(),
            end_hash: HashMap::new(),
        }
    }

    pub fn insert(&mut self, off: u64, len: u64) {
        if len == 0 {
            return;
        }
        // Todo(xj): merge the continues space
        let mut start = off;
        let mut end = off + len;

        if let Some((_old_end, v)) = self.end_hash.remove_entry(&start) {
            // |old_start .. old_end|start .. end| => |old_start .. end|
            start = v.start;
        }
        if let Some((_old_start, v)) = self.start_hash.remove_entry(&end) {
            // |start .. end|old_start .. old_end| => |start .. end|
            end = v.end;
        }
        self.free_list.push_back(Arc::new(Zone { start, end }));
        self.start_hash
            .insert(start, self.free_list.back_mut().unwrap().clone());
        self.end_hash
            .insert(end, self.free_list.back_mut().unwrap().clone());
        log::debug!(
            "Free_List: {:?} start_hash: {:?} end_hash: {:?}",
            self.start_hash.values(),
            self.start_hash.keys(),
            self.end_hash.keys()
        );
    }

    pub fn get(&mut self, requested_len: u64) -> Option<(u64, u64)> {
        //Todo(xj): find the biggest block
        // find the block larger than head
        self.clear();
        let mut ret: Option<(u64, u64)> = None;
        for zone in self.free_list.iter() {
            if self.is_valid(zone) && zone.len() >= requested_len {
                self.start_hash.remove_entry(&zone.start);
                self.end_hash.remove_entry(&zone.end);
                ret = Some((zone.start, zone.len()));
                break;
            }
        }
        if let Some((off, len)) = ret {
            self.insert(off+requested_len, len-requested_len);
            return Some((off, requested_len));
        }
        None
    }

    fn clear(&mut self) {
        let mut new_free_list = LinkedList::new();
        for zone in self.free_list.iter() {
            if self.is_valid(zone) {
                new_free_list.push_back(zone.clone());
            }
        }
        self.free_list = new_free_list;
    }

    fn is_valid(&self, zone: &Zone) -> bool {
        self.start_hash.contains_key(&zone.start) && self.end_hash.contains_key(&zone.end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let len = [10, 20, 30, 40, 50];
        let start = 0;
        let mut end = 0;
        let mut space = Vec::new();
        for l in &len {
            space.push((end, *l));
            end += *l;
        }

        let mut fl = FreeList::new();
        for (len, off) in &space {
            fl.insert(*len, *off);
        }
        assert_eq!(fl.get(end - start), Some((start, end)));
        assert_eq!(fl.get(1), None);

        let mut max = (0, 0);
        for (idx, (off, len)) in space.iter().enumerate() {
            if (idx & 1) == 0 {
                fl.insert(*off, *len);
                max = (*off, *len);
            }
        }
        assert_eq!(fl.get(max.1 - max.0), Some(max));
        fl.insert(max.0, max.1);

        for (idx, (off, len)) in space.iter().enumerate() {
            if (idx & 1) == 1 {
                fl.insert(*off, *len);
            }
        }
        assert_eq!(fl.get(end - start), Some((start, end)));
    }
}
