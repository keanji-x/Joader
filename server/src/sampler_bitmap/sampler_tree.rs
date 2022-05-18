use super::sampler_node::{Node, NodeRef};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Default, Debug)]
pub struct SamplerTree {
    root: Option<NodeRef>,
    // (loader_id, loader size)
    loader_set: Vec<(u64, usize)>,
}

impl SamplerTree {
    pub fn new() -> Self {
        SamplerTree {
            root: None,
            loader_set: Vec::new(),
        }
    }

    pub fn insert(&mut self, indices: Vec<u32>, id: u64) {
        log::debug!("Sampler insert {:?} data {:?}", indices.len(), id);
        let mut loader_set = HashSet::new();
        loader_set.insert(id);
        let node = Node::new(indices, loader_set);
        if let Some(mut root) = self.root.clone() {
            self.root = Some(root.insert(node));
        } else {
            self.root = Some(node);
        }
        self.loader_set.clear();
        // keep order
        self.root
            .clone()
            .unwrap()
            .get_loader_set(&mut self.loader_set, 0);
    }

    pub fn delete(&mut self, id: u64) {
        log::debug!("Del Sampler {}", id);
        if let Some(root) = &mut self.root {
            self.root = root.delete(id);
        }
        self.loader_set.clear();
        if let Some(root) = &self.root {
            root.get_loader_set(&mut self.loader_set, 0);
        }
        log::debug!("Del Sampler {} finish.....", id);
    }

    pub fn get_task_values(&self, loader_id: u64) -> Vec<u32> {
        if let Some(root) = &self.root {
            return root.get_loader_values(loader_id);
        }
        Vec::new()
    }

    pub fn clear_loader(&mut self) -> Vec<u64> {
        let mut new_loader_set = Vec::new();
        let mut del_loader = Vec::new();
        for loader in &self.loader_set {
            if loader.1 == 0 {
                del_loader.push(loader.0);
            } else {
                new_loader_set.push(loader.clone());
            }
        }
        if let Some(mut root) = self.root.clone() {
            for id in &del_loader {
                self.root = root.delete(*id);
            }
        }

        self.loader_set = new_loader_set;
        del_loader
    }

    pub fn sample(&mut self, mask: &HashSet<u64>) -> HashMap<u32, HashSet<u64>> {
        let mut loaders = Vec::new();
        for loader in &self.loader_set {
            if loader.1 != 0 {
                loaders.push(loader.clone())
            }
        }
        log::debug!("Sampler sample {:?}", loaders);
        let mut decisions = Vec::new();
        let mut res = HashMap::<u32, HashSet<u64>>::new();
        match self.root.clone() {
            Some(mut root) => root.decide(&mut loaders, &mut decisions, vec![]),
            None => return res,
        }

        for decision in decisions.iter_mut() {
            let ret = decision.execute(mask);
            if let Some(loader_set) = res.get_mut(&ret) {
                for loader in decision.get_loaders() {
                    loader_set.insert(loader);
                }
            } else {
                if !decision.get_loaders().is_empty() {
                    res.insert(ret, decision.get_loaders());
                }
            }
        }

        let mut reload = false;
        for decision in decisions.iter_mut() {
            reload |= decision.complent(self.root.clone().unwrap());
        }
        for (id, len) in self.loader_set.iter_mut() {
            if !mask.contains(id) && *len != 0 {
                *len -= 1;
            }
        }
        if reload {
            self.loader_set.clear();
            if let Some(root) = &self.root {
                root.get_loader_set(&mut self.loader_set, 0);
            }
        }
        self.clear_loader();
        log::debug!("Sampler get {:?}", res);
        res
    }

    pub fn is_empty(&self) -> bool {
        let mut loaders = Vec::new();
        for loader in &self.loader_set {
            if loader.1 != 0 {
                loaders.push(loader.clone())
            }
        }
        loaders.is_empty()
    }

    pub fn get_loader_values(&self, loader_id: u64) -> Vec<u32> {
        if let Some(root) = self.root.as_ref() {
            return root.get_loader_values(loader_id);
        }
        Vec::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use std::{iter::FromIterator, time::Instant};
    #[test]
    fn test_bm_mask() {
        // log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
        let mut mask = HashSet::new();
        mask.insert(1);
        mask.insert(2);
        mask.insert(3);
        sample_mask(8, &mask);
    }

    fn sample_mask(tasks: u64, mask: &HashSet<u64>) {
        let mut sampler = SamplerTree::new();
        let mut rng = rand::thread_rng();
        let mut vec_keys = Vec::<HashSet<u32>>::new();
        let mut map: HashMap<u64, HashSet<u32>> = HashMap::new();

        // let sizes = [1, 2, 3, 4, 16, 32];
        for id in 0..tasks {
            let size = rng.gen_range(100000..1000000);
            // let size = sizes[id as usize];
            let keys = (0..size).into_iter().collect::<Vec<u32>>();
            vec_keys.push(HashSet::from_iter(keys.iter().cloned()));
            sampler.insert(keys, id);
            map.insert(id, HashSet::new());
        }

        let mut time;
        loop {
            let now = Instant::now();
            sampler.clear_loader();
            let res = sampler.sample(mask);
            time = now.elapsed().as_secs_f32();
            if res.is_empty() {
                break;
            }
            for (x, tasks) in &res {
                for task in tasks {
                    map.get_mut(task).unwrap().insert(*x);
                }
            }
        }
        
        println!("time cost in one turn: {}", time);
        for (task, set) in &map {
            if mask.contains(task) {
                assert_eq!(&HashSet::new(), set);
            } else {
                let keys = &vec_keys[(*task) as usize];
                assert_eq!(keys, set, "task {} with len {}",*task, vec_keys[(*task) as usize].len());
            }
        }

        let mut time;
        loop {
            let now = Instant::now();
            sampler.clear_loader();
            let res = sampler.sample(&HashSet::new());
            time = now.elapsed().as_secs_f32();
            if res.is_empty() {
                break;
            }
            for (x, tasks) in &res {
                for task in tasks {
                    map.get_mut(task).unwrap().insert(*x);
                }
            }
        }
        println!("time cost in one turn: {}", time);
        for (task, set) in &map {
            let keys = &vec_keys[(*task) as usize];
            assert_eq!(keys, set);
        }

    }

    #[test]
    fn test_bm_sampler() {
        // log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
        sample(16);
    }

    fn sample(tasks: u64) {
        let mut sampler = SamplerTree::new();
        let mut rng = rand::thread_rng();
        let mut vec_keys = Vec::<HashSet<u32>>::new();
        let mut map: HashMap<u64, HashSet<u32>> = HashMap::new();

        // let sizes = [1, 2, 4, 8, 16, 32];
        for id in 0..tasks {
            let size = rng.gen_range(100000..1000000);
            // let size = sizes[id as usize];
            let keys = (0..size).into_iter().collect::<Vec<u32>>();
            vec_keys.push(HashSet::from_iter(keys.iter().cloned()));
            sampler.insert(keys, id);
            map.insert(id, HashSet::new());
        }

        let mut time;
        loop {
            let now = Instant::now();
            sampler.clear_loader();
            let res = sampler.sample(&HashSet::new());
            time = now.elapsed().as_secs_f32();
            if res.is_empty() {
                break;
            }
            for (x, tasks) in &res {
                for task in tasks {
                    map.get_mut(task).unwrap().insert(*x);
                }
            }
        }
        println!("time cost in one turn: {}", time);
        for (task, set) in &map {
            let keys = &vec_keys[(*task) as usize];
            assert_eq!(keys, set);
        }
    }
    #[test]
    fn test_insert() {
        insert(16);
    }
    fn insert(tasks: u32) {
        let mut sampler = SamplerTree::new();
        let mut rng = rand::thread_rng();
        let mut vec_keys = Vec::<Vec<u32>>::new();

        for _i in 0..tasks {
            let size = rng.gen_range(500000..2000000);
            let keys = (0..size).into_iter().collect();
            vec_keys.push(keys);
        }

        let vec_tasks = Vec::new();
        for (idx, keys) in vec_keys.iter().enumerate() {
            let now = Instant::now();
            sampler.insert(keys.clone(), idx as u64);
            println!("inserting {:} elements costs {:}", keys.len() ,now.elapsed().as_secs_f32());
        }

        for task in vec_tasks {
            let mut values = sampler.get_task_values(task);
            values.sort();
            let mut keys = vec_keys[task as usize].clone();
            keys.sort();
            assert!(values.eq(&keys));
        }
    }

    #[test]
    fn test_delete_sampler() {
        let mut sampler = SamplerTree::new();
        let mut vec_keys = Vec::<HashSet<u32>>::new();
        let mut map: HashMap<u64, HashSet<u32>> = HashMap::new();
        let tasks_sizes = [2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096];
        let delete_id = [0, 3, 5];
        for id in 0..tasks_sizes.len() {
            let size = tasks_sizes[id as usize];
            let keys = (0..size).into_iter().collect::<Vec<u32>>();
            vec_keys.push(HashSet::from_iter(keys.iter().cloned()));
            sampler.insert(keys, id as u64);
            if !delete_id.contains(&id) {
                map.insert(id as u64, HashSet::new());
            }
        }

        for id in delete_id {
            sampler.delete(id as u64);
        }

        let mut time;
        loop {
            let now = Instant::now();
            let res = sampler.sample(&HashSet::new());
            time = now.elapsed().as_secs_f32();
            if res.is_empty() {
                break;
            }
            for (x, tasks) in &res {
                for task in tasks {
                    map.get_mut(task).unwrap().insert(*x);
                }
            }
        }
        println!("time cost in one turn: {}", time);
        for (task, set) in &map {
            let keys = &vec_keys[(*task) as usize];
            assert_eq!(keys, set);
        }
    }
}
