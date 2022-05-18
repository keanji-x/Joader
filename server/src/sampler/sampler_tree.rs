use super::sampler_node::{Node, NodeRef};
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Clone, Default, Debug)]
pub struct SamplerTree {
    root: Option<NodeRef>,
    // (job_id, loader size)
    job_set: Vec<(u64, usize)>,
    job_buffer: HashMap<u64, VecDeque<u32>>
}

impl SamplerTree {
    pub fn new() -> Self {
        SamplerTree {
            root: None,
            job_set: Vec::new(),
            job_buffer: HashMap::new(),
        }
    }

    pub fn insert(&mut self, indices: Vec<u32>, id: u64) {
        log::debug!("Sampler insert {:?} data {:?}", indices.len(), id);
        let mut job_set = HashSet::new();
        job_set.insert(id);
        let node = Node::new(indices, job_set);
        if let Some(mut root) = self.root.clone() {
            self.root = Some(root.insert(node));
        } else {
            self.root = Some(node);
        }
        self.job_set.clear();
        // keep order
        self.root
            .clone()
            .unwrap()
            .get_job_set(&mut self.job_set, 0);
        self.job_buffer.insert(id, VecDeque::new());
    }

    pub fn delete(&mut self, id: u64) {
        log::debug!("Del Sampler {}", id);
        if let Some(root) = &mut self.root {
            self.root = root.delete(id);
        }
        self.job_set.clear();
        if let Some(root) = &self.root {
            root.get_job_set(&mut self.job_set, 0);
        }
        log::debug!("Del Sampler {} finish.....", id);
    }

    pub fn get_task_values(&self, job_id: u64) -> Vec<u32> {
        if let Some(root) = &self.root {
            return root.get_job_values(job_id);
        }
        Vec::new()
    }

    pub fn clear_loader(&mut self) -> Vec<u64> {
        let mut new_job_set = Vec::new();
        let mut del_loader = Vec::new();
        for loader in &self.job_set {
            if loader.1 == 0 {
                del_loader.push(loader.0);
            } else {
                new_job_set.push(loader.clone());
            }
        }
        if let Some(mut root) = self.root.clone() {
            for id in &del_loader {
                self.root = root.delete(*id);
            }
        }

        self.job_set = new_job_set;
        del_loader
    }

    pub fn sample(&mut self, mask: &HashSet<u64>) -> HashMap<u32, HashSet<u64>> {
        let mut jobs = Vec::new();
        for loader in &self.job_set {
            if loader.1 != 0 {
                jobs.push(loader.clone())
            }
        }
        log::debug!("Sampler sample {:?}", jobs);
        let mut decisions = Vec::new();
        let mut res = HashMap::<u32, HashSet<u64>>::new();
        match self.root.clone() {
            Some(mut root) => root.decide(&mut jobs, &mut decisions, vec![]),
            None => return res,
        }

        
        for decision in decisions.iter_mut() {
            let ret = decision.execute(mask);
            if let Some(job_set) = res.get_mut(&ret) {
                for loader in decision.get_jobs() {
                    job_set.insert(loader);
                }
            } else {
                if !decision.get_jobs().is_empty() {
                    res.insert(ret, decision.get_jobs());
                }
            }
        }

        let mut reload = false;
        for decision in decisions.iter_mut() {
            reload |= decision.complent(self.root.clone().unwrap());
        }
        for (id, len) in self.job_set.iter_mut() {
            if !mask.contains(id) && *len != 0 {
                *len -= 1;
            }
        }
        if reload {
            self.job_set.clear();
            if let Some(root) = &self.root {
                root.get_job_set(&mut self.job_set, 0);
            }
        }
        self.clear_loader();
        log::debug!("Sampler get {:?}", res);
        res
    }

    pub fn sample_with_buffer(&mut self, mask: &HashSet<u64>) -> HashMap<u32, HashSet<u64>> {
        // get the kv job_id: sample
        let mut sampling_res_table = HashMap::new();
        let mut jobs = Vec::new();
        let mut sample_job = HashSet::new();
        for job in &self.job_set {
            jobs.push(job.clone());
            sample_job.insert(job.0);
        }
        for (job_id, buffer) in self.job_buffer.iter_mut() {
            if !mask.contains(job_id) && !sample_job.contains(job_id) {
                match buffer.pop_front() {
                    Some(v) => sampling_res_table.insert(*job_id, v),
                    _ => continue,
                };
            }
        }
        log::debug!("Sampler sample {:?} with buffer {:?}", jobs, self.job_buffer);
        let mut decisions = Vec::new();
        let mut res = HashMap::<u32, HashSet<u64>>::new();
        match self.root.clone() {
            Some(mut root) => root.decide(&mut jobs, &mut decisions, vec![]),
            None => (),
        }

        for decision in decisions.iter_mut() {
            let ret = decision.execute(&HashSet::new());
            for job_id in decision.get_jobs() {
                let buffer = self.job_buffer.get_mut(&job_id).unwrap();
                buffer.push_back(ret);
                if !mask.contains(&job_id) {
                    sampling_res_table.insert(job_id, buffer.pop_front().unwrap());
                }
            }
        }
        // reverser to sample_res : job_set
        for (k,v) in sampling_res_table.iter() {
            res.entry(*v).and_modify(|s| {s.insert(*k);}).or_insert(HashSet::from([*k]));
        }

        for (_, len) in self.job_set.iter_mut() {
            *len -= 1;
        }
        self.clear_loader();
        log::debug!("Sampler get {:?}", res);
        res
    }

    pub fn is_empty(&self) -> bool {
        let mut capacity = 0;
        for job in &self.job_set {
            capacity += job.1 + self.job_buffer[&job.0].len();
        }
        capacity != 0
    }

    pub fn get_job_values(&self, job_id: u64) -> Vec<u32> {
        if let Some(root) = self.root.as_ref() {
            return root.get_job_values(job_id);
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
