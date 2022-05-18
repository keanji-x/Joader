use rand::Rng;

use std::collections::{HashMap, HashSet};

#[derive(Clone, Default, Debug)]
pub struct SamplerTree {
    root: HashMap<u64, Vec<u32>>,
    // (job_id, loader size)
    job_set: Vec<(u64, usize)>,
}

fn random_choose(data: &mut Vec<u32>) -> u32 {
    let mut rng = rand::thread_rng();
    let rand_idx = rng.gen_range(0usize..data.len());
    let last_idx = data.len() - 1;

    let v = data[rand_idx];
    data[rand_idx] = data[last_idx];
    data.pop();
    return v;
}

impl SamplerTree {
    pub fn new() -> Self {
        SamplerTree {
            root: HashMap::new(),
            job_set: Vec::new(),
        }
    }

    pub fn insert(&mut self, indices: Vec<u32>, id: u64) {
        self.job_set.push((id, indices.len()));
        self.root.insert(id, indices);
    }

    pub fn delete(&mut self, id: u64) {
        todo!()
    }

    pub fn sample(&mut self, mask: &HashSet<u64>) -> HashMap<u32, HashSet<u64>> {
        let mut res = HashMap::new();
        for (id, size) in self.job_set.iter_mut() {
            if mask.contains(id) || *size == 0 {
                continue;
            }
            *size -= 1;
            let sample_res = random_choose(&mut self.root.get_mut(id).unwrap());
            if !res.contains_key(&sample_res) {
                res.insert(sample_res, HashSet::new());
            }
            res.get_mut(&sample_res).unwrap().insert(*id);
            
        }
        res
    }

    pub fn sample_with_buffer(&mut self, mask: &HashSet<u64>) -> HashMap<u32, HashSet<u64>> {
        let mut res = HashMap::new();
        
        for (id, size) in self.job_set.iter_mut() {
            if mask.contains(id) || *size == 0 {
                continue;
            }
            *size -= 1;
            let sample_res = random_choose(&mut self.root.get_mut(id).unwrap());
            let mut v = HashSet::new();
            v.insert(*id);
            res.insert(sample_res, v);
        }
        res
    }



    pub fn is_empty(&self) -> bool {
        let mut capacity = 0;
        for job in &self.job_set {
            capacity += job.1;
        }
        capacity != 0
    }

    pub fn get_job_values(&self, job_id: u64) -> Vec<u32> {
        Vec::default()
    }
}
