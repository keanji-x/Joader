use super::{decision::Decision, values_set::ValueSet};
use rand::{distributions::WeightedIndex, prelude::Distribution, thread_rng};
use std::{collections::HashSet, iter::FromIterator, sync::Arc};
#[derive(Clone, Debug)]
pub struct Node {
    values_set: ValueSet,
    // The LoaderId set which hold the data in the Node
    job_id: HashSet<u64>,
    // The left is smaller task, and the right is larger
    left: Option<NodeRef>,
    right: Option<NodeRef>,
}

#[inline]
fn random_probility() -> f32 {
    rand::random::<f32>()
}

fn random_weight(weights: &[usize]) -> usize {
    let dist = WeightedIndex::new(weights).unwrap();
    dist.sample(&mut thread_rng())
}
pub type NodeRef = Arc<Node>;

impl Node {
    #[inline]
    pub fn get_mut_unchecked<'a>(self: &'a mut NodeRef) -> &'a mut Self {
        unsafe { Arc::get_mut_unchecked(self) }
    }

    pub fn new(values: Vec<u32>, job_id: HashSet<u64>) -> NodeRef {
        let mut values_set = ValueSet::new();
        for v in values {
            values_set.set(v);
        }
        Arc::new(Node {
            values_set,
            job_id,
            left: None,
            right: None,
        })
    }

    pub fn get_job_id(&self) -> &HashSet<u64> {
        &self.job_id
    }

    fn len(&self) -> usize {
        return self.values_set.len();
    }

    fn min_task_length(&self) -> usize {
        let mut l = self.len();
        if let Some(left) = &self.left {
            l += left.len();
        }
        l
    }

    fn intersect_update(&mut self, other: &mut Node) -> NodeRef {
        let values_set = self.values_set.intersection(&other.values_set);
        let job_id = self
            .job_id
            .union(&other.job_id)
            .cloned()
            .collect::<HashSet<_>>();
        self.values_set = self.values_set.difference(&values_set);
        other.values_set = other.values_set.difference(&values_set);
        Arc::new(Node {
            values_set,
            job_id,
            left: None,
            right: None,
        })
    }

    fn pushdown(&mut self) -> (Option<NodeRef>, Option<NodeRef>) {
        let mut left = self.left.clone().unwrap();
        let mut right = self.right.clone().unwrap();
        let l = left.get_mut_unchecked();
        l.values_set = l.values_set.union(&self.values_set);
        let r = right.get_mut_unchecked();
        r.values_set = r.values_set.union(&self.values_set);
        return (Some(left), Some(right));
    }

    pub fn insert(self: &mut NodeRef, mut other: NodeRef) -> NodeRef {
        let mut new_root;
        let root_ref = self.get_mut_unchecked();
        if other.len() <= root_ref.min_task_length() {
            new_root = root_ref.intersect_update(other.get_mut_unchecked());
            let mut new_root_ref = new_root.get_mut_unchecked();
            new_root_ref.left = Some(other.clone());
            new_root_ref.right = Some(self.clone());
        } else {
            new_root = root_ref.intersect_update(other.get_mut_unchecked());
            let mut new_root_ref = new_root.get_mut_unchecked();
            if let None = root_ref.left {
                new_root_ref.left = Some(self.clone());
                new_root_ref.right = Some(other.clone());
            } else {
                let (left_tree, right_tree) = Node::pushdown(root_ref);
                new_root_ref.left = left_tree;
                new_root_ref.right = Some(right_tree.unwrap().insert(other));
            }
        }
        return new_root;
    }

    pub fn get_job_values(&self, job_id: u64) -> Vec<u32> {
        let mut res = Vec::<u32>::new();
        if self.job_id.contains(&job_id) {
            res.append(&mut self.values_set.as_vec());
            if let Some(left) = &self.left {
                let mut left_v = left.get_job_values(job_id);
                res.append(&mut left_v);
            }
            if let Some(right) = &self.right {
                let mut right_v = right.get_job_values(job_id);
                res.append(&mut right_v);
            }
        }
        res
    }

    pub fn get_job_set(&self, job_set: &mut Vec<(u64, usize)>, mut pre_len: usize) {
        pre_len += self.len();
        job_set.push((*self.job_id.iter().next().unwrap(), pre_len));
        if let Some(right) = &self.right {
            let left = self.left.as_ref().unwrap();
            job_set.pop();
            job_set.push((
                *left.get_job_id().iter().next().unwrap(),
                pre_len + left.len(),
            ));
            right.get_job_set(job_set, pre_len);
        }
    }

    pub fn delete(self: &mut NodeRef, id: u64) -> Option<NodeRef> {
        let mut_ref = self.get_mut_unchecked();
        mut_ref.job_id.remove(&id);
        if mut_ref.job_id.is_empty() {
            return None;
        }

        if let Some(left) = &mut mut_ref.left {
            mut_ref.left = left.delete(id);
        }

        if let Some(mut right) = mut_ref.right.clone() {
            if let None = mut_ref.left {
                mut_ref.values_set = mut_ref.values_set.union(&right.values_set);
                mut_ref.right = right.right.clone();
                mut_ref.left = right.left.clone();
            } else {
                mut_ref.right = right.delete(id);
            }
        }
        return Some((*self).clone());
    }
}

// sampling
impl Node {
    pub fn decide(
        self: &mut NodeRef,
        jobs: &mut Vec<(u64, usize)>,
        decisions: &mut Vec<Decision>,
        mut node_set: Vec<NodeRef>,
    ) {
        if jobs.is_empty() {
            return;
        }
        if self.len() != 0 {
            node_set.push(self.clone());
        }

        // push down and add self in node set
        let job_id: HashSet<_> = HashSet::from_iter(jobs.iter().map(|(id, _)| *id));
        if !self.job_id.eq(&job_id) {
            if let Some(mut right) = self.right.clone() {
                right.decide(jobs, decisions, node_set);
            }
            return;
        }

        let common = node_set.iter().fold(0, |x, n| x + n.len());
        let mut last_common = common;
        let jobs_cloned = jobs.clone();
        let mut decided_loader = HashSet::new();
        for (id, len) in jobs_cloned.iter().cloned() {
            if random_probility() >= (last_common as f32) / (len as f32) {
                break;
            }
            //choose current node
            last_common = len;
            decided_loader.insert(id);
            jobs.remove(0);
        }

        if decided_loader.is_empty() {
            //The first task choose diff
            let mut job_set = HashSet::new();
            job_set.insert(jobs[0].0);
            log::trace!(
                "Dicide: {:?} decide node [{:?}, {:?}]",
                job_set,
                self.left.as_ref().unwrap().get_job_id(),
                self.left.as_ref().unwrap().values_set.as_vec(),
            );
            jobs.remove(0);
            let decision = Decision::new(self.left.clone().unwrap(), job_set);
            decisions.push(decision);
        } else {
            // Some tasks choose intersection
            self.choose_intersection(decisions, decided_loader, &node_set);
        }

        if !jobs.is_empty() {
            for (_, len) in jobs.iter_mut() {
                *len -= common;
            }
            // Other tasks push down right child
            if let Some(mut right) = self.right.clone() {
                right.decide(jobs, decisions, vec![])
            }
        }
    }

    fn choose_intersection(
        self: &mut NodeRef,
        decisions: &mut Vec<Decision>,
        job_set: HashSet<u64>,
        node_set: &Vec<NodeRef>,
    ) {
        let weights = node_set.iter().map(|x| x.len()).collect::<Vec<_>>();
        if weights.iter().sum::<usize>() == 0 {
            return;
        }
        let intersection = node_set[random_weight(&weights)].clone();
        log::trace!(
            "Dicide: {:?} decide node [{:?}, {:?}]",
            job_set,
            intersection.get_job_id(),
            intersection.values_set.as_vec()
        );
        let decision = Decision::new(intersection, job_set);
        decisions.push(decision);
    }

    pub fn random_choose(&mut self, job_ids: &HashSet<u64>) -> (u32, HashSet<u64>) {
        let choice_item = self.values_set.random_pick();
        log::trace!(
            "Choose: {:?} choose {:} from node [{:?}]",
            job_ids,
            choice_item,
            self.job_id,
        );
        let compensation: HashSet<_> =
            HashSet::from_iter(self.job_id.difference(job_ids).cloned());

        (choice_item, compensation)
    }

    pub fn complent(&mut self, comp: &mut HashSet<u64>, item: u32) -> bool {
        if comp.is_empty() {
            return false;
        }
        if self.job_id.is_subset(comp) {
            // We should complent in next turn to avoild sample it in this turn
            self.values_set.set(item as u32);
            log::trace!(
                "Complent: {:?} in node [{:?}] with compset {:?}",
                item,
                self.job_id,
                comp
            );
            for task in &self.job_id {
                comp.remove(task);
            }
        }
        let mut res = false;
        if let (Some(left), Some(right)) = (&mut self.left, &mut self.right) {
            let l = left.get_mut_unchecked();
            res |= l.complent(comp, item);
            let r = right.get_mut_unchecked();
            res |= r.complent(comp, item);
            log::trace!(
                "{:?} len: {}, {:?} len: {}",
                l.get_job_id(),
                l.min_task_length(),
                r.get_job_id(),
                r.min_task_length()
            );
            if l.min_task_length() > r.min_task_length() {
                res = true;
                match (&mut r.left, &mut r.right) {
                    (Some(rl), Some(_)) => {
                        let lid_set = l.job_id.clone();
                        let lvs = l.values_set.clone();
                        l.values_set = r.values_set.union(&rl.values_set);
                        l.job_id = rl.job_id.clone();
                        remake(r, lvs, lid_set);
                    }
                    (None, None) => {
                        let temp = self.left.clone();
                        self.left = self.right.clone();
                        self.right = temp;
                    }
                    _ => unreachable!(),
                }
            }
        }
        // if remake, we need to reload task_set
        res
    }
}

fn remake(node: &mut Node, new_vs: ValueSet, new_job_id: HashSet<u64>) {
    match (&mut node.left, &mut node.right) {
        (Some(left), Some(right)) => {
            let l = left.get_mut_unchecked();
            let r = right.get_mut_unchecked();
            log::trace!("swap {:?} {:?}", l.get_job_id(), r.get_job_id());
            for lid in &l.job_id {
                node.job_id.remove(lid);
            }
            for id in &new_job_id {
                node.job_id.insert(*id);
            }
            let diff = node.values_set.difference(&new_vs);
            node.values_set = node.values_set.intersection(&new_vs);
            l.values_set = new_vs.difference(&node.values_set);
            l.job_id = new_job_id;
            r.values_set = r.values_set.union(&diff);
        }
        _ => unreachable!(),
    }
}
