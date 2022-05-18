use super::{decision::Decision, values_set::ValueSet};
use rand::{distributions::WeightedIndex, prelude::Distribution, thread_rng};
use std::{collections::HashSet, iter::FromIterator, sync::Arc};
#[derive(Clone, Debug)]
pub struct Node {
    values_set: ValueSet,
    // The LoaderId set which hold the data in the Node
    loader_id: HashSet<u64>,
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

    pub fn new(values: Vec<u32>, loader_id: HashSet<u64>) -> NodeRef {
        Arc::new(Node {
            values_set: ValueSet::init(values.len()),
            loader_id,
            left: None,
            right: None,
        })
    }

    pub fn get_loader_id(&self) -> &HashSet<u64> {
        &self.loader_id
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
        let loader_id = self
            .loader_id
            .union(&other.loader_id)
            .cloned()
            .collect::<HashSet<_>>();
        self.values_set = self.values_set.difference(&values_set);
        other.values_set = other.values_set.difference(&values_set);
        Arc::new(Node {
            values_set,
            loader_id,
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

    pub fn get_loader_values(&self, loader_id: u64) -> Vec<u32> {
        let mut res = Vec::<u32>::new();
        if self.loader_id.contains(&loader_id) {
            res.append(&mut self.values_set.as_vec());
            if let Some(left) = &self.left {
                let mut left_v = left.get_loader_values(loader_id);
                res.append(&mut left_v);
            }
            if let Some(right) = &self.right {
                let mut right_v = right.get_loader_values(loader_id);
                res.append(&mut right_v);
            }
        }
        res
    }

    pub fn get_loader_set(&self, loader_set: &mut Vec<(u64, usize)>, mut pre_len: usize) {
        pre_len += self.len();
        loader_set.push((*self.loader_id.iter().next().unwrap(), pre_len));
        if let Some(right) = &self.right {
            let left = self.left.as_ref().unwrap();
            loader_set.pop();
            loader_set.push((
                *left.get_loader_id().iter().next().unwrap(),
                pre_len + left.len(),
            ));
            right.get_loader_set(loader_set, pre_len);
        }
    }

    pub fn delete(self: &mut NodeRef, id: u64) -> Option<NodeRef> {
        let mut_ref = self.get_mut_unchecked();
        mut_ref.loader_id.remove(&id);
        if mut_ref.loader_id.is_empty() {
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
        loaders: &mut Vec<(u64, usize)>,
        decisions: &mut Vec<Decision>,
        mut node_set: Vec<NodeRef>,
    ) {
        if loaders.is_empty() {
            return;
        }
        if self.len() != 0 {
            node_set.push(self.clone());
        }

        // push down and add self in node set
        let loader_id: HashSet<_> = HashSet::from_iter(loaders.iter().map(|(id, _)| *id));
        if !self.loader_id.eq(&loader_id) {
            if let Some(mut right) = self.right.clone() {
                right.decide(loaders, decisions, node_set);
            }
            return;
        }

        let common = node_set.iter().fold(0, |x, n| x + n.len());
        let mut last_common = common;
        let loaders_cloned = loaders.clone();
        let mut decided_loader = HashSet::new();
        for (id, len) in loaders_cloned.iter().cloned() {
            if random_probility() >= (last_common as f32) / (len as f32) {
                break;
            }
            //choose current node
            last_common = len;
            decided_loader.insert(id);
            loaders.remove(0);
        }

        if decided_loader.is_empty() {
            //The first task choose diff
            let mut loader_set = HashSet::new();
            loader_set.insert(loaders[0].0);
            log::trace!(
                "Dicide: {:?} decide node [{:?}, {:?}]",
                loader_set,
                self.left.as_ref().unwrap().get_loader_id(),
                self.left.as_ref().unwrap().values_set.as_vec(),
            );
            loaders.remove(0);
            let decision = Decision::new(self.left.clone().unwrap(), loader_set);
            decisions.push(decision);
        } else {
            // Some tasks choose intersection
            self.choose_intersection(decisions, decided_loader, &node_set);
        }

        if !loaders.is_empty() {
            for (_, len) in loaders.iter_mut() {
                *len -= common;
            }
            // Other tasks push down right child
            if let Some(mut right) = self.right.clone() {
                right.decide(loaders, decisions, vec![])
            }
        }
    }

    fn choose_intersection(
        self: &mut NodeRef,
        decisions: &mut Vec<Decision>,
        loader_set: HashSet<u64>,
        node_set: &Vec<NodeRef>,
    ) {
        let weights = node_set.iter().map(|x| x.len()).collect::<Vec<_>>();
        if weights.iter().sum::<usize>() == 0 {
            return;
        }
        let intersection = node_set[random_weight(&weights)].clone();
        log::trace!(
            "Dicide: {:?} decide node [{:?}, {:?}]",
            loader_set,
            intersection.get_loader_id(),
            intersection.values_set.as_vec()
        );
        let decision = Decision::new(intersection, loader_set);
        decisions.push(decision);
    }

    pub fn random_choose(&mut self, loader_ids: &HashSet<u64>) -> (u32, HashSet<u64>) {
        let choice_item = self.values_set.random_pick();
        log::trace!(
            "Choose: {:?} choose {:} from node [{:?}]",
            loader_ids,
            choice_item,
            self.loader_id,
        );
        let compensation: HashSet<_> =
            HashSet::from_iter(self.loader_id.difference(loader_ids).cloned());

        (choice_item, compensation)
    }

    pub fn complent(&mut self, comp: &mut HashSet<u64>, item: u32) -> bool {
        if comp.is_empty() {
            return false;
        }
        if self.loader_id.is_subset(comp) {
            // We should complent in next turn to avoild sample it in this turn
            self.values_set.set(item as u32);
            log::trace!(
                "Complent: {:?} in node [{:?}] with compset {:?}",
                item,
                self.loader_id,
                comp
            );
            for task in &self.loader_id {
                comp.remove(task);
            }
        }
        let mut res = false;
        if let (Some(left), Some(right)) = (&mut self.left, &mut self.right) {
            let l = left.get_mut_unchecked();
            res |= l.complent(comp, item);
            let r = right.get_mut_unchecked();
            res |= r.complent(comp, item);
            log::debug!("{:?} len: {}, {:?} len: {}", l.get_loader_id(), l.min_task_length(), r.get_loader_id(), r.min_task_length());
            if l.min_task_length() > r.min_task_length() {
                res = true;
                match (&mut r.left, &mut r.right) {
                    (Some(rl), Some(_)) => {
                        let lid_set = l.loader_id.clone();
                        let lvs = l.values_set.clone();
                        l.values_set = r.values_set.union(&rl.values_set);
                        l.loader_id = rl.loader_id.clone();
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

fn remake(node: &mut Node, new_vs: ValueSet, new_loader_id: HashSet<u64>) {
    match (&mut node.left, &mut node.right) {
        (Some(left), Some(right)) => {
            let l = left.get_mut_unchecked();
            let r = right.get_mut_unchecked();
            log::debug!("swap {:?} {:?}", l.get_loader_id(), r.get_loader_id());
            for lid in &l.loader_id {
                node.loader_id.remove(lid);
            }
            for id in &new_loader_id {
                node.loader_id.insert(*id);
            }
            let diff = node.values_set.difference(&new_vs);
            node.values_set = node.values_set.intersection(&new_vs);
            l.values_set = new_vs.difference(&node.values_set);
            l.loader_id = new_loader_id;
            r.values_set = r.values_set.union(&diff);
        }
        _ => unreachable!(),
    }
}
