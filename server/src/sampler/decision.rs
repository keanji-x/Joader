use super::sampler_node::NodeRef;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

#[derive(Clone)]
pub struct Decision {
    node: NodeRef,
    loader_ids: HashSet<u64>,
    compensation: HashSet<u64>,
    item: u32,
}

impl Hash for Decision {
    fn hash<H: Hasher>(&self, _state: &mut H) {
        self.node.get_loader_id().hasher();
    }
}

impl PartialEq for Decision {
    fn eq(&self, other: &Self) -> bool {
        self.node.get_loader_id() == other.node.get_loader_id()
    }
}

impl Eq for Decision {}

impl Decision {
    pub fn new(node: NodeRef, loader_ids: HashSet<u64>) -> Self {
        Self {
            node,
            loader_ids,
            compensation: HashSet::new(),
            item: 0,
        }
    }

    pub fn execute(&mut self) -> u32 {
        let mut_ref = self.node.get_mut_unchecked();
        let (ret, comp) = mut_ref.random_choose(self.loader_ids.clone());
        self.compensation = comp;
        self.item = ret;
        ret
    }

    pub fn complent(&mut self) {
        if self.compensation.is_empty() {
            return;
        }
        self.node
            .get_mut_unchecked()
            .complent(&mut self.compensation, self.item);
    }

    pub fn get_loaders(&self) -> HashSet<u64> {
        self.loader_ids.clone()
    }
}
