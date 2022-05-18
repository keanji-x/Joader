use crate::cache::head::Head;
use crate::cache::head::HEAD_SIZE;

#[derive(Debug)]
pub struct HeadSegment {
    head_segment: Vec<Head>,
    // Record the ref cnt of each data in the sampling tree, 64 level
    ref_table: Vec<Vec<usize>>,
}

impl HeadSegment {
    pub fn new(ptr: *mut u8, head_num: u64) -> HeadSegment {
        let mut head_segment = Vec::new();
        for i in 0..head_num {
            head_segment.push(unsafe { ptr.offset((i * HEAD_SIZE) as isize).into() })
        }
        HeadSegment {
            head_segment,
            // there are 64 level
            ref_table: vec![Vec::new(); 64],
        }
    }

    pub fn size(&self) -> u64 {
        (self.head_segment.len() as u64) * HEAD_SIZE
    }

    pub fn mark_unreaded(&mut self, idx: usize, loader_cnt: usize) {
        self.head_segment[idx].set_unread(loader_cnt);
    }

    pub fn allocate(&mut self, ref_cnt: usize) -> Option<(Head, usize)> {
        assert!(ref_cnt < 64);
        for (idx, head) in self.head_segment.iter_mut().enumerate() {
            if head.is_free() {
                self.ref_table[ref_cnt].push(idx);
                log::debug!(
                    "Allocate head {:?}: {:?}{:?}",
                    idx,
                    head.is_readed(),
                    head.get()
                );
                head.allocated();
                return Some((head.clone(), idx));
            }
        }
        None
    }

    // only free the unvalid head
    // travel from the lowest level, if all table is valid. return None
    pub fn free(&mut self) -> Option<Vec<(Head, usize)>> {
        let mut ret = Vec::new();
        for heads in self.ref_table.iter_mut() {
            if heads.len() == 0 {
                continue;
            }
            let mut heads_clone = heads.clone();
            heads.clear();
            for idx in heads_clone.iter_mut() {
                let head = &mut self.head_segment[*idx];
                if head.is_readed() {
                    log::debug!("Free head {:?} {:?}{:?}", idx, head.is_readed(), head.get());
                    head.set_free();
                    ret.push((head.clone(), *idx));
                } else {
                    heads.push(*idx);
                };
            }
            if ret.len() != 0 {
                return Some(ret);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let mut bytes = [0u8; 1024 * 17];
        let mut hs = HeadSegment::new(bytes.as_mut_ptr(), 1024);
        for i in 0..1024 {
            let (mut head, _) = hs.allocate(0).unwrap();
            head.set(i, i as u64, 0);
        }
        for i in 0..1024 {
            let head: Head = bytes[i * HEAD_SIZE as usize..(i + 1) * HEAD_SIZE as usize]
                .as_mut_ptr()
                .into();
            let (readed, len, off) = head.get();
            assert!(readed);
            assert_eq!(off, i as u64);
            assert_eq!(len, i as u32);
        }
    }
}
