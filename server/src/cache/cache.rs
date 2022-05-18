use crate::cache::cached_data::CachedData;
use crate::cache::data_segment::DataSegment;
use crate::cache::head_segment::HeadSegment;
use core::time;
use libc::{ftruncate, mmap, shm_open};
use libc::{off_t, shm_unlink};
use libc::{MAP_SHARED, O_CREAT, O_RDWR, PROT_WRITE, S_IRUSR, S_IWUSR};
use std::slice::from_raw_parts_mut;
use std::time::Duration;
use std::{ptr, thread, usize};

use super::data_block::{Data, DataBlock};
use super::head::Head;

#[derive(Debug)]
pub struct Cache {
    shmpath: String,
    head_segment: HeadSegment,
    data_segment: DataSegment,
    cached_data: CachedData,
    start_ptr: *mut u8,
    sleep_iterver: Duration,
}
unsafe impl Send for Cache {}

impl Cache {
    pub fn new(capacity: usize, shmpath: &str, head_num: u64) -> Cache {
        let (_, addr) = unsafe {
            let shmpath = shmpath.as_ptr() as *const i8;
            let fd = shm_open(shmpath, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
            let _res = ftruncate(fd, capacity as off_t);
            let addr = mmap(ptr::null_mut(), capacity, PROT_WRITE, MAP_SHARED, fd, 0);
            // Todo(xj): It's just avoid empty file, we should add a magic code in the front of the file
            *(addr as *mut u8).offset(3) = 5u8;
            (fd, addr as *mut u8)
        };
        let head_segment = HeadSegment::new(addr, head_num);
        let data_segment = unsafe {
            DataSegment::new(
                addr.offset(head_segment.size() as isize),
                head_segment.size(),
                capacity as u64 - head_segment.size(),
            )
        };

        Cache {
            shmpath: shmpath.to_string(),
            head_segment,
            data_segment,
            start_ptr: addr,
            cached_data: CachedData::new(),
            sleep_iterver: time::Duration::from_secs_f32(0.001),
        }
    }

    fn free(&mut self) {
        if let Some(mut unvalid_heads) = self.head_segment.free() {
            for (head, idx) in unvalid_heads.iter_mut() {
                self.cached_data.remove(*idx);
                let len = head.get_len();
                let off = head.get_off();
                self.data_segment.free(off, len as u64);
            }
        }
    }

    pub fn free_block(&mut self, mut block: DataBlock) {
        // the head is lazy copied
        self.data_segment
            .free(block.data().off(), block.data().len());
    }

    pub fn allocate(
        &mut self,
        len: usize,
        ref_cnt: usize,
        data_id: u64,
        loader_cnt: usize,
    ) -> (&'static mut [u8], usize) {
        // allocate data can cause gc, we should first allocate data
        let data = self.allocate_data(len as u64);
        let (head, idx) = self.allocate_head(ref_cnt);
        self.cached_data.add(idx, data_id);
        let mut block = DataBlock::new(head, data, loader_cnt);
        let ptr = block.data().as_mut_ptr();
        log::debug!(
            "allocate head_idx:{}, data:{} [{},{}), loader_cnt:{}",
            idx,
            data.len(),
            data.off(),
            data.off() + data.len(),
            loader_cnt
        );
        return (unsafe { from_raw_parts_mut(ptr, len) }, idx);
    }

    fn allocate_data(&mut self, request_len: u64) -> Data {
        // This function return a data or loop
        // Todo(xj): better free method
        let mut data = self.data_segment.allocate(request_len);
        if let Some(data) = data {
            assert_eq!(data.len(), request_len);
            return data;
        }
        loop {
            self.free();
            data = self.data_segment.allocate(request_len);
            if let Some(data) = data {
                assert_eq!(data.len(), request_len);
                return data;
            }
            thread::sleep(self.sleep_iterver);
            log::debug!("Loop in allocate data");
        }
    }

    fn allocate_head(&mut self, ref_cnt: usize) -> (Head, usize) {
        let mut ret = self.head_segment.allocate(ref_cnt);
        if let Some((head, idx)) = ret {
            return (head, idx);
        }
        loop {
            self.free();
            ret = self.head_segment.allocate(ref_cnt);
            if let Some((head, idx)) = ret {
                return (head, idx);
            }
            thread::sleep(self.sleep_iterver);
            log::debug!("Loop in allocate head");
        }
    }

    pub fn contains_data(&self, data_id: u64) -> Option<usize> {
        self.cached_data.contains(data_id)
    }

    pub fn mark_unreaded(&mut self, head_idx: usize, loader_cnt: usize) {
        self.head_segment.mark_unreaded(head_idx, loader_cnt);
    }

    pub fn start_ptr(&self) -> *mut u8 {
        self.start_ptr.clone()
    }

    pub fn close(shmpath: String) {
        unsafe {
            let shmpath = shmpath.as_ptr() as *const i8;
            shm_unlink(shmpath);
        }
    }

    pub fn get_shm_path(&self) -> &str {
        &self.shmpath
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::cache::head::{Head, HEAD_SIZE};
    use crossbeam::channel::{unbounded, Receiver, Sender};
    use std::{slice::from_raw_parts, sync::atomic::AtomicPtr, time::SystemTime};
    #[test]
    fn single_thread_test() {
        log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
        let head_num = 8;
        let len = head_num * HEAD_SIZE + 1024;
        let name = "DLCache".to_string();

        let mut cache = Cache::new(len as usize, &name, head_num);

        let size_list = &[(20, 0), (27, 1), (60, 2), (20, 3)];
        let mut idx_list = vec![];
        for (idx, (size, ref_cnt)) in size_list.iter().enumerate() {
            let idx = write(&mut cache, *size, *ref_cnt, 7, idx as u64);
            idx_list.push(idx);
        }
        for ((size, _), off) in size_list.iter().zip(idx_list.iter()) {
            let data = read(*off, cache.start_ptr(), 7);
            assert_eq!(data.len(), *size);
        }

        // some data should be free
        let size_list = &[40, 38];
        let mut idx_list = vec![];
        for (idx, size) in size_list.iter().enumerate() {
            let idx = write(&mut cache, *size, size % 2, 3, idx as u64);
            idx_list.push(idx);
        }
        for (size, off) in size_list.iter().zip(idx_list.iter()) {
            let data = read(*off, cache.start_ptr(), 3);
            assert_eq!(data.len(), *size);
        }

        // some data should be free
        let size_list = &[127];
        let mut idx_list = vec![];
        for (idx, size) in size_list.iter().enumerate() {
            let idx = write(&mut cache, *size, size % 3, 5, idx as u64);
            idx_list.push(idx);
        }
        for (size, off) in size_list.iter().zip(idx_list.iter()) {
            let data = read(*off, cache.start_ptr(), 5);
            assert_eq!(data.len(), *size);
        }
        Cache::close(name);
    }

    #[test]
    fn two_thread_test() {
        log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
        const TURN: usize = 10000;
        let head_num: usize = 1024;
        let len = (HEAD_SIZE as usize) * ((head_num + TURN - 1) * HEAD_SIZE as usize);

        let name = "DLCache".to_string();
        let (wc, rc) = unbounded::<usize>();
        let (addr_wc, addr_rc) = unbounded();
        let writer = thread::spawn(move || {
            let cache = Cache::new(len, &name, head_num as u64);
            log::debug!("writer start {:?}", cache.start_ptr());
            addr_wc.send(AtomicPtr::new(cache.start_ptr())).unwrap();
            writer_func(cache, TURN, vec![wc]);
            log::debug!("write finish.......");
            thread::sleep(time::Duration::from_secs(5));
            Cache::close(name);
        });
        let reader = thread::spawn(move || {
            let mut start_ptr = addr_rc.recv().unwrap();
            log::debug!("reader start {:?}", *start_ptr.get_mut());
            reader_func(*start_ptr.get_mut(), TURN, rc, 1);
            println!("read finish.......");
        });
        reader.join().unwrap();
        writer.join().unwrap();
    }

    #[test]
    fn two_thread_test_with_two_reader() {
        log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
        const TURN: usize = 10000;
        let head_num: usize = 1024;
        let len = (HEAD_SIZE as usize) * ((head_num + TURN - 1) * HEAD_SIZE as usize);

        let name = "DLCache".to_string();
        let (wc1, rc1) = unbounded::<usize>();
        let (wc2, rc2) = unbounded::<usize>();
        let (addr_wc2, addr_rc2) = unbounded();
        let (addr_wc1, addr_rc1) = unbounded();
        let writer = thread::spawn(move || {
            let cache = Cache::new(len, &name, head_num as u64);
            log::debug!("writer start {:?}", cache.start_ptr());
            addr_wc1.send(AtomicPtr::new(cache.start_ptr())).unwrap();
            addr_wc2.send(AtomicPtr::new(cache.start_ptr())).unwrap();
            writer_func(cache, TURN, vec![wc1, wc2]);
            log::debug!("write finish.......");
            thread::sleep(time::Duration::from_secs(5));
            Cache::close(name);
        });
        let reader1 = thread::spawn(move || {
            let mut start_ptr = addr_rc1.recv().unwrap();
            log::debug!("reader start {:?}", *start_ptr.get_mut());
            reader_func(*start_ptr.get_mut(), TURN, rc1, 1);
            println!("read1 finish.......");
        });
        let reader2 = thread::spawn(move || {
            let mut start_ptr = addr_rc2.recv().unwrap();
            log::debug!("reader start {:?}", *start_ptr.get_mut());
            reader_func(*start_ptr.get_mut(), TURN, rc2, 2);
            println!("read2 finish.......");
        });
        reader1.join().unwrap();
        reader2.join().unwrap();
        writer.join().unwrap();
    }

    fn writer_func(mut cache: Cache, turn: usize, wcs: Vec<Sender<usize>>) {
        let mut start = SystemTime::now();
        for i in 1..turn {
            let len = i * HEAD_SIZE as usize;
            let idx = {
                let (block_slice, idx) = cache.allocate(len, i % 1, i as u64, wcs.len());
                assert_eq!(block_slice.len(), len);
                block_slice.copy_from_slice(vec![7u8; len].as_slice());
                idx
            };
            for wc in wcs.iter() {
                wc.send(idx).unwrap();
            }
            if i % 1000 == 0 {
                println!(
                    "write..{:} avg time: {:}",
                    i,
                    SystemTime::now().duration_since(start).unwrap().as_secs() as f64 / 1000 as f64
                );
                start = SystemTime::now();
            }
        }
        for wc in wcs.iter() {
            drop(wc);
        }
    }

    fn reader_func(start_ptr: *mut u8, turn: usize, rc: Receiver<usize>, read_off: usize) {
        let mut start = SystemTime::now();
        for i in 1..turn {
            if i % 1000 == 0 {
                println!(
                    "read..{:} avg time: {:}",
                    i,
                    SystemTime::now().duration_since(start).unwrap().as_secs() as f64 / 1000 as f64
                );
                start = SystemTime::now();
            }
            let idx = rc.recv().unwrap();
            let addr = unsafe { start_ptr.offset((idx as isize) * (Head::size() as isize)) };
            let mut head = Head::from(addr);
            let (_, len, off) = head.get();
            let data = unsafe { from_raw_parts(start_ptr.offset(off as isize), len as usize) };
            assert_eq!(len as usize, i * (HEAD_SIZE as usize));
            data.iter().fold((), |_, x| assert_eq!(*x, 7));
            head.readed(read_off);
        }
        drop(rc);
    }

    fn write(cache: &mut Cache, len: usize, ref_cnt: usize, value: u8, data_id: u64) -> usize {
        let (block_slice, idx) = cache.allocate(len, ref_cnt, data_id, 1);
        assert_eq!(len, block_slice.len());
        (0..len).fold((), |_, i| block_slice[i] = value);
        idx
    }

    fn read(idx: usize, start_ptr: *mut u8, value: u8) -> Vec<u8> {
        let addr = unsafe { start_ptr.offset((idx as isize) * (Head::size() as isize)) };
        let mut head = Head::from(addr);
        let (_, len, off) = head.get();
        let mut res = Vec::new();
        let data = unsafe { from_raw_parts(start_ptr.offset(off as isize), len as usize) };
        log::debug!("read [{:?}, {:?})", off, off + len as u64);
        data.iter().fold((), |_, x| {
            assert!(*x == value);
            res.push(*x)
        });

        head.readed(1);
        res
    }
}
