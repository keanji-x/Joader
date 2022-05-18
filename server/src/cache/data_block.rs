use std::slice::{from_raw_parts, from_raw_parts_mut};

use libc::c_void;

use crate::cache::head::{Head, HEAD_SIZE};

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Data {
    // the ptr is the point of data start, that is the global ptr + off
    ptr: *mut u8,
    // the size of data is less than 4GB
    len: u64,
    off: u64,
}

impl Data {
    pub fn new(ptr: *mut u8, off: u64, len: u64) -> Data {
        Data { ptr, len, off }
    }

    pub fn allocate(&mut self, off: u64, len: u64) -> Data {
        Data {
            ptr: unsafe { self.ptr.offset(off as isize - self.off as isize) },
            off,
            len,
        }
    }

    pub fn tail_head(&mut self) -> Head {
        unsafe { self.ptr.offset((self.len - HEAD_SIZE) as isize) }.into()
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { from_raw_parts_mut(self.ptr, self.len as usize) }
    }

    pub fn as_slice(&mut self) -> &[u8] {
        unsafe { from_raw_parts(self.ptr, self.len as usize) }
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn off(&self) -> u64 {
        self.off
    }

    pub fn copy_head(&mut self, head: Head) {
        assert!(self.len > HEAD_SIZE);
        unsafe {
            self.ptr.copy_from(head.as_ptr(), HEAD_SIZE as usize);
        }
    }

    pub fn remain(&mut self, occupy_size: u64) -> Option<Data> {
        assert!(occupy_size <= self.len);
        if occupy_size == self.len {
            return None;
        }
        Some(Data {
            ptr: unsafe { self.ptr.offset(occupy_size as isize) },
            len: self.len - occupy_size,
            off: self.off + occupy_size,
        })
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DataBlock {
    pub head: Head,
    pub data: Data
}

impl DataBlock {
    pub fn new(mut head: Head, data: Data, cnt: usize) -> Self {
        head.set(data.len() as u32, data.off(), cnt);
        Self { head, data }
    }

    pub fn size(&self) -> u64 {
        self.data.len()
    }

    pub fn ptr(&mut self) -> *mut c_void {
        self.data.as_mut_ptr().cast::<c_void>()
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { from_raw_parts_mut(self.data.as_mut_ptr(), self.size() as usize) }
    }

    pub fn data(&mut self) -> Data {
        self.data
    }
}
