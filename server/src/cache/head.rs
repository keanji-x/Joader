use std::{convert::TryInto, slice::from_raw_parts, slice::from_raw_parts_mut};

// head: |--len--|----off----|read|*8
//       |--4--|----8----|1|*8
pub const HEAD_SIZE: u64 = 1 * 8 + 12;
pub const UNREADED: u64 = 0xffffffffffffffff;
pub const READ_OFF: isize = 12;
pub const LEN_OFF: isize = 0;
pub const OFF_OFF: isize = 4;
pub const READ_LEN: usize = 8;
pub const LEN_LEN: usize = 4;
pub const OFF_LEN: usize = 8;
#[derive(Debug, Clone, Copy)]
pub struct Head {
    ptr: *mut u8,
    state: HeadState,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum HeadState {
    Free,
    Allocated,
}

impl From<*mut u8> for Head {
    fn from(ptr: *mut u8) -> Self {
        Self { ptr, state: HeadState::Free }
    }
}

impl Head {
    pub fn set(&mut self, len: u32, off: u64, cnt: usize) {
        log::debug!(
            "Write head off: {:} len:{:} cnt {:}, {:})",
            off,
            len,
            cnt,
            UNREADED >> (READ_LEN - cnt)
        );
        self.set_unread(cnt);
        self.set_len(len);
        self.set_off(off);
    }

    pub fn set_unread(&mut self, cnt: usize) {
        let v = UNREADED << ((READ_LEN - cnt)*8);
        unsafe {
            self.ptr
                .offset(READ_OFF)
                .copy_from((v).to_be_bytes().as_ptr(), READ_LEN)
        };
    }

    fn set_len(&mut self, len: u32) {
        unsafe {
            self.ptr
                .offset(LEN_OFF)
                .copy_from(len.to_be_bytes().as_ptr(), LEN_LEN)
        };
    }

    fn set_off(&mut self, off: u64) {
        unsafe {
            self.ptr
                .offset(OFF_OFF)
                .copy_from(off.to_be_bytes().as_ptr(), OFF_LEN)
        };
    }

    pub fn get(&self) -> (bool, u32, u64) {
        (self.get_readed(), self.get_len(), self.get_off())
    }

    pub fn get_len(&self) -> u32 {
        let slice = unsafe { from_raw_parts(self.ptr.offset(LEN_OFF), LEN_LEN) };

        u32::from_be_bytes(slice.try_into().unwrap())
    }

    pub fn get_off(&self) -> u64 {
        let slice = unsafe { from_raw_parts(self.ptr.offset(OFF_OFF), OFF_LEN) };
        u64::from_be_bytes(slice.try_into().unwrap())
    }

    pub fn get_readed(&self) -> bool {
        let slice = unsafe { from_raw_parts(self.ptr.offset(READ_OFF), READ_LEN) };
        u64::from_be_bytes(slice.try_into().unwrap()) == 0
    }

    pub fn is_readed(&self) -> bool {
        let slice = unsafe { from_raw_parts(self.ptr.offset(READ_OFF), READ_LEN) };
        u64::from_be_bytes(slice.try_into().unwrap()) == 0
    }

    pub fn readed(&mut self, cnt: usize) {
        log::debug!("readed {:?}", self.as_ptr());
        unsafe {
            self.ptr
                .offset(READ_OFF + (cnt - 1) as isize)
                .copy_from((0 as u8).to_be_bytes().as_ptr(), 1)
        };
    }

    pub fn is_free(&self) -> bool {
        self.state == HeadState::Free
    }

    pub fn set_free(&mut self) {
        self.state = HeadState::Free;
    }

    pub fn allocated(&mut self) {
        self.state = HeadState::Allocated;
    }

    pub fn size() -> u64 {
        HEAD_SIZE
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { from_raw_parts_mut(self.ptr, HEAD_SIZE as usize) }
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }
}
