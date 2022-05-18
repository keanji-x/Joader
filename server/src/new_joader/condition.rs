use std::sync::{Condvar, Mutex};

#[derive(Debug)]
pub struct Cond(Mutex<bool>, Condvar);

impl Cond {
    pub fn new() -> Self {
        Cond(Mutex::new(true), Condvar::new())
    }
    pub fn wait(&self) {
        let mut m = self.0.lock().unwrap();
        *m = false;
        while !*m {
            m = self.1.wait(m).unwrap();
        }
    }
    pub fn notify(&self) {
        let mut m = self.0.lock().unwrap();
        if *m == true {
            return;
        }
        *m = true;
        self.1.notify_one();
    }
}
