use std::sync::{
    atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering},
    Mutex,
};

#[derive(Debug)]
pub struct GenIdI64 {
    atomic: AtomicI64,
    cache_id: Mutex<Vec<i64>>,
}

impl GenIdI64 {
    pub fn new(v: i64) -> GenIdI64 {
        GenIdI64 {
            atomic: AtomicI64::new(v),
            cache_id: Mutex::new(vec![]),
        }
    }

    pub fn next_id(&self) -> i64 {
        let mut cache = self.cache_id.lock().unwrap();
        match cache.pop() {
            Some(id) => id,
            None => {
                let id = self.atomic.fetch_add(1, Ordering::Relaxed);
                if id < 0 {
                    panic!("Over Max id!!");
                }
                id
            }
        }
    }

    pub fn give_back(&self, id: i64) {
        let mut cache = self.cache_id.lock().unwrap();
        cache.push(id)
    }
}

#[derive(Debug)]
pub struct GenIdU64 {
    atomic: AtomicU64,
    cache_id: Mutex<Vec<u64>>,
}

impl GenIdU64 {
    pub fn new(v: u64) -> GenIdU64 {
        GenIdU64 {
            atomic: AtomicU64::new(v),
            cache_id: Mutex::new(vec![]),
        }
    }

    pub fn next_id(&self) -> u64 {
        let mut cache = self.cache_id.lock().unwrap();
        match cache.pop() {
            Some(id) => id,
            None => {
                let id = self.atomic.fetch_add(1, Ordering::Relaxed);
                if id == u64::MAX {
                    panic!("Over u64::Max id!!");
                }
                id
            }
        }
    }

    pub fn give_back(&self, id: u64) {
        let mut cache = self.cache_id.lock().unwrap();
        cache.push(id)
    }
}

#[derive(Debug)]
pub struct GenIdU32 {
    atomic: AtomicU32,
    cache_id: Mutex<Vec<u32>>,
}

impl GenIdU32 {
    pub fn new(v: u32) -> GenIdU32 {
        GenIdU32 {
            atomic: AtomicU32::new(v),
            cache_id: Mutex::new(vec![]),
        }
    }

    pub fn next_id(&self) -> u32 {
        let mut cache = self.cache_id.lock().unwrap();
        match cache.pop() {
            Some(id) => id,
            None => {
                let id = self.atomic.fetch_add(1, Ordering::Relaxed);
                if id == u32::MAX {
                    panic!("Over u32::Max id!!");
                }
                id
            }
        }
    }

    pub fn give_back(&self, id: u32) {
        let mut cache = self.cache_id.lock().unwrap();
        cache.push(id)
    }
}
