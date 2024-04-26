use std::{
    collections::BinaryHeap,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use ahash::AHashMap;
use tokio::sync::{
    mpsc,
    oneshot::{self, Receiver, Sender},
    Notify,
};

use crate::{shutdown::Shutdown, util::time::current_timestamp};

use super::{
    error::LeaseError,
    lease::{Lease, LeaseItem, LeaseItemGurad, LeaseWithTime},
};

#[derive(Debug)]
pub struct Lessor {
    state: Mutex<LessorState>,
    gurad: Mutex<LessorGurad>,
    atomic: AtomicI64,
    background_task: Notify,
}

#[derive(Debug)]
struct LessorState {
    lease_map: AHashMap<i64, Lease>,
    expired_heap: BinaryHeap<LeaseWithTime>,
}

impl Default for LessorState {
    fn default() -> Self {
        Self {
            lease_map: AHashMap::new(),
            expired_heap: BinaryHeap::new(),
        }
    }
}

#[derive(Debug, Default)]
struct LessorGurad {
    watch_map: AHashMap<i64, AHashMap<LeaseItem, Sender<bool>>>,
    item_map: AHashMap<LeaseItem, LeaseItemGurad>,
}

impl Lessor {
    pub fn new() -> Lessor {
        Lessor {
            state: Mutex::new(Default::default()),
            gurad: Mutex::new(Default::default()),
            atomic: AtomicI64::new(0),
            background_task: Notify::new(),
        }
    }

    pub async fn resource_lock(
        &self,
        lease_id: i64,
        lease_item: LeaseItem,
    ) -> Result<(), LeaseError> {
        if !self.check_lease(&lease_id) {
            return Err(LeaseError::UnknownLease);
        }

        loop {
            match self.get_item(lease_id, lease_item.clone()) {
                Some(recv) => {
                    if !recv.await? {
                        return Err(LeaseError::LockedError);
                    }
                }
                None => return Ok(()),
            }
        }
    }

    pub fn resource_unlock(&self, lease_id: &i64, lease_item: &LeaseItem) {
        let mut gurad = self.gurad.lock().unwrap();
        if let Some(item_gurad) = gurad.item_map.get_mut(lease_item) {
            if let Some(front_lease_id) = item_gurad.waiter.front() {
                if front_lease_id != lease_id {
                    return;
                }

                item_gurad.waiter.pop_front();
                if item_gurad.waiter.is_empty() {
                    gurad.item_map.remove(lease_item);
                }
                gurad.waker_watch(lease_id, lease_item);
            }
        }
    }

    pub fn update_lease(&self, lease_id: &i64, ttl: u64) -> bool {
        let mut state = self.state.lock().unwrap();
        state.update_lease(lease_id, ttl)
    }

    pub fn lease_grant(&self, ttl: u64) -> i64 {
        let lease_id = self.next_id();
        let mut state = self.state.lock().unwrap();
        let expiration_time = current_timestamp() + ttl;
        state
            .lease_map
            .insert(lease_id, Lease::new(expiration_time));
        state.expired_heap.push(LeaseWithTime {
            id: lease_id,
            time: expiration_time,
        });
        if state.lease_map.len() == 1 {
            self.background_task.notify_one();
        }
        lease_id
    }

    pub fn lease_revoke(&self, lease_id: &i64) {
        let mut state = self.state.lock().unwrap();
        if let Some(_) = state.lease_map.remove(lease_id) {
            drop(state);
            self.clean_watch_with_lease_id(lease_id);
        }
    }

    pub fn contains_lease(&self, lease_id: &i64) -> bool {
        let state = self.state.lock().unwrap();
        state.lease_map.contains_key(lease_id)
    }

    fn get_item(&self, lease_id: i64, lease_item: LeaseItem) -> Option<Receiver<bool>> {
        let mut gurad = self.gurad.lock().unwrap();
        match gurad.item_map.get_mut(&lease_item) {
            Some(item_gurad) => {
                if let Some(watch_lease_index) = item_gurad.try_acquire_index(&lease_id) {
                    let watch_lease_id = *item_gurad.waiter.get(watch_lease_index).unwrap();

                    let state = self.state.lock().unwrap();
                    // if don't exists lease, renew get
                    if !state.lease_map.contains_key(&watch_lease_id) {
                        item_gurad.waiter.remove(watch_lease_index);
                        return None;
                    }
                    drop(state);

                    let (tx, rx) = oneshot::channel::<bool>();
                    match gurad.watch_map.get_mut(&watch_lease_id) {
                        Some(iw_map) => {
                            if let Some(unknown_recv) = iw_map.insert(lease_item, tx) {
                                let _ = unknown_recv.send(false);
                            }
                        }
                        None => {
                            let mut iw_map = AHashMap::new();
                            iw_map.insert(lease_item, tx);
                            gurad.watch_map.insert(watch_lease_id, iw_map);
                        }
                    }
                    return Some(rx);
                }
                None
            }
            None => {
                gurad
                    .item_map
                    .insert(lease_item, LeaseItemGurad::with_lease_id(lease_id));
                None
            }
        }
    }

    fn next_id(&self) -> i64 {
        let id = self.atomic.fetch_add(1, Ordering::Relaxed);
        if id < 0 {
            panic!("Over I64::MAX lease id")
        }
        id
    }

    fn check_lease(&self, lease_id: &i64) -> bool {
        let state = self.state.lock().unwrap();
        state.lease_map.contains_key(lease_id)
    }

    fn clean_watch_with_lease_id(&self, lease_id: &i64) {
        let mut gurad = self.gurad.lock().unwrap();
        if let Some(iw_map) = gurad.watch_map.remove(lease_id) {
            for (_, recv) in iw_map.into_iter() {
                let _ = recv.send(true);
            }
        }
    }

    pub async fn check_lease_alive(&self) -> bool {
        use std::cmp;

        let mut state = self.state.lock().unwrap();
        let current_time = current_timestamp();
        while let Some(peek) = state.expired_heap.peek() {
            if let cmp::Ordering::Less = current_time.cmp(&peek.time) {
                break;
            }

            let lwt = state.expired_heap.pop().unwrap();
            if let Some(lease) = state.lease_map.get(&lwt.id) {
                if let cmp::Ordering::Greater = current_time.cmp(&lease.expiration_time) {
                    self.clean_watch_with_lease_id(&lwt.id);
                    state.lease_map.remove(&lwt.id);
                }
            }
        }

        state.lease_map.is_empty()
    }

    pub fn do_shutdown(&self) {
        let mut gurad = self.gurad.lock().unwrap();
        std::mem::take(&mut gurad.watch_map)
            .into_iter()
            .for_each(|(_, item_set)| {
                for (_, recv) in item_set.into_iter() {
                    let _ = recv.send(false);
                }
            });
    }
}

impl LessorState {
    fn update_lease(&mut self, lease_id: &i64, ttl: u64) -> bool {
        if let Some(lease) = self.lease_map.get_mut(lease_id) {
            lease.expiration_time = ttl;
            self.expired_heap.push(LeaseWithTime {
                id: *lease_id,
                time: lease.expiration_time,
            });
            return true;
        }
        false
    }
}

impl LessorGurad {
    fn waker_watch(&mut self, lease_id: &i64, lease_item: &LeaseItem) {
        if let Some(item_watch_map) = self.watch_map.get_mut(lease_id) {
            if let Some(recv) = item_watch_map.remove(lease_item) {
                let _ = recv.send(true);
            }

            // Avoid too much useless monitoring
            if item_watch_map.is_empty() {
                self.watch_map.remove(lease_id);
            }
        }
    }
}

pub async fn check_lease_alive_tasks(
    lessor: Arc<Lessor>,
    mut shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
) {
    static INTERVAL_TIME: Duration = tokio::time::Duration::from_secs(1);

    while !shutdown.is_shutdown() {
        let when = tokio::time::Instant::now() + INTERVAL_TIME;

        // If the lease does not exist, the process is blocked and waits for the next execution.
        tokio::select! {
            _ = async {
                if lessor.check_lease_alive().await {
                    lessor.atomic.store(0, Ordering::Relaxed);
                    lessor.background_task.notified().await;
                }

                tokio::time::sleep_until(when).await;
            } => (),
            _ = shutdown.recv() => break,
        }
    }

    lessor.do_shutdown();
    tracing::info!("Lessor shutdown complete");
}

// #[cfg(test)]
// mod test {
//     use std::{cell::UnsafeCell, sync::Arc, time::Instant};

//     use tokio::runtime::Runtime;

//     use crate::lease::lease::LeaseItem;

//     use super::Lessor;

//     struct Data {
//         inner: UnsafeCell<i32>,
//     }

//     unsafe impl Send for Data {}
//     unsafe impl Sync for Data {}

//     #[test]
//     fn test() {
//         // 创建一个 Tokio 异步运行时
//         let rt = Runtime::new().unwrap();

//         // 在异步运行时中执行异步任务
//         rt.block_on(async {
//             // 开始计时
//             let start_time = Instant::now();

//             let lessor = Lessor::new();
//             let data_arc = Arc::new(Data {
//                 inner: UnsafeCell::new(500),
//             });

//             let requests_count = 10000;
//             let mut handle_list = vec![];

//             for _ in 0..requests_count {
//                 let handle_lessor = lessor.clone();
//                 let data = data_arc.clone();
//                 let lease_id = handle_lessor.apply_lease().unwrap();
//                 let lease_item: LeaseItem = b"file".to_vec().into();

//                 handle_list.push(tokio::spawn(async move {
//                     // println!("Lease ID: {:?} want get lock", lease_id);
//                     let _ = handle_lessor
//                         .lock(lease_id, lease_item.clone())
//                         .await
//                         .unwrap();
//                     // println!("Lease ID: {:?} get lock", lease_id);
//                     let value = unsafe { &mut *data.inner.get() };
//                     if *value > 0 {
//                         *value -= 1;
//                     }
//                     handle_lessor.unlock(&lease_id, &lease_item);
//                     // After testing, it was found that failure to release the lease
//                     // in time will seriously affect performance.
//                     handle_lessor.cancel_lease(&lease_id);
//                     // println!("Lease ID: {:?} relieve lock", lease_id);
//                 }));
//             }

//             for handle in handle_list {
//                 handle.await.unwrap();
//             }

//             // 结束计时
//             let end_time = Instant::now();

//             // 计算程序运行时间（以纳秒为单位）
//             let elapsed_time = end_time - start_time;

//             // 打印程序运行时间（以秒为单位）
//             println!("程序运行时间: {:.2?}", elapsed_time);
//             println!("last value: {}", unsafe { &*data_arc.inner.get() });
//         });
//     }
// }
