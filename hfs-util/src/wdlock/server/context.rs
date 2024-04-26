use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use ahash::AHashMap;
use tokio::sync::{
    oneshot::{self, Receiver, Sender},
    Notify,
};

use crate::wdlock::lease::LeaseID;

#[derive(Debug, Clone)]
pub struct DLockContext {
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    shutdown: Mutex<bool>,
    background_task: Notify,
}

// #[derive(Debug)]
// pub enum WatchMessage {
//     UnLock,
//     CancleWatch,
//     CancleLease,
//     UnKnownErr,
// }

#[derive(Debug)]
struct State {
    watch_map: AHashMap<LeaseID, Sender<bool>>,
    lease_map: AHashMap<LeaseID, Duration>,
    atomic: AtomicU64,
    start_time: Instant,
}

impl DLockContext {
    pub fn new() -> DLockContext {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                watch_map: AHashMap::new(),
                lease_map: AHashMap::new(),
                atomic: AtomicU64::new(0),
                start_time: Instant::now(),
            }),
            shutdown: Mutex::new(false),
            background_task: Notify::new(),
        });

        // Start the background task.
        // tokio::spawn(check_lease_alive_tasks(shared.clone()));
        DLockContext { shared }
    }

    pub fn watch_lease(&self, lease_id: LeaseID) -> Option<Receiver<bool>> {
        let mut state = self.shared.state.lock().unwrap();
        // Unknown lease id
        if !state.lease_map.contains_key(&lease_id) {
            return None;
        }

        let (tx, rx) = oneshot::channel::<bool>();
        if let Some(recv) = state.watch_map.insert(lease_id, tx) {
            recv.send(false).unwrap(); // Unblock unknown
        }

        Some(rx)
    }

    pub fn apply_lease(&self) -> Option<LeaseID> {
        static DEFAULT_ALIVE_TIME: Duration = Duration::from_secs(10);
        let mut state = self.shared.state.lock().unwrap();
        state.next_lease_id().map(|lease_id| {
            let current_time = Instant::now().duration_since(state.start_time) + DEFAULT_ALIVE_TIME;
            state.lease_map.insert(lease_id, current_time);
            lease_id
        })
    }

    pub fn cancle_watch(&self, lease_id: &LeaseID) {
        let mut state = self.shared.state.lock().unwrap();
        if let Some(recv) = state.watch_map.remove(lease_id) {
            recv.send(false).unwrap();
        }
    }

    pub fn cancel_lease(&self, lease_id: &LeaseID) {
        let mut state = self.shared.state.lock().unwrap();
        if state.lease_map.remove(lease_id).is_some() {
            if let Some(recv) = state.watch_map.remove(lease_id) {
                recv.send(true).unwrap();
            }
        }
    }
}

impl Shared {
    /// True, no lease
    /// False, there's also a lease
    fn check_lease_alive(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        state.clean_timeout_lease();
        let should_reset_atomic = state.lease_map.is_empty();
        if should_reset_atomic {
            state.atomic = AtomicU64::new(0);
        }
        should_reset_atomic
    }

    fn is_shutdown(&self) -> bool {
        *self.shutdown.lock().unwrap()
    }

    fn do_shutdown(&self) {
        let mut state = self.state.lock().unwrap();
        std::mem::take(&mut state.watch_map)
            .into_iter()
            .for_each(|(_, recv)| recv.send(false).unwrap());
    }
}

impl State {
    fn next_lease_id(&self) -> Option<LeaseID> {
        let id = self.atomic.fetch_add(1, Ordering::Relaxed);
        if id == u64::MAX {
            return None;
            // panic!("Over `u64::MAX` lease id created.");
        }
        Some(id.into())
    }

    fn clean_timeout_lease(&mut self) {
        let current_time = Instant::now().duration_since(self.start_time);
        self.lease_map
            .retain(|lease_id, out_time| match current_time.cmp(&out_time) {
                std::cmp::Ordering::Greater => {
                    if let Some(recv) = self.watch_map.remove(&lease_id) {
                        recv.send(true).unwrap();
                    }
                    false
                }
                _ => true,
            });
    }
}

async fn check_lease_alive_tasks(shared: Arc<Shared>) {
    static INTERVAL_TIME: Duration = Duration::from_millis(5);
    while !shared.is_shutdown() {
        // If the lease does not exist, the process is blocked and waits for the next execution.
        if shared.check_lease_alive() {
            shared.background_task.notified().await;
        }

        tokio::time::sleep(INTERVAL_TIME).await;
    }

    shared.do_shutdown();
}
