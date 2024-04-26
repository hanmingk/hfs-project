use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    ops::{Deref, DerefMut},
    sync::{Mutex, MutexGuard},
};

use crate::wdlock::{error::DLockError, lease::LeaseID, server::DLockContext};

#[derive(Debug)]
pub struct DLock<T: ?Sized> {
    inner: Inner,
    data: UnsafeCell<T>,
}

pub struct DLockGuard<'a, T: ?Sized> {
    lock: &'a DLock<T>,
}

#[derive(Debug)]
struct Inner {
    waiter: Mutex<VecDeque<LeaseID>>,
}

impl<T> DLock<T> {
    pub fn new(t: T) -> DLock<T> {
        DLock {
            inner: Inner::new(),
            data: UnsafeCell::new(t),
        }
    }
}

impl<T: ?Sized> DLock<T> {
    pub async fn lock(
        &self,
        ctx: &DLockContext,
        lease_id: &LeaseID,
    ) -> Result<DLockGuard<'_, T>, DLockError> {
        loop {
            match self.inner.acquire(lease_id) {
                Some(locker_lease_id) => match ctx.watch_lease(locker_lease_id) {
                    Some(recv) => {
                        if !recv.await? {
                            return Err(DLockError::LockedError);
                        }
                    }
                    None => return Err(DLockError::UnknownLease),
                },
                None => {
                    return Ok(DLockGuard::new(self));
                }
            }
        }
    }

    pub fn unlock(&self) {
        self.inner.waiter.lock().unwrap().pop_front();
    }

    pub fn try_lock(
        &self,
        ctx: &DLockContext,
        lease_id: &LeaseID,
    ) -> Result<DLockGuard<'_, T>, DLockError> {
        match self.inner.try_acquire(lease_id) {
            Some(watch_lease_id) => {
                ctx.cancle_watch(&watch_lease_id);
                Err(DLockError::LockedError)
            }
            None => Ok(DLockGuard::new(self)),
        }
    }
}

impl<'dlock, T: ?Sized> DLockGuard<'dlock, T> {
    fn new(lock: &'dlock DLock<T>) -> DLockGuard<'dlock, T> {
        DLockGuard { lock }
    }
}

impl<T: ?Sized> Deref for DLockGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.lock.data.get() }
    }
}

impl<T: ?Sized> DerefMut for DLockGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.lock.data.get() }
    }
}

impl Inner {
    fn new() -> Inner {
        Inner {
            waiter: Mutex::new(VecDeque::new()),
        }
    }

    fn acquire(&self, lease_id: &LeaseID) -> Option<LeaseID> {
        let mut waiter = self.waiter.lock().unwrap();
        Inner::try_acquire_index(&mut waiter, lease_id).map(|index| *waiter.get(index).unwrap())
    }

    /// if return Some(LeaseID), Listening lease id
    /// if return None, get lock success
    fn try_acquire(&self, lease_id: &LeaseID) -> Option<LeaseID> {
        let mut waiter = self.waiter.lock().unwrap();
        Inner::try_acquire_index(&mut waiter, lease_id).map(|index| {
            let watch_lease_id = waiter.get(index).unwrap().clone();
            waiter.remove(index + 1);
            watch_lease_id
        })
    }

    fn try_acquire_index(
        waiter: &mut MutexGuard<'_, VecDeque<LeaseID>>,
        lease_id: &LeaseID,
    ) -> Option<usize> {
        match waiter
            .iter()
            .position(|wait_lease_id| wait_lease_id == lease_id)
        {
            Some(index) => {
                if index == 0 {
                    None
                } else {
                    Some(index - 1)
                }
            }
            None => {
                if waiter.is_empty() {
                    waiter.push_back(*lease_id);
                    None
                } else {
                    waiter.push_back(*lease_id);
                    Some(waiter.len() - 2)
                }
            }
        }
    }
}

unsafe impl<T> Send for DLock<T> where T: ?Sized + Send {}
unsafe impl<T> Sync for DLock<T> where T: ?Sized + Send {}
