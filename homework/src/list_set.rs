#![allow(clippy::mutex_atomic)]
use std::cmp;
use std::ptr;
use std::sync::{Mutex, MutexGuard};

#[derive(Debug)]
struct Node<T> {
    data: T,
    next: Mutex<*mut Node<T>>,
}

unsafe impl<T> Send for Node<T> {}
unsafe impl<T> Sync for Node<T> {}

/// Concurrent sorted singly linked list using lock-coupling.
#[derive(Debug)]
pub struct OrderedListSet<T> {
    head: Mutex<*mut Node<T>>,
}

unsafe impl<T> Send for OrderedListSet<T> {}
unsafe impl<T> Sync for OrderedListSet<T> {}

// reference to the `next` field of previous node which points to the current node
struct Cursor<'l, T>(MutexGuard<'l, *mut Node<T>>);

impl<T> Node<T> {
    fn new(data: T, next: *mut Self) -> *mut Self {
        Box::into_raw(Box::new(Self {
            data,
            next: Mutex::new(next),
        }))
    }
}

impl<'l, T: Ord> Cursor<'l, T> {
    /// Move the cursor to the position of key in the sorted list. If the key is found in the list,
    /// return `true`.
    fn find(&mut self, key: &T) -> bool {
        while !(*self.0).is_null() {
            unsafe {
                if *key == (**self.0).data {
                    return true;
                }
                if let Ok(next) = (**self.0).next.try_lock() {
                    self.0 = next;
                }
            }
        }
        false
    }
}

impl<T> OrderedListSet<T> {
    /// Creates a new list.
    pub fn new() -> Self {
        Self {
            head: Mutex::new(ptr::null_mut()),
        }
    }
}

impl<T: Ord> OrderedListSet<T> {
    fn find(&self, key: &T) -> (bool, Cursor<T>) {
        loop {
            if let Ok(guard) = self.head.try_lock() {
                let mut cursor = Cursor(guard);
                return (cursor.find(key), cursor);
            }
        }
    }

    /// Returns `true` if the set contains the key.
    pub fn contains(&self, key: &T) -> bool {
        self.find(key).0
    }
    /// Insert a key to the set. If the set already has the key, return the provided key in `Err`.
    pub fn insert(&self, key: T) -> Result<(), T> {
        {
            if self.contains(&key) {
                return Err(key);
            }
        }
        loop {
            if let Ok(guard) = self.head.try_lock() {
                unsafe {
                    let mut cursor = guard;
                    loop {
                        if (*cursor).is_null() || key < (**cursor).data {
                            let node = Node::new(key, *cursor);
                            *cursor = node;
                            return Ok(());
                        }
                        if let Ok(next) = (**cursor).next.try_lock() {
                            cursor = next;
                        }
                    }
                }
            }
        }
    }

    /// Remove the key from the set and return it.
    pub fn remove(&self, key: &T) -> Result<T, ()> {
        let (is_key, mut cursor) = self.find(key);
        if !is_key {
            return Err(());
        }
        unsafe {
            let data = Box::from_raw(*cursor.0);
            loop {
                if let Ok(guard) = data.next.try_lock() {
                    //drop(*cursor.0);
                    *cursor.0 = *guard;
                    return Ok(data.data);
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct Iter<'l, T>(Option<MutexGuard<'l, *mut Node<T>>>);

impl<T> OrderedListSet<T> {
    /// An iterator visiting all elements.
    pub fn iter(&self) -> Iter<T> {
        Iter(Some(self.head.lock().unwrap()))
    }
}

impl<'l, T> Iterator for Iter<'l, T> {
    type Item = &'l T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(guard) = self.0.as_mut() {
            if (**guard).is_null() {
                self.0 = None;
                return None;
            } else {
                unsafe {
                    let data = &(***guard).data;
                    loop {
                        if let Ok(g) = (***guard).next.try_lock() {
                            self.0 = Some(g);
                            return Some(data);
                        }
                    }
                }
            }
        }
        None
    }
}

impl<T> Drop for OrderedListSet<T> {
    fn drop(&mut self) {
        drop(self)
    }
}

impl<T> Default for OrderedListSet<T> {
    fn default() -> Self {
        Self::new()
    }
}
