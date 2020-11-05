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
                let next = (**self.0).next.lock().unwrap();
                self.0 = next;
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
        let mut cursor = Cursor(self.head.lock().unwrap());
        (cursor.find(key), cursor)
    }

    /// Returns `true` if the set contains the key.
    pub fn contains(&self, key: &T) -> bool {
        Cursor(self.head.lock().unwrap()).find(key)
    }
    /// Insert a key to the set. If the set already has the key, return the provided key in `Err`.
    pub fn insert(&self, key: T) -> Result<(), T> {
        {
            if Cursor(self.head.lock().unwrap()).find(&key) {
                return Err(key);
            }
        }

        let mut cursor = Cursor(self.head.lock().unwrap());
        unsafe {
            if (*cursor.0).is_null() || (**cursor.0).data > key {
                let node = Node::new(key, *cursor.0);
                *cursor.0 = node;
                return Ok(());
            }
            loop {
                let mut next = Cursor((**cursor.0).next.lock().unwrap());

                if (*next.0).is_null() || (**next.0).data > key {
                    let node = Node::new(key, *next.0);
                    *next.0 = node;
                    return Ok(());
                }
                drop(cursor);
                cursor = next;
            }
        }
    }

    /// Remove the key from the set and return it.
    pub fn remove(&self, key: &T) -> Result<T, ()> {
        {
            if !Cursor(self.head.lock().unwrap()).find(&key) {
                return Err(());
            }
        }

        let mut cursor = Cursor(self.head.lock().unwrap());
        unsafe {
            if (**cursor.0).data == *key {
                let data = Box::from_raw(*cursor.0).data;
                *cursor.0 = *(**cursor.0).next.lock().unwrap();
                return Ok(data);
            }
            loop {
                let mut next = Cursor((**cursor.0).next.lock().unwrap());
                if (**next.0).data == *key {
                    let data = Box::from_raw(*next.0).data;
                    let target = (**next.0).next.lock().unwrap();
                    *next.0 = *target;
                    return Ok(data);
                }
                drop(cursor);
                cursor = next;
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
        loop {
            match self.0.as_mut() {
                Some(guard) => {
                    if (**guard).is_null() {
                        return None;
                    } else {
                        unsafe {
                            let data = &(***guard).data;
                            match (***guard).next.try_lock() {
                                Ok(g) => {
                                    self.0 = Some(g);
                                    return Some(data);
                                }
                                Err(_) => return None,
                            }
                        }
                    }
                }
                None => continue,
            }
        }
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
