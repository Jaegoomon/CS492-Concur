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
        loop {
            if (*self.0).is_null() {
                return false;
            } else {
                unsafe {
                    if *key == (*(*self.0)).data {
                        return true;
                    } else {
                        let next = (*(*self.0)).next.lock().unwrap();
                        self.0 = next;
                    }
                }
            }
        }
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
        let (is_key, cursor) = self.find(key);
        is_key
    }
    /// Insert a key to the set. If the set already has the key, return the provided key in `Err`.
    pub fn insert(&self, key: T) -> Result<(), T> {
        {
            let (is_key, cursor) = self.find(&key);
            if is_key {
                return Err(key);
            }
        }

        let mut start = self.head.lock().unwrap();
        let node = Node::new(key, ptr::null_mut());
        let node_data = unsafe { Box::from_raw(node) };
        loop {
            if (*start).is_null() {
                *start = node;
                return Ok(());
            } else {
                let curr = unsafe { Box::from_raw(*start) };
                if node_data.data < curr.data {
                    *node_data.next.lock().unwrap() = *start;
                    *start = node;
                    return Ok(());
                } else {
                    let mut next = curr.next.lock().unwrap();
                    if (*next).is_null() {
                        *next = node;
                        return Ok(());
                    } else {
                        let next_node = unsafe { Box::from_raw(*next) };
                        if node_data.data > next_node.data {
                            *node_data.next.lock().unwrap() = *next;
                            *start = node;
                            return Ok(());
                        } else {
                            *start = *next;
                        }
                    }
                }
            }
        }
    }

    /// Remove the key from the set and return it.
    pub fn remove(&self, key: &T) -> Result<T, ()> {
        todo!()
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
        match self.0.as_mut() {
            Some(guard) => {
                if (**guard).is_null() {
                    return None;
                } else {
                    unsafe {
                        let data = &(***guard).data;
                        *guard = (***guard).next.lock().unwrap();
                        Some(&data)
                    }
                }
            }
            None => None,
        }
    }
}

impl<T> Drop for OrderedListSet<T> {
    fn drop(&mut self) {
        todo!()
    }
}

impl<T> Default for OrderedListSet<T> {
    fn default() -> Self {
        Self::new()
    }
}
