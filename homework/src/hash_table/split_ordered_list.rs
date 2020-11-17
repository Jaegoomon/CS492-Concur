//! Split-ordered linked list.

use core::mem;
use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_epoch::{Guard, Owned};
use lockfree::list::{Cursor, List, Node};

use super::growable_array::GrowableArray;
use crate::map::NonblockingMap;

/// Lock-free map from `usize` in range [0, 2^63-1] to `V`.
///
/// NOTE: We don't care about hashing in this homework for simplicity.
#[derive(Debug)]
pub struct SplitOrderedList<V> {
    /// Lock-free list sorted by recursive-split order. Use `None` sentinel node value.
    list: List<usize, Option<V>>,
    /// array of pointers to the buckets
    buckets: GrowableArray<Node<usize, Option<V>>>,
    /// number of buckets
    size: AtomicUsize,
    /// number of items
    count: AtomicUsize,
}

impl<V> Default for SplitOrderedList<V> {
    fn default() -> Self {
        Self {
            list: List::new(),
            buckets: GrowableArray::new(),
            size: AtomicUsize::new(2),
            count: AtomicUsize::new(0),
        }
    }
}

impl<V> SplitOrderedList<V> {
    /// `size` is doubled when `count > size * LOAD_FACTOR`.
    const LOAD_FACTOR: usize = 2;

    /// Creates a new split ordered list.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a cursor and moves it to the bucket for the given index.  If the bucket doesn't
    /// exist, recursively initializes the buckets.
    fn lookup_bucket<'s>(&'s self, index: usize, guard: &'s Guard) -> Cursor<'s, usize, Option<V>> {
        // compare bucket size with index
        let size = self.size.load(Ordering::Acquire);
        let count = self.count.load(Ordering::Acquire);
        //if count > size * 2 {
        //    self.size.fetch_add(size, Ordering::Release);
        //}

        let target = self.buckets.get(index, guard);
        let curr = target.load(Ordering::Acquire, guard);
        if !curr.is_null() {
            return unsafe { Cursor::from_raw(target as *const _, curr.deref() as *const _) };
        } else {
            // initialize the bucket
            let sentinel = Owned::new(Node::new(index.reverse_bits(), None));
            let sentinel = sentinel.into_shared(guard);
            target.store(sentinel, Ordering::Release);
            loop {
                let mut cursor = self.list.head(guard);
                if cursor
                    .insert(unsafe { sentinel.into_owned() }, guard)
                    .is_ok()
                {
                    return self.lookup_bucket(index, guard);
                }
            }
        }
    }

    /// Moves the bucket cursor returned from `lookup_bucket` to the position of the given key.
    /// Returns `(size, found, cursor)`
    fn find<'s>(
        &'s self,
        key: &usize,
        guard: &'s Guard,
    ) -> (usize, bool, Cursor<'s, usize, Option<V>>) {
        // size of the bucket
        let size = self.size.load(Ordering::Acquire);
        // convert key to index by using modulo
        let index = key % size;

        loop {
            let mut cursor = self.lookup_bucket(index, guard);
            // go to sentinel find with traverse algorithm
            if let Ok(found) = cursor.find_harris_michael(&(*key).reverse_bits(), guard) {
                return (size, found, cursor);
            }
        }
    }

    fn assert_valid_key(key: usize) {
        assert!(key.leading_zeros() != 0);
    }
}

impl<V> NonblockingMap<usize, V> for SplitOrderedList<V> {
    fn lookup<'a>(&'a self, key: &usize, guard: &'a Guard) -> Option<&'a V> {
        Self::assert_valid_key(*key);
        let (_, found, cursor) = self.find(key, guard);
        if found {
            let curr = cursor.lookup().unwrap().as_ref();
            curr
        } else {
            None
        }
    }

    fn insert(&self, key: &usize, value: V, guard: &Guard) -> Result<(), V> {
        Self::assert_valid_key(*key);
        let (_, found, mut cursor) = self.find(key, guard);
        if found {
            return Err(value);
        } else {
            let node = Owned::new(Node::new((*key).reverse_bits(), Some(value)));
            if cursor.insert(node, guard).is_ok() {
                self.count.fetch_add(1, Ordering::Release);
            }
            Ok(())
        }
    }

    fn delete<'a>(&'a self, key: &usize, guard: &'a Guard) -> Result<&'a V, ()> {
        Self::assert_valid_key(*key);
        let (_, found, cursor) = self.find(key, guard);
        if found {
            match cursor.delete(guard) {
                Ok(ok) => match ok.as_ref() {
                    Some(v) => {
                        self.count.fetch_sub(1, Ordering::Release);
                        Ok(v)
                    }
                    None => Err(()),
                },
                Err(_) => Err(()),
            }
        } else {
            return Err(());
        }
    }
}
