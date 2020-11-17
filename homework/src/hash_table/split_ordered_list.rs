//! Split-ordered linked list.

use core::mem;
use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_epoch::{Guard, Owned, Shared};
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
        loop {
            //println!("loop1");
            let target = self.buckets.get(index, guard);
            let curr = target.load(Ordering::Acquire, guard);

            if !curr.is_null() {
                return unsafe { Cursor::from_raw(target as *const _, curr.deref() as *const _) };
            } else {
                // check is there any parent bucket
                let size = self.size.load(Ordering::Acquire);
                let parent = get_parent(index, size);
                if parent != 0 {
                    let parent_bucket = self.buckets.get(parent, guard);
                    if parent_bucket.load(Ordering::Acquire, guard).is_null() {
                        self.lookup_bucket(parent, guard);
                    }
                }

                // add sentinel node
                let sentinel = Owned::new(Node::new(index.reverse_bits(), None));
                let sentinel = sentinel.into_shared(guard);
                if target
                    .compare_and_set(Shared::null(), sentinel, Ordering::Release, guard)
                    .is_ok()
                {
                    let mut cursor = self.list.head(guard);
                    if cursor
                        .insert(unsafe { sentinel.into_owned() }, guard)
                        .is_ok()
                    {
                        let target = self.buckets.get(index, guard);
                        let curr = target.load(Ordering::Acquire, guard);
                        return unsafe {
                            Cursor::from_raw(target as *const _, curr.deref() as *const _)
                        };
                    }
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
            //println!("loop2");
            let mut cursor = self.lookup_bucket(index, guard);
            if let Ok(found) = cursor.find_harris(&(*key).reverse_bits(), guard) {
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
        }
        // check resizing is needed
        let count = self.count.load(Ordering::Acquire);
        let size = self.size.load(Ordering::Acquire);
        if count > 2 * size {
            self.size
                .compare_and_swap(size, size * 2, Ordering::Release);
        }
        Ok(())
    }

    fn delete<'a>(&'a self, key: &usize, guard: &'a Guard) -> Result<&'a V, ()> {
        Self::assert_valid_key(*key);
        let (_, found, cursor) = self.find(key, guard);
        if found {
            if let Ok(ok) = cursor.delete(guard) {
                if let Some(v) = ok.as_ref() {
                    self.count.fetch_sub(1, Ordering::Release);
                    return Ok(v);
                }
            }
        }
        return Err(());
    }
}

fn get_parent(bucket: usize, size: usize) -> usize {
    let mut parent = size;
    loop {
        parent = parent >> 1;
        if parent <= bucket {
            break;
        }
    }
    return bucket - parent;
}
