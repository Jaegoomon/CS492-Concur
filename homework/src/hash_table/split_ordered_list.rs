//! Split-ordered linked list.

use core::mem;
use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_epoch::{Guard, Owned, Shared};
use lockfree::list::{Cursor, List, Node};

use super::growable_array::GrowableArray;
use crate::map::NonblockingMap;

const HI_MASK: usize = !(usize::MAX >> 1);
const LOAD_FACTOR: usize = 2;

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

    /// Creates a new split ordered list.
    pub fn new() -> Self {
        Self::default()
    }

    fn is_null_bucket<'s>(&'s self, index: usize, guard: &'s Guard) -> bool {
        let target = self.buckets.get(index, guard);
        let bucket = target.load(Ordering::Acquire, guard);
        bucket.is_null()
    }

    /// Creates a cursor and moves it to the bucket for the given index.  If the bucket doesn't
    /// exist, recursively initializes the buckets.
    fn lookup_bucket<'s>(&'s self, index: usize, guard: &'s Guard) -> Cursor<'s, usize, Option<V>> {
        //let reverse = index.reverse_bits();
        //let sentinel = Owned::new(Node::new(reverse, None)).into_shared(guard);
        //// parent check
        //let size = self.size.load(Ordering::Acquire);
        //let parent = get_parent(index, size);
        //if parent != 0 {
        //    if self.is_null_bucket(parent, guard) {
        //        self.lookup_bucket(parent, guard);
        //    }
        //}
        //loop {
        //    let mut cursor = self.list.head(guard);
        //    if let Ok(found) = cursor.find_harris(&reverse, guard) {
        //        if found {
        //            drop(unsafe { sentinel.into_owned() });
        //            return cursor;
        //        } else {
        //            let bucket = self.buckets.get(index, guard);
        //            if !bucket.load(Ordering::Acquire, guard).is_null() {
        //                continue;
        //            }
        //            if bucket
        //                .compare_and_set(Shared::null(), sentinel, Ordering::Release, guard)
        //                .is_ok()
        //            {
        //                let mut sentinel = unsafe { sentinel.into_owned() };
        //                loop {
        //                    let mut cursor = self.list.head(guard);
        //                    if let Ok(found) = cursor.find_harris(&reverse, guard) {
        //                        if found {
        //                            drop(sentinel);
        //                            return cursor;
        //                        }
        //                    }
        //                    match cursor.insert(sentinel, guard) {
        //                        Ok(_) => return cursor,
        //                        Err(n) => sentinel = n,
        //                    }
        //                }
        //            }
        //        }
        //    }
        //}
        loop {
            let target = self.buckets.get(index, guard);
            let curr = target.load(Ordering::Acquire, guard);
            if !curr.is_null() {
                return unsafe { Cursor::from_raw(target as *const _, curr.deref() as *const _) };
            } else {
                // check is there any parent bucket
                let size = self.size.load(Ordering::Acquire);
                let parent = get_parent(index, size);
                if parent != 0 {
                    self.lookup_bucket(parent, guard);
                }
                // make sentinel node
                let reverse = index.reverse_bits();
                let sentinel = Owned::new(Node::new(reverse, None)).into_shared(guard);
                // insert sentinel node
                if self
                    .buckets
                    .get(index, guard)
                    .compare_and_set(Shared::null(), sentinel, Ordering::Release, guard)
                    .is_ok()
                {
                    let mut sentinel = unsafe { sentinel.into_owned() };
                    loop {
                        let mut cursor = self.list.head(guard);
                        if let Ok(found) = cursor.find_harris(&reverse, guard) {
                            if found {
                                // Why do I write continue?
                                drop(sentinel);
                                return cursor;
                                //continue;
                            }
                            match cursor.insert(sentinel, guard) {
                                Ok(_) => return cursor,
                                Err(n) => sentinel = n,
                            }
                        }
                    }
                } else {
                    drop(unsafe { sentinel.into_owned() });
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
        let reverse = (*key | HI_MASK).reverse_bits();
        loop {
            let size = self.size.load(Ordering::Acquire);
            let index = key % size;

            let mut cursor = self.lookup_bucket(index, guard);
            if let Ok(found) = cursor.find_harris_michael(&reverse, guard) {
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
            cursor.lookup().unwrap().as_ref()
        } else {
            None
        }
    }

    fn insert(&self, key: &usize, value: V, guard: &Guard) -> Result<(), V> {
        Self::assert_valid_key(*key);
        let reverse = (*key | HI_MASK).reverse_bits();
        //case 1: succeed
        let mut node = Owned::new(Node::new(reverse, Some(value)));
        let (_, found, _) = self.find(key, guard);
        if found {
            let failure = node.into_box().into_value();
            return Err(failure.unwrap());
        } else {
            loop {
                let mut cursor = self.list.head(guard);
                if let Ok(f) = cursor.find_harris(&reverse, guard) {
                    if f {
                        let failure = node.into_box().into_value();
                        return Err(failure.unwrap());
                    } else {
                        match cursor.insert(node, guard) {
                            Ok(_) => {
                                let count = self.count.fetch_add(1, Ordering::Release);
                                let size = self.size.load(Ordering::Acquire);
                                if count > LOAD_FACTOR * size {
                                    self.size.compare_and_swap(
                                        size,
                                        size * LOAD_FACTOR,
                                        Ordering::Release,
                                    );
                                }
                                return Ok(());
                            }
                            Err(n) => node = n,
                        }
                    }
                }
            }
        }

        // case2: succedd
        //let (_, found, _) = self.find(key, guard);
        //if found {
        //    return Err(value);
        //} else {
        //    if self.list.harris_insert(reverse, Some(value), guard) {
        //        let count = self.count.fetch_add(1, Ordering::Release) + 1;
        //        let size = self.size.load(Ordering::Acquire);
        //        if count > LOAD_FACTOR * size {
        //            self.size
        //                .compare_and_swap(size, size * LOAD_FACTOR, Ordering::Release);
        //        }
        //    }
        //    return Ok(());
        //}

        // case3: fail
        //let mut node = Owned::new(Node::new(reverse, Some(value)));
        //loop {
        //    let (_, found, mut cursor) = self.find(key, guard);
        //    if found {
        //        let failure = node.into_box().into_value();
        //        return Err(failure.unwrap());
        //    } else {
        //        match cursor.insert(node, guard) {
        //            Ok(_) => {
        //                let count = self.count.fetch_add(1, Ordering::Release) + 1;
        //                let size = self.size.load(Ordering::Acquire);
        //                if count > LOAD_FACTOR * size {
        //                    self.size
        //                        .compare_and_swap(size, size * LOAD_FACTOR, Ordering::Release);
        //                }
        //                return Ok(());
        //            }
        //            Err(n) => node = n,
        //        }
        //    }
        //}
    }

    fn delete<'a>(&'a self, key: &usize, guard: &'a Guard) -> Result<&'a V, ()> {
        Self::assert_valid_key(*key);
        loop {
            let (_, found, cursor) = self.find(key, guard);
            if found {
                match cursor.delete(guard) {
                    Ok(value) => {
                        self.count.fetch_sub(1, Ordering::Release);
                        return Ok(value.as_ref().unwrap());
                    }
                    Err(_) => continue,
                }
            } else {
                return Err(());
            }
        }
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
