//! Growable array.

use core::fmt::Debug;
use core::marker::PhantomData;
use core::mem;
use core::ops::{Deref, DerefMut};
use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_epoch::{unprotected, Atomic, Guard, Owned, Pointer, Shared};

/// Growable array of `Atomic<T>`.
///
/// This is more complete version of the dynamic sized array from the paper. In the paper, the
/// segment table is an array of arrays (segments) of pointers to the elements. In this
/// implementation, a segment contains the pointers to the elements **or other segments**. In other
/// words, it is a tree that has segments as internal nodes.
///
/// # Example run
///
/// Suppose `SEGMENT_LOGSIZE = 3` (segment size 8).
///
/// When a new `GrowableArray` is created, `root` is initialized with `Atomic::null()`.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
/// ```
///
/// When you store element `cat` at the index `0b001`, it first initializes a segment.
///
/// ```text
///
///                          +----+
///                          |root|
///                          +----+
///                            | height: 1
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                                           |
///                                           v
///                                         +---+
///                                         |cat|
///                                         +---+
/// ```
///
/// When you store `fox` at `0b111011`, it is clear that there is no room for indices larger than
/// `0b111`. So it first allocates another segment for upper 3 bits and moves the previous root
/// segment (`0b000XXX` segment) under the `0b000XXX` branch of the the newly allocated segment.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                                               |
///                                               v
///                                      +---+---+---+---+---+---+---+---+
///                                      |111|110|101|100|011|010|001|000|
///                                      +---+---+---+---+---+---+---+---+
///                                                                |
///                                                                v
///                                                              +---+
///                                                              |cat|
///                                                              +---+
/// ```
///
/// And then, it allocates another segment for `0b111XXX` indices.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                   |                           |
///                   v                           v
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
/// |111|110|101|100|011|010|001|000|    |111|110|101|100|011|010|001|000|
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
///                   |                                            |
///                   v                                            v
///                 +---+                                        +---+
///                 |fox|                                        |cat|
///                 +---+                                        +---+
/// ```
///
/// Finally, when you store `owl` at `0b000110`, it traverses through the `0b000XXX` branch of the
/// level-1 segment and arrives at its 0b110` leaf.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                   |                           |
///                   v                           v
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
/// |111|110|101|100|011|010|001|000|    |111|110|101|100|011|010|001|000|
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
///                   |                        |                   |
///                   v                        v                   v
///                 +---+                    +---+               +---+
///                 |fox|                    |owl|               |cat|
///                 +---+                    +---+               +---+
/// ```
///
/// When the array is dropped, only the segments are dropped and the **elements must not be
/// dropped/deallocated**.
///
/// ```test
///                 +---+                    +---+               +---+
///                 |fox|                    |owl|               |cat|
///                 +---+                    +---+               +---+
/// ```
///
/// Instead, it should be handled by the container that the elements actually belong to. For
/// example in `SplitOrderedList`, destruction of elements are handled by `List`.
///
#[derive(Debug)]
pub struct GrowableArray<T> {
    root: Atomic<Segment>,
    _marker: PhantomData<T>,
}

const SEGMENT_LOGSIZE: usize = 10;

struct Segment {
    /// `AtomicUsize` here means `Atomic<T>` or `Atomic<Segment>`.
    inner: [AtomicUsize; 1 << SEGMENT_LOGSIZE],
}

impl Segment {
    fn new() -> Self {
        Self {
            inner: unsafe { mem::zeroed() },
        }
    }
}

impl Deref for Segment {
    type Target = [AtomicUsize; 1 << SEGMENT_LOGSIZE];

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Segment {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Debug for Segment {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Segment")
    }
}

impl<T> Drop for GrowableArray<T> {
    /// Deallocate segments, but not the individual elements.
    fn drop(&mut self) {
        //get the tag
        let guard = unsafe { &unprotected() };
        let seg = self.root.load(Ordering::Relaxed, guard);
        let t = seg.tag();
        drop_garbage(seg, t);
    }
}

impl<T> Default for GrowableArray<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> GrowableArray<T> {
    /// Create a new growable array.
    pub fn new() -> Self {
        Self {
            root: Atomic::null(),
            _marker: PhantomData,
        }
    }

    /// Returns the reference to the `Atomic` pointer at `index`. Allocates new segments if
    /// necessary.
    pub fn get(&self, mut index: usize, guard: &Guard) -> &Atomic<T> {
        // check if root is null_ptr
        let new_seg = Owned::new(Segment::new()).with_tag(1);
        let new_seg = new_seg.into_shared(guard);
        loop {
            let root = self.root.load(Ordering::Acquire, guard);
            if root.is_null() {
                // CAS with root and new Segment
                if self
                    .root
                    .compare_and_set(root, new_seg, Ordering::Release, guard)
                    .is_ok()
                {
                    break;
                }
            } else {
                unsafe { drop(new_seg.into_owned()) };
                break;
            }
        }

        // check the segment range can hold the index
        let h = get_height(index, SEGMENT_LOGSIZE);
        loop {
            let child = self.root.load(Ordering::Acquire, guard);
            if h > child.tag() {
                //let child = self.root.load(Ordering::Acquire, guard);
                let parent = Owned::new(Segment::new()).with_tag(child.tag() + 1);
                parent[0].store(child.into_usize(), Ordering::Release);

                // CAS with root and parent Segment
                let parent = parent.into_shared(guard);
                if self
                    .root
                    .compare_and_set(child, parent, Ordering::Release, guard)
                    .is_err()
                {
                    unsafe { drop(parent.into_owned()) };
                }
            } else {
                break;
            }
        }

        // traversing
        let mut something = self.root.load(Ordering::Acquire, guard);
        let mut th = something.tag();
        loop {
            let position = (index >> (SEGMENT_LOGSIZE * (th - 1))) & ((1 << SEGMENT_LOGSIZE) - 1);
            if let Some(target) = unsafe { something.deref() }.get(position) {
                // checking heigt
                if th == 1 {
                    return unsafe { &*(target as *const _ as *const Atomic<T>) };
                }

                let t = target.load(Ordering::Acquire);
                // implement new branch
                if t == 0 {
                    let aux = Owned::new(Segment::new());
                    let aux = aux.into_shared(guard);
                    if unsafe { something.deref().get_unchecked(position) }
                        .compare_exchange(0, aux.into_usize(), Ordering::Release, Ordering::Relaxed)
                        .is_err()
                    {
                        // if failure drop aux
                        unsafe { drop(aux.into_owned()) };
                    } else {
                        something = aux;
                        th -= 1;
                    }
                    continue;
                }
                // keep going
                something = unsafe { Shared::<Segment>::from_usize(t) };
                th -= 1;
            }
        }
    }
}

fn get_height(mut index: usize, capacity: usize) -> usize {
    let mut h = 0;
    while index > 0 {
        index = index >> capacity;
        h += 1;
    }
    h
}

fn drop_garbage(seg: Shared<Segment>, height: usize) -> () {
    // height is equal to zero, stop dropping
    if height == 0 {
        return;
    }

    unsafe {
        let curr_seg = seg.deref();
        for i in 0..(1 << SEGMENT_LOGSIZE) {
            let curr_usize = curr_seg.get_unchecked(i).load(Ordering::Relaxed);
            let next = Shared::<Segment>::from_usize(curr_usize);
            if !next.is_null() {
                // recursive dropping
                drop_garbage(next, height - 1);
            }
        }
        // lastly drop current seg
        drop(seg.into_owned());
    }
}
