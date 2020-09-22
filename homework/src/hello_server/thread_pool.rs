//! Thread pool that joins all thread when dropped.

#![allow(clippy::mutex_atomic)]

// NOTE: Crossbeam channels are MPMC, which means that you don't need to wrap the receiver in
// Arc<Mutex<..>>. Just clone the receiver and give it to each worker thread.
use crossbeam_channel::{unbounded, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

enum Message {
    NewJob(Job, Arc<(Mutex<usize>, Condvar)>),
    Terminate,
}

trait FnBox {
    fn call_box(self: Box<Self>);
}

impl<F: FnOnce()> FnBox for F {
    fn call_box(self: Box<F>) {
        (*self)()
    }
}

type Job = Box<dyn FnBox + Send + 'static>;

#[derive(Debug)]
struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Drop for Worker {
    /// When dropped, the thread's `JoinHandle` must be `join`ed.  If the worker panics, then this
    /// function should panic too.  NOTE: that the thread is detached if not `join`ed explicitly.
    fn drop(&mut self) {
        // join worker thread
        // println!("[Worker {}] joined", self.id);

        if let Some(thread) = self.thread.take() {
            match thread.join() {
                Ok(v) => (),
                Err(e) => (),
            }
        }
    }
}

impl Worker {
    fn new(id: usize, receiver: Receiver<Message>) -> Worker {
        let thread = thread::spawn(move || loop {
            let message = receiver.recv().unwrap();
            match message {
                Message::NewJob(job, pair) => {
                    //println!("[Worker {}] starts a job.", id);
                    job.call_box();
                    let (lock, cvar) = &*pair;
                    let mut data = lock.lock().unwrap();
                    *data += 1;
                    cvar.notify_one();
                }
                Message::Terminate => {
                    //println!("[Worker {}] was terminating.", id);
                    break;
                }
            }
        });

        Worker {
            id: id,
            thread: Some(thread),
        }
    }
}

/// Internal data structure for tracking the current job status. This is shared by the worker
/// closures via `Arc` so that the workers can report to the pool that it started/finished a job.
#[derive(Debug, Default)]
struct ThreadPoolInner {
    job_count: Arc<Mutex<usize>>,
    empty_condvar: Arc<(Mutex<usize>, Condvar)>,
}

impl ThreadPoolInner {
    /// Increment the job count.
    fn start_job(&self) {
        let count = Arc::clone(&self.job_count);
        let mut data = count.lock().unwrap();
        *data += 1;
        //println!("[tpool] add (job count: {})", data);
    }

    /// Decrement the job count.
    fn finish_job(&self) {
        let count = Arc::clone(&self.job_count);
        let mut data = count.lock().unwrap();
        *data -= 1;
        //println!("[tpool] finish (job count: {})", data);
    }

    /// Wait until the job count becomes 0.
    ///
    /// NOTE: We can optimize this function by adding another field to `ThreadPoolInner`, but let's
    /// not care about that in this homework.
    fn wait_empty(&self) {
        loop {
            let count = Arc::clone(&self.job_count);
            let data = count.lock().unwrap();
            if *data == 0 {
                break;
            }
        }
    }
}

/// Thread pool.
#[derive(Debug)]
pub struct ThreadPool {
    workers: Vec<Worker>,
    job_sender: Sender<Message>,
    pool_inner: Arc<ThreadPoolInner>,
}

impl ThreadPool {
    /// Create a new ThreadPool with `size` threads. Panics if the size is 0.
    pub fn new(size: usize) -> Self {
        assert!(size > 0);
        let (sender, receiver) = unbounded();
        let job_sender = sender;
        let job_count = Arc::new(Mutex::new(0));
        let empty_condvar = Arc::new((Mutex::new(0), Condvar::new()));
        let pool_inner = ThreadPoolInner {
            job_count,
            empty_condvar,
        };
        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            workers.push(Worker::new(id, receiver.clone()));
        }
        ThreadPool {
            workers: workers,
            job_sender: job_sender,
            pool_inner: Arc::new(pool_inner),
        }
    }

    /// Execute a new job in the thread pool.
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);
        self.pool_inner.start_job();

        let pair = Arc::clone(&self.pool_inner.empty_condvar);
        let pair2 = pair.clone();
        self.job_sender.send(Message::NewJob(job, pair2)).unwrap();
        let pool_inner = Arc::clone(&self.pool_inner);

        thread::spawn(move || {
            let (lock, cvar) = &*pair;
            let data = lock.lock().unwrap();
            cvar.wait(data);
            pool_inner.finish_job();
        });
    }

    /// Block the current thread until all jobs in the pool have been executed.  NOTE: This method
    /// has nothing to do with `JoinHandle::join`.
    pub fn join(&self) {
        self.pool_inner.wait_empty()
    }
}

impl Drop for ThreadPool {
    /// When dropped, all worker threads' `JoinHandle` must be `join`ed. If the thread panicked,
    /// then this function should panic too.
    fn drop(&mut self) {
        //println!("Sending terminate message to all workers.");
        // sending terminate message
        for _ in &mut self.workers {
            self.job_sender.send(Message::Terminate).unwrap();
        }
        //println!("Shutting down workers.");
        self.join()
    }
}

#[cfg(test)]
mod test {
    use super::ThreadPool;
    use crossbeam_channel::bounded;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread::sleep;
    use std::time::Duration;

    const NUM_THREADS: usize = 4;
    const NUM_JOBS: usize = 1024;

    #[test]
    fn thread_pool_parallel() {
        let pool = ThreadPool::new(NUM_THREADS);
        let barrier = Arc::new(Barrier::new(NUM_THREADS));
        let (done_sender, done_receiver) = bounded(NUM_THREADS);
        for _ in 0..NUM_THREADS {
            let barrier = barrier.clone();
            let done_sender = done_sender.clone();
            pool.execute(move || {
                barrier.wait();
                done_sender.send(()).unwrap();
            });
        }
        for _ in 0..NUM_THREADS {
            done_receiver.recv_timeout(Duration::from_secs(3)).unwrap();
        }
    }

    // Run jobs that take NUM_JOBS milliseconds as a whole.
    fn run_jobs(pool: &ThreadPool, counter: &Arc<AtomicUsize>) {
        for _ in 0..NUM_JOBS {
            let counter = counter.clone();
            pool.execute(move || {
                sleep(Duration::from_millis(NUM_THREADS as u64));
                counter.fetch_add(1, Ordering::Relaxed);
            });
        }
    }

    /// `join` blocks until all jobs are finished.
    #[test]
    fn thread_pool_join_block() {
        let pool = ThreadPool::new(NUM_THREADS);
        let counter = Arc::new(AtomicUsize::new(0));
        run_jobs(&pool, &counter);
        pool.join();
        assert_eq!(counter.load(Ordering::Relaxed), NUM_JOBS);
    }

    /// `drop` blocks until all jobs are finished.
    #[test]
    fn thread_pool_drop_block() {
        let pool = ThreadPool::new(NUM_THREADS);
        let counter = Arc::new(AtomicUsize::new(0));
        run_jobs(&pool, &counter);
        drop(pool);
        assert_eq!(counter.load(Ordering::Relaxed), NUM_JOBS);
    }

    /// This indirectly tests if the worker threads' `JoinHandle`s are joined when the pool is
    /// dropped.
    #[test]
    #[should_panic]
    fn thread_pool_drop_propagate_panic() {
        let pool = ThreadPool::new(NUM_THREADS);
        pool.execute(move || {
            panic!();
        });
    }
}
