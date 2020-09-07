//! Thread pool that joins all thread when dropped.

#![allow(clippy::mutex_atomic)]

// NOTE: Crossbeam channels are MPMC, which means that you don't need to wrap the receiver in
// Arc<Mutex<..>>. Just clone the receiver and give it to each worker thread.
use crossbeam_channel::{unbounded, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

enum Message {
    NewJob(Job),
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
    /// When dropped, the thread should be `join`ed. NOTE that the thread is detached if not
    /// `join`ed explicitly.
    fn drop(&mut self) {
        // join worker thread
        println!("[Worker {}] joined", self.id);

        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

impl Worker {
    fn new(id: usize, receiver: Receiver<Message>) -> Worker {
        let thread = thread::spawn(move || loop {
            let message = receiver.recv().unwrap();
            match message {
                Message::NewJob(job) => {
                    println!("[Worker {}] starts a job.", id);
                    job.call_box();
                    println!("[Worker {}] finishes a job.", id);
                }
                Message::Terminate => {
                    println!("[Worker {}] was terminating.", id);
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
    job_count: Mutex<usize>,
    //empty_condvar: Condvar,
}

impl ThreadPoolInner {
    /// Increment the job count.
    fn start_job(&self) {
        let mut data = self.job_count.lock().unwrap();
        *data += 1;
        println!("[tpool] add (job count: {})", data);
    }

    /// Decrement the job count.
    fn finish_job(&self) {
        let mut data = self.job_count.lock().unwrap();
        *data -= 1;
        println!("[tpool] finish (job count: {})", data);
    }

    /// Wait until the job count becomes 0.
    ///
    /// NOTE: We can optimize this function by adding another field to `ThreadPoolInner`, but let's
    /// not care about that in this homework.
    fn wait_empty(&self) {
        todo!()
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
        let mut job_count = Mutex::new(0);
        let mut pool_inner = ThreadPoolInner { job_count };
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
        self.job_sender.send(Message::NewJob(job)).unwrap();
        self.pool_inner.finish_job();
    }

    /// Block the current thread until all jobs in the pool have been executed.
    pub fn join(&self) {
        todo!()
    }
}

impl Drop for ThreadPool {
    /// When dropped, all worker threads must be properly `join`ed.
    fn drop(&mut self) {
        println!("Sending terminate message to all workers.");
        // sending terminate message
        for _ in &mut self.workers {
            self.job_sender.send(Message::Terminate).unwrap();
        }
        println!("Shutting down workers.");
        // join worker thread
        /*for worker in &mut self.workers {
            println!("Shutting down worker {}", worker.id);

            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }*/
    }
}
