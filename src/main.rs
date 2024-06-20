///
/// Super basic producer consumer pattern using mpsc
/// Simple fanout/fanin work handler
///
/// producer 1:1 - main  - 1:M - worker - M:1 - main
///
use std::sync::mpsc::{self, Sender};
use std::thread;

use rand::prelude::*;
use std::io;
use std::time;

#[derive(Debug)]
enum ProducerMessage {
    Item(String),
}

#[derive(Debug)]
enum WorkerMessage {
    Item(String),
}

fn producer(work_sender: Sender<ProducerMessage>) -> Sender<ProducerMessage> {
    // we create a send channel... we don't care about the receiver
    let (s, _) = mpsc::channel();
    _ = thread::spawn(move || {
        for i in 0..10 {
            let x = rand::thread_rng().gen_range(0..800);
            thread::sleep(time::Duration::from_millis(x));
            let message = ProducerMessage::Item(format!("Item: {}", i));
            println!("Produced: {:?}", message);
            work_sender.send(message).unwrap();
        }
    });
    s
}

fn worker(work_sender: Sender<WorkerMessage>) -> Sender<ProducerMessage> {
    // we want to send a Producer Message into this worker ...
    // 1. set up a channel
    // 2. hold onto the reciver in the lanuched thread
    //      2.a The launched thread should
    let (s, r) = mpsc::channel();
    _ = thread::spawn(move || {
        for item in r {
            println!("Recv: {:?}", item);

            let x = rand::thread_rng().gen_range(0..1000);
            thread::sleep(time::Duration::from_millis(x));
            work_sender
                .send(WorkerMessage::Item(format!(
                    "Processed: {:?} from {:?}",
                    item,
                    thread::current()
                )))
                .unwrap();
        }
        println!("thread {:?} finished", thread::current());
    });
    s
}

fn main() {
    // single that "produces" a message
    // in this design we are 1:1 .. it just happens to be on another thread...
    let (prod_sender, prod_reciever) = mpsc::channel::<ProducerMessage>();

    // launch the producer
    let _ = producer(prod_sender);

    // spawn some workers
    // we must clone the sender... main owns these channels and will need to clean up. This is critical.
    let (work_sender, work_receiver) = mpsc::channel();
    let worker_chans = vec![
        work_sender.clone(),
        work_sender.clone(),
        work_sender.clone(),
    ];

    // create the worker pool.
    // this is just a vec of the returned send channels for each worker thread.
    let mut workers = vec![];
    for sender in worker_chans {
        workers.push(worker(sender));
    }

    // start receiving from our producer
    // mpsc senders never block so this will assign work randomly to one of the worker threads
    // as fast as possible
    for message in prod_reciever {
        let i = rand::thread_rng().gen_range(0..3);
        workers[i].send(message).unwrap();
    }

    // In this app we must drop the OG worker and its pool explicitly here (or implicitly with a scope)
    // before we start receiving...
    // otherwise the receivers will never break out of their loop
    drop(workers);
    drop(work_sender);

    // gather the work from the threads here...
    // based on the above design these should come in very quickly.
    for message in work_receiver {
        println!("Recv on main: {:?}", message)
    }
}
