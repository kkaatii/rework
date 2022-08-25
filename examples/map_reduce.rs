use std::sync::mpsc::{channel, Sender};

use rework::Request;

#[derive(Debug)]
struct CountRequest {
    word: String,
    tx: Sender<usize>,
}

impl Request for CountRequest {}

async fn count(req: CountRequest) {
    let CountRequest { ref word, ref tx } = req;
    let n = word.chars().filter(|c| c != &' ').count();
    let _ = tx.send(n);
}

fn main() {
    let poem = r#"so much depends
    upon
    
    a red wheel
    barrow
    
    glazed with rain
    water
    
    beside the white
    chickens"#;

    let (tx, rx) = channel();

    let handle = rework::builder(|| count).build();
    for word in poem.lines() {
        handle.request(CountRequest {
            word: word.to_owned(),
            tx: tx.clone(),
        });
    }
    drop(tx);

    let sum = rx.iter().fold(0, |acc, x| acc + x);
    println!("{sum} characters counted!");
}
