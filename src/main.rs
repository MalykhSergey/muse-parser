use anyhow::Result;
use quick_xml::events::Event;
use quick_xml::Reader;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::time::Instant;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio::sync::mpsc;
use tokio_postgres::NoTls;

use crate::comment::*;
use crate::db::*;
use crate::post::*;
use crate::tag::*;
use crate::user::*;
use crate::vote::*;

mod comment;
mod db;
mod post;
mod tag;
mod user;
mod vote;

#[tokio::main]
async fn main() -> Result<()> {
    let start_time = Instant::now();
    let args: Vec<String> = env::args().collect();
    let input_dir = args.get(1).expect("Pass directory to dump");
    let db_connection_string = args.get(2).expect("Pass connection string. Example: host=127.0.0.1 port=5432 user=spring password=boot dbname=muse");

    let (pg_client, connection) = tokio_postgres::connect(
        &db_connection_string,
        NoTls,
    )
    .await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Postgres connection error: {}", e);
        }
    });

    // создаём канал для передачи элементов
    let (tx, mut rx) = mpsc::channel::<Item>(100_000);

    // DB worker: принимает items и вставляет батчами
    let db_client = pg_client;
    let db_handle = tokio::spawn(async move {
        if let Err(e) = db_worker(db_client, &mut rx).await {
            eprintln!("DB worker error: {:?}", e);
        }
    });

    parse_xml_and_send(
        PathBuf::from(format!("{}/Users.xml", input_dir)),
        tx.clone(),
        parse_users,
    )
    .await?;
    parse_xml_and_send(
        PathBuf::from(format!("{}/Tags.xml", input_dir)),
        tx.clone(),
        parse_tags,
    )
    .await?;
    parse_xml_and_send(
        PathBuf::from(format!("{}/Posts.xml", input_dir)),
        tx.clone(),
        parse_posts,
    )
    .await?;
    parse_xml_and_send(
        PathBuf::from(format!("{}/Comments.xml", input_dir)),
        tx.clone(),
        parse_comments,
    )
    .await?;
    parse_xml_and_send(
        PathBuf::from(format!("{}/Votes.xml", input_dir)),
        tx.clone(),
        parse_votes,
    )
    .await?;

    drop(tx);
    db_handle.await?;
    let duration = start_time.elapsed();
    println!("Elapsed: {:?}", duration);
    Ok(())
}

async fn parse_xml_and_send<F>(path: PathBuf, tx: mpsc::Sender<Item>, mut handler: F) -> Result<()>
where
    F: FnMut(HashMap<String, String>, &mpsc::Sender<Item>) -> Result<()> + Send,
{
    if !path.exists() {
        println!("File not found: {:?}", path);
        return Ok(());
    }
    let f = File::open(&path).await?;
    let buf_reader = BufReader::with_capacity(32 * 1024 * 1024, f);
    let mut reader = Reader::from_reader(buf_reader);

    let mut buf_event = Vec::new();
    loop {
        match reader.read_event_into_async(&mut buf_event).await? {
            Event::Empty(ref e) | Event::Start(ref e) if e.name().as_ref() == b"row" => {
                let mut attrs = HashMap::new();
                for a in e.attributes().with_checks(false) {
                    if let Ok(att) = a {
                        let key = String::from_utf8_lossy(att.key.as_ref()).to_string();
                        let val = att.unescape_value().unwrap_or_default().into_owned();
                        attrs.insert(key, val);
                    }
                }
                handler(attrs, &tx)?;
            }
            Event::Eof => break,
            _ => {}
        }
        buf_event.clear();
    }

    Ok(())
}
