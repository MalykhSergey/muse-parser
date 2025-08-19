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

    if args.len() < 3 {
        eprintln!(
            "Usage: {} <input_dir> <connection_string> [files...]",
            args[0]
        );
        eprintln!(
            "Example: {} ./dump 'host=localhost user=postgres' Users Posts",
            args[0]
        );
        std::process::exit(1);
    }

    let input_dir = &args[1];
    let db_connection_string = &args[2];
    let files_to_parse: Vec<&str> = args.iter().skip(3).map(|s| s.as_str()).collect();

    let parse_all = files_to_parse.is_empty();

    let (pg_client, connection) = tokio_postgres::connect(db_connection_string, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Postgres connection error: {}", e);
        }
    });

    let (tx, mut rx) = mpsc::channel::<Item>(100_000);

    let db_client = pg_client;
    let db_handle = tokio::spawn(async move {
        if let Err(e) = db_worker(db_client, &mut rx).await {
            eprintln!("DB worker error: {:?}", e);
        }
    });
    type ParserFn = for<'a> fn(HashMap<String, String>, &'a mpsc::Sender<Item>) -> Result<()>;

    let file_parsers: Vec<(&str, ParserFn)> = vec![
        ("Users", user::parse_users as ParserFn),
        ("Tags", tag::parse_tags as ParserFn),
        ("Posts", post::parse_posts as ParserFn),
        ("Comments", comment::parse_comments as ParserFn),
        ("Votes", vote::parse_votes as ParserFn),
    ];

    for (file_name, parser) in file_parsers {
        if parse_all || files_to_parse.contains(&file_name) {
            let file_path = PathBuf::from(format!("{}/{}.xml", input_dir, file_name));
            parse_xml_and_send(file_path, tx.clone(), parser).await?;
        }
    }

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
