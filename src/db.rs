use crate::comment::*;
use crate::post::*;
use crate::tag::*;
use crate::user::*;
use crate::vote::*;
use anyhow::Result;
use tokio::sync::mpsc;
use tokio_postgres::Client;
const BATCH_SIZE: usize = 500;


pub enum Item {
    User(User),
    Post(Post),
    Comment(Comment),
    Vote(Vote),
    Tag(Tag),
}


pub async fn db_worker(mut client: Client, rx: &mut mpsc::Receiver<Item>) -> Result<()> {
    use Item::*;
    let mut users_batch = Vec::with_capacity(BATCH_SIZE);
    let mut posts_batch = Vec::with_capacity(BATCH_SIZE);
    let mut comments_batch = Vec::with_capacity(BATCH_SIZE);
    let mut votes_batch = Vec::with_capacity(BATCH_SIZE);
    let mut tags_batch = Vec::with_capacity(BATCH_SIZE);

    while let Some(item) = rx.recv().await {
        match item {
            User(u) => {
                users_batch.push(u);
                if users_batch.len() >= BATCH_SIZE {
                    flush_users(&mut client, &mut users_batch).await?;
                }
            }
            Post(p) => {
                posts_batch.push(p);
                if posts_batch.len() >= BATCH_SIZE {
                    flush_posts(&mut client, &mut posts_batch).await?;
                }
            }
            Comment(c) => {
                comments_batch.push(c);
                if comments_batch.len() >= BATCH_SIZE {
                    flush_comments(&mut client, &mut comments_batch).await?;
                }
            }
            Vote(v) => {
                votes_batch.push(v);
                if votes_batch.len() >= BATCH_SIZE {
                    flush_votes(&mut client, &mut votes_batch).await?;
                }
            }
            Tag(v) => {
                tags_batch.push(v);
                if tags_batch.len() >= BATCH_SIZE {
                    flush_tags(&mut client, &mut tags_batch).await?
                }
            }
        }
    }

    flush_users(&mut client, &mut users_batch).await?;
    flush_tags(&mut client, &mut tags_batch).await?;
    flush_posts(&mut client, &mut posts_batch).await?;
    flush_comments(&mut client, &mut comments_batch).await?;
    flush_votes(&mut client, &mut votes_batch).await?;

    println!("DB worker finished");
    Ok(())
}
