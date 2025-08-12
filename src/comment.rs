use crate::db::Item;
use anyhow::Result;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio_postgres::Client;


pub struct Comment {
    xml_id: i64,
    post_xml_id: i64,
    user_xml_id: Option<i64>,
    text: Option<String>,
    creation_date: Option<String>,
}

pub fn parse_comments(attrs: HashMap<String, String>, tx: &mpsc::Sender<Item>) -> Result<()> {
    if let Some(id_s) = attrs.get("Id") {
        let id: i64 = id_s.parse().unwrap_or(0);
        let post_id: i64 = attrs
            .get("PostId")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let user_id = attrs.get("UserId").and_then(|s| s.parse().ok());
        let text = attrs.get("Text").cloned();
        let creation_date = attrs.get("CreationDate").cloned();
        let c = Comment {
            xml_id: id,
            post_xml_id: post_id,
            user_xml_id: user_id,
            text,
            creation_date,
        };

        let _ = tx.try_send(Item::Comment(c));
    }
    Ok(())
}

pub async fn flush_comments(client: &mut Client, batch: &mut Vec<Comment>) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let txn = client.transaction().await?;
    let stmt = txn
        .prepare(
            "INSERT INTO comments (id, body, author_id, post_id, created, updated)
         VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .await?;
    for c in batch.drain(..) {
        txn.execute(
            &stmt,
            &[
                &c.xml_id,
                &c.text,
                &c.user_xml_id,
                &c.post_xml_id,
                &parse_ts(&c.creation_date),
                &parse_ts(&c.creation_date),
            ],
        )
        .await?;
    }
    txn.commit().await?;
    Ok(())
}


fn parse_ts(opt: &Option<String>) -> Option<chrono::NaiveDateTime> {
    opt.as_ref()
        .and_then(|s| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f").ok())
}
