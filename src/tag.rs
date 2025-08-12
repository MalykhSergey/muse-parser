use crate::{db::Item};
use anyhow::Result;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio_postgres::Client;



pub struct Tag {
    xml_id: i64,
    xml_name: Option<String>,
    xml_post_id: Option<i64>
}


pub fn parse_tags(attrs: HashMap<String, String>, tx: &mpsc::Sender<Item>) -> Result<()> {
    if let Some(id_s) = attrs.get("Id") {
        let id: i64 = id_s.parse().unwrap_or(0);
        let tag_name: Option<String> = attrs
            .get("TagName").cloned();
        let post_id = attrs.get("ExcerptPostId").and_then(|s| s.parse().ok());
        let t = Tag {
            xml_id: id,
            xml_name: tag_name,
            xml_post_id: post_id
        };

        let _ = tx.try_send(Item::Tag(t));
    }
    Ok(())
}

pub async fn flush_tags(client: &mut Client, batch: &mut Vec<Tag>) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let txn = client.transaction().await?;
    let stmt = txn
        .prepare(
            "INSERT INTO tags (id, name, post_id)
         VALUES ($1, $2, $3)",
        )
        .await?;
    for t in batch.drain(..) {
        txn.execute(
            &stmt,
            &[
                &t.xml_id,
                &t.xml_name,
                &t.xml_post_id
            ],
        )
        .await?;
    }
    txn.commit().await?;
    Ok(())
}