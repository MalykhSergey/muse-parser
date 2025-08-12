use std::collections::HashMap;

use crate::{db::Item};
use anyhow::Result;
use tokio::sync::mpsc;
use tokio_postgres::Client;


pub struct User {
    xml_id: i64,
    display_name: Option<String>,
}

pub fn parse_users(attrs: HashMap<String, String>, tx: &mpsc::Sender<Item>) -> Result<()> {
    if let Some(id_s) = attrs.get("Id") {
        let id: i64 = id_s.parse().unwrap_or(0);
        let name = attrs.get("DisplayName").cloned();
        let user = User {
            xml_id: id,
            display_name: name,
        };
        let _ = tx.try_send(Item::User(user));
    }
    Ok(())
}

pub async fn flush_users(client: &mut Client, batch: &mut Vec<User>) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let txn = client.transaction().await?;
    let stmt = txn.prepare(
        "INSERT INTO users (id, user_type, external_id, name) VALUES ($1, 'EXTERNAL', $1, $2) ON CONFLICT (id) DO NOTHING"
    ).await?;
    for u in batch.drain(..) {
        let name = u.display_name.as_deref();
        txn.execute(&stmt, &[&u.xml_id, &name]).await?;
    }
    txn.commit().await?;
    Ok(())
}
