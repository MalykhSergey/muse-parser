use crate::{db::Item};
use anyhow::Result;
use std::{collections::HashMap, fmt::Display};
use tokio::sync::mpsc;
use tokio_postgres::Client;
use postgres_types::{to_sql_checked, ToSql};



pub struct Vote {
    post_xml_id: i64,
    vote_type_id: i32,
    user_xml_id: Option<i64>,
    creation_date: Option<String>,
}
#[derive(Debug)]
pub enum VoteType{
    POSITIVE,NEGATIVE
}

impl Display for VoteType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VoteType::POSITIVE => write!(f, "POSITIVE"),
            VoteType::NEGATIVE => write!(f, "NEGATIVE"),
        }
    }
}

impl ToSql for VoteType {
    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        format!("{}", self).to_sql(ty, out)
    }

    fn accepts(ty: &postgres_types::Type) -> bool
    where
        Self: Sized,
    {
        return ty.name() == "vote_type";
    }

    to_sql_checked!();
}

pub fn parse_votes(attrs: HashMap<String, String>, tx: &mpsc::Sender<Item>) -> Result<()> {
    if let Some(_id_s) = attrs.get("Id") {
        let post_id: i64 = attrs.get("PostId").and_then(|s| s.parse().ok()).unwrap_or(0);
        let vote_type_id = attrs.get("VoteTypeId").and_then(|s| s.parse().ok()).unwrap_or(0);
        let user_id = attrs.get("UserId").and_then(|s| s.parse().ok());
        let creation_date = attrs.get("CreationDate").cloned();
        let v = Vote { post_xml_id: post_id, vote_type_id, user_xml_id: user_id, creation_date };
        let _ = tx.try_send(Item::Vote(v));
    }
    Ok(())
}

pub async fn flush_votes(client: &mut Client, batch: &mut Vec<Vote>) -> Result<()> {
    if batch.is_empty() { return Ok(()); }
    let txn = client.transaction().await?;
    let stmt = txn.prepare(
        "INSERT INTO votes (author_id, post_id, created, type)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT DO NOTHING"
    ).await?;
    for v in batch.drain(..) {
        let vote_type = match v.vote_type_id {
            2 => VoteType::POSITIVE,
            3 => VoteType::NEGATIVE,
            _ => VoteType::POSITIVE,
        };
        txn.execute(&stmt, &[&v.user_xml_id, &v.post_xml_id, &parse_ts(&v.creation_date), &vote_type]).await?;
    }
    txn.commit().await?;
    Ok(())
}

fn parse_ts(opt: &Option<String>) -> Option<chrono::NaiveDateTime> {
    opt.as_ref()
        .and_then(|s| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f").ok())
    }
