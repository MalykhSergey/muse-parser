use crate::db::Item;
use anyhow::Result;
use postgres_types::{to_sql_checked, ToSql};
use std::{collections::HashMap, fmt::Display};
use tokio::sync::mpsc;
use tokio_postgres::Client;

#[derive(Debug)]
pub enum PostType {
    QUESTION,
    ANSWER,
}

impl Display for PostType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PostType::QUESTION => write!(f, "QUESTION"),
            PostType::ANSWER => write!(f, "ANSWER"),
        }
    }
}

impl ToSql for PostType {
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
        return ty.name() == "post_type";
    }

    to_sql_checked!();
}

pub struct Post {
    xml_id: i64,
    post_type_id: i32,
    owner_user_xml_id: Option<i64>,
    title: Option<String>,
    body: Option<String>,
    tags: Option<String>,
    creation_date: Option<String>,
    last_activity_date: Option<String>,
    parent_xml_id: Option<i64>,
    accepted_answer_xml_id: Option<i64>,
}

pub fn parse_posts(attrs: HashMap<String, String>, tx: &mpsc::Sender<Item>) -> Result<()> {
    if let Some(id_s) = attrs.get("Id") {
        let id: i64 = id_s.parse().unwrap_or(0);
        let post_type_id = attrs
            .get("PostTypeId")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let owner = attrs.get("OwnerUserId").and_then(|s| s.parse().ok());
        let title = attrs.get("Title").cloned();
        let body = attrs.get("Body").cloned();
        let tags = attrs.get("Tags").cloned();
        let creation_date = attrs.get("CreationDate").cloned();
        let last_activity_date = attrs.get("LastActivityDate").cloned();
        let parent_xml_id = attrs.get("ParentId").and_then(|s| s.parse().ok());
        let accepted_answer_xml_id = attrs.get("AcceptedAnswerId").and_then(|s| s.parse().ok());

        let p = Post {
            xml_id: id,
            post_type_id,
            owner_user_xml_id: owner,
            title,
            body,
            tags,
            creation_date,
            last_activity_date,
            parent_xml_id,
            accepted_answer_xml_id,
        };
        if post_type_id <= 2 {
            let _ = tx.try_send(Item::Post(p));
        }
    }
    Ok(())
}

pub async fn flush_posts(client: &mut Client, batch: &mut Vec<Post>) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let txn = client.transaction().await?;

    let stmt = txn
        .prepare(
            "
        INSERT INTO posts
            (id, title, body, post_type, author_id, parent_id, answer_id, created, updated)
        VALUES ($1, $2, $3, $4, $5,$6, $7, $8, $9)",
        )
        .await?;

    for p in batch.drain(..) {
        let post_type_str = match p.post_type_id {
            1 => PostType::QUESTION,
            2 => PostType::ANSWER,
            _ => PostType::ANSWER,
        };
        let created = parse_ts(&p.creation_date);
        let updated = parse_ts(&p.last_activity_date);

        txn.execute(
            &stmt,
            &[
                &p.xml_id,
                &p.title,
                &p.body,
                &post_type_str,
                &p.owner_user_xml_id,
                &p.parent_xml_id,
                &p.accepted_answer_xml_id,
                &created,
                &updated,
            ],
        )
        .await?;

        if let Some(tags_raw) = p.tags {
            let tag_names = parse_tags_from_field(&tags_raw);
            for tag_name in tag_names {
                let t_stmt = txn
                    .prepare(
                        "
                        WITH tag AS (
                            SELECT id FROM tags WHERE name = $2
                        )
                        INSERT INTO posts_tags (post_id, tag_id)
                        SELECT $1, id FROM tag
                        WHERE EXISTS (SELECT 1 FROM tag);",
                    )
                    .await?;
                txn.execute(&t_stmt, &[&p.xml_id, &tag_name]).await?;
            }
        }
    }

    txn.commit().await?;
    Ok(())
}

fn parse_ts(opt: &Option<String>) -> Option<chrono::NaiveDateTime> {
    opt.as_ref()
        .and_then(|s| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f").ok())
}

fn parse_tags_from_field(raw: &str) -> Vec<String> {
    raw.trim_matches('|')
        .split('|')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}
