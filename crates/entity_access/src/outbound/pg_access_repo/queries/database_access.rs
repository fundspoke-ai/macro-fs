//! Query for database access level.

use crate::{domain::models::AccessLevel, outbound::pg_access_repo::queries::SourceIds};
use sqlx::PgPool;
use std::collections::BTreeMap;
use std::str::FromStr;
use uuid::Uuid;

/// The grant-bearing owner of a database; standalone databases own themselves.
pub async fn access_owner(
    pool: &PgPool,
    id: Uuid,
) -> Result<Option<(Uuid, model_entity::EntityType)>, sqlx::Error> {
    let row = sqlx::query!(
        "SELECT COALESCE(access_entity_id, id) AS id, COALESCE(access_entity_type, 'database') AS kind FROM databases WHERE id = $1", id
    ).fetch_optional(pool).await?;
    row.map(|row| {
        let kind = row
            .kind
            .unwrap_or_else(|| "database".into())
            .parse()
            .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        Ok((row.id.unwrap_or(id), kind))
    })
    .transpose()
}

#[cfg(test)]
mod test;

/// The highest access level `source_ids` hold on a database. A database has no
/// `SharePermission`, so there is no public-sharing arm.
#[tracing::instrument(err, skip(pool, source_ids))]
pub async fn get_database_access(
    pool: &PgPool,
    database_id: &uuid::Uuid,
    source_ids: &SourceIds,
) -> Result<Option<AccessLevel>, sqlx::Error> {
    if source_ids.0.is_empty() {
        return Ok(None);
    }

    let all_level_strings: Vec<Option<String>> = sqlx::query_scalar!(
        r#"
        SELECT ea.access_level::text
        FROM databases d JOIN entity_access ea
          ON ea.entity_id = COALESCE(d.access_entity_id, d.id)
         AND ea.entity_type = COALESCE(d.access_entity_type, 'database')
        WHERE d.id = $1 AND ea.source_id = ANY($2)
        "#,
        database_id,
        &source_ids.0,
    )
    .fetch_all(pool)
    .await?;

    highest_access_level(&all_level_strings)
}

/// The highest of a set of `access_level::text` values; an unknown level is a decode error.
pub(super) fn highest_access_level(
    levels: &[Option<String>],
) -> Result<Option<AccessLevel>, sqlx::Error> {
    levels
        .iter()
        .flatten()
        .map(|level| {
            AccessLevel::from_str(level).map_err(|error| sqlx::Error::Decode(error.into()))
        })
        .try_fold(None, |highest: Option<AccessLevel>, level| {
            Ok(highest.max(Some(level?)))
        })
}

/// The highest access level a user has on every database their source ids
/// reach, ordered by database id. Trash is the databases domain's concern:
/// a trashed database's grants are still listed.
#[tracing::instrument(err, skip(pool, source_ids))]
pub async fn list_database_access(
    pool: &PgPool,
    source_ids: &SourceIds,
) -> Result<Vec<(Uuid, AccessLevel)>, sqlx::Error> {
    if source_ids.0.is_empty() {
        return Ok(Vec::new());
    }

    let rows = sqlx::query!(
        r#"
        SELECT d.id AS "entity_id!", ea.access_level AS "access_level!: AccessLevel"
        FROM databases d JOIN entity_access ea
          ON ea.entity_id = COALESCE(d.access_entity_id, d.id)
         AND ea.entity_type = COALESCE(d.access_entity_type, 'database')
        WHERE ea.source_id = ANY($1)
        "#,
        &source_ids.0,
    )
    .fetch_all(pool)
    .await?;

    let mut highest: BTreeMap<Uuid, AccessLevel> = BTreeMap::new();
    for row in rows {
        highest
            .entry(row.entity_id)
            .and_modify(|level| *level = (*level).max(row.access_level))
            .or_insert(row.access_level);
    }
    Ok(highest.into_iter().collect())
}
