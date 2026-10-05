//! Storage entry points for consumers with their own entity and authorization.

use super::*;
use crate::domain::models::DatabaseId;
use crate::domain::ports::DatabaseStorage;

impl<Properties> DatabaseStorage for PgCellStore<Properties>
where
    Properties: PropertiesRepo<Err = anyhow::Error>
        + DatabaseCellWriter<Transaction = Transaction<'static, Postgres>>
        + DatabaseOptionWriter
        + DatabaseDefinitionWriter<Transaction = Transaction<'static, Postgres>>
        + Send
        + Sync
        + 'static,
{
    type Error = PgCellStoreError;

    #[tracing::instrument(err, skip(self))]
    async fn create_storage(&self) -> Result<DatabaseId, Self::Error> {
        let id = DatabaseId::new();
        sqlx::query!("INSERT INTO databases (id) VALUES ($1)", id.into_uuid())
            .execute(&self.pool)
            .await?;
        Ok(id)
    }

    async fn storage_row_page(
        &self,
        table: TableId,
        after: Option<RowId>,
        limit: i64,
    ) -> Result<Vec<crate::domain::models::RowRef>, Self::Error> {
        let rows = sqlx::query!(
            "SELECT id, position FROM database_rows WHERE table_id = $1 AND ($2::uuid IS NULL OR id > $2) ORDER BY id LIMIT $3",
            table.into_uuid(), after.map(RowId::into_uuid), limit,
        ).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|row| {
                Ok(crate::domain::models::RowRef {
                    id: RowId::from_uuid(row.id),
                    position: row.position.parse().map_err(PgDatabasesRepoError::from)?,
                })
            })
            .collect()
    }

    #[tracing::instrument(err, skip(self))]
    async fn delete_storage(&self, id: DatabaseId) -> Result<(), Self::Error> {
        let mut transaction = self.pool.begin().await?;
        schema::lock_database(&mut transaction, id, true).await?;
        journal::purge(&mut *transaction, id).await?;
        sqlx::query!("DELETE FROM databases WHERE id = $1", id.into_uuid())
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    #[tracing::instrument(err, skip(self, writes), fields(writes = writes.writes.len()))]
    async fn apply_storage_writes(&self, writes: &Writes) -> Result<WritesOutcome, Self::Error> {
        match self.apply_in(self.pool.begin().await?, writes).await {
            Err(error) => match storage_constraint(&error) {
                Some(message) => Ok(WritesOutcome::ConstraintViolation(message.into())),
                None => Err(error),
            },
            outcome => outcome,
        }
    }
}

// Core consumers may add relational invariants. Translate standard constraint
// failures without exposing SQL details or depending on the consumer's schema.
fn storage_constraint(mut error: &(dyn std::error::Error + 'static)) -> Option<&'static str> {
    loop {
        let code = if let Some(sqlx::Error::Database(db)) = error.downcast_ref::<sqlx::Error>() {
            db.code()
        } else if let Some(db) = error.downcast_ref::<Box<dyn sqlx::error::DatabaseError>>() {
            db.code()
        } else {
            error
                .downcast_ref::<sqlx::postgres::PgDatabaseError>()
                .map(|db| db.code().into())
        };
        match code.as_deref() {
            Some("23505") => return Some("This record or unique value is already in use."),
            Some("23514") => {
                return Some(
                    "Choose a valid record of the required type. Required records cannot be cleared.",
                );
            }
            Some("23503") => return Some("This change would remove a required reference."),
            _ => error = error.source()?,
        }
    }
}
