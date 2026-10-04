//! Pipeline metadata and grants, composed with the database domain's writer.

use crate::domain::pipelines::{
    Pipeline, PipelineBlueprint, PipelineRecordType, PipelineRepo, PipelineSharing,
};
use databases::domain::provisioning::ManagedDatabaseWriter;
use entity_access_db_utils::{AccessLevel, EntityAccessSourceType, EntityType};
use models_databases::{DatabaseId, TableId};
use sqlx::{PgPool, Postgres, Transaction};
use std::collections::HashMap;
use uuid::Uuid;

/// Pipeline persistence errors.
#[derive(Debug, thiserror::Error)]
pub enum PipelineRepoError {
    /// SQL failure rolls back the entire operation.
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    /// Database provisioning failure.
    #[error("Database provisioning failed: {0}")]
    Database(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Owns only CRM metadata; database storage is accessed through its domain port.
pub struct PgPipelineRepo<Database> {
    pub(super) pool: PgPool,
    pub(super) database: Database,
}

impl<Database> PgPipelineRepo<Database> {
    /// Construct at the application's composition root.
    pub fn new(pool: PgPool, database: Database) -> Self {
        Self { pool, database }
    }
}

impl<Database> PgPipelineRepo<Database>
where
    Database: ManagedDatabaseWriter<Transaction = Transaction<'static, Postgres>>,
{
    pub(super) async fn create_in(
        &self,
        transaction: &mut Transaction<'static, Postgres>,
        blueprint: &PipelineBlueprint,
    ) -> Result<(), PipelineRepoError> {
        self.database
            .create_managed_database_in(transaction, &blueprint.database)
            .await
            .map_err(|error| PipelineRepoError::Database(Box::new(error)))?;
        sqlx::query!(
            "INSERT INTO crm_pipelines (id, team_id, database_id, table_id, record_type) VALUES ($1, $2, $3, $4, $5)",
            blueprint.id, blueprint.team_id, blueprint.database.id.into_uuid(),
            blueprint.database.table_id.into_uuid(), blueprint.record_type.as_str(),
        ).execute(&mut **transaction).await?;
        entity_access_db_utils::insert_entity_access_row(
            transaction,
            &blueprint.id,
            EntityType::CrmPipeline,
            blueprint.database.creator.as_ref(),
            EntityAccessSourceType::User,
            AccessLevel::Owner,
        )
        .await?;
        if blueprint.sharing == PipelineSharing::Team {
            entity_access_db_utils::team_share::upsert_direct(
                transaction,
                &blueprint.id,
                EntityType::CrmPipeline,
                blueprint.team_id,
                AccessLevel::Edit,
            )
            .await?;
        }
        Ok(())
    }
}

impl<Database> PipelineRepo for PgPipelineRepo<Database>
where
    Database: ManagedDatabaseWriter<Transaction = Transaction<'static, Postgres>>,
{
    type Error = PipelineRepoError;

    async fn crm_enabled(&self, team: Uuid) -> Result<bool, Self::Error> {
        Ok(sqlx::query_scalar!(
            "SELECT crm_enabled FROM team_crm_settings WHERE team_id = $1",
            team
        )
        .fetch_optional(&self.pool)
        .await?
        .unwrap_or(false))
    }

    async fn get_many(&self, ids: &[Uuid]) -> Result<Vec<Pipeline>, Self::Error> {
        let rows = sqlx::query!(
            r#"SELECT p.id, p.team_id, p.record_type, p.database_id, p.table_id,
                      EXISTS (SELECT 1 FROM entity_access ea WHERE ea.entity_id = p.id
                        AND ea.entity_type = 'crm_pipeline' AND ea.source_type = 'team'
                        AND ea.source_id = p.team_id::text) AS "team_shared!"
               FROM crm_pipelines p WHERE p.id = ANY($1) ORDER BY p.id"#,
            ids,
        )
        .fetch_all(&self.pool)
        .await?;
        let ids = rows
            .iter()
            .map(|row| DatabaseId::from_uuid(row.database_id))
            .collect::<Vec<_>>();
        let mut metadata: HashMap<_, _> = self
            .database
            .managed_databases(&ids)
            .await
            .map_err(|error| PipelineRepoError::Database(Box::new(error)))?
            .into_iter()
            .map(|database| (database.id, database))
            .collect();
        rows.into_iter()
            .map(|row| {
                let database = metadata
                    .remove(&DatabaseId::from_uuid(row.database_id))
                    .ok_or(sqlx::Error::RowNotFound)?;
                let record_type = match row.record_type.as_str() {
                    "company" => PipelineRecordType::Company,
                    "contact" => PipelineRecordType::Contact,
                    _ => {
                        return Err(
                            sqlx::Error::Decode("Invalid pipeline record type".into()).into()
                        );
                    }
                };
                Ok(Pipeline {
                    id: row.id,
                    team_id: row.team_id,
                    name: database.name,
                    record_type,
                    database_id: DatabaseId::from_uuid(row.database_id),
                    table_id: TableId::from_uuid(row.table_id),
                    primary_column_id: database.primary_column_id,
                    created_at: database.created_at,
                    trashed_at: database.trashed_at,
                    sharing: if row.team_shared {
                        PipelineSharing::Team
                    } else {
                        PipelineSharing::Private
                    },
                })
            })
            .collect()
    }

    #[tracing::instrument(skip_all, err)]
    async fn create(&self, blueprint: &PipelineBlueprint) -> Result<(), Self::Error> {
        let mut transaction = self.pool.begin().await?;
        entity_access_db_utils::team_share::acquire_guard(&mut transaction).await?;
        self.create_in(&mut transaction, blueprint).await?;
        transaction.commit().await?;
        Ok(())
    }

    #[tracing::instrument(skip_all, err)]
    async fn share(
        &self,
        pipeline: &Pipeline,
        sharing: PipelineSharing,
    ) -> Result<(), Self::Error> {
        let mut transaction = self.pool.begin().await?;
        entity_access_db_utils::team_share::acquire_guard(&mut transaction).await?;
        // Serialize sharing with pipeline deletion.
        sqlx::query!(
            "SELECT id FROM crm_pipelines WHERE id = $1 FOR UPDATE",
            pipeline.id
        )
        .fetch_one(&mut *transaction)
        .await?;
        match sharing {
            PipelineSharing::Team => {
                entity_access_db_utils::team_share::upsert_direct(
                    &mut transaction,
                    &pipeline.id,
                    EntityType::CrmPipeline,
                    pipeline.team_id,
                    AccessLevel::Edit,
                )
                .await?
            }
            PipelineSharing::Private => {
                entity_access_db_utils::team_share::delete_direct(
                    &mut transaction,
                    &pipeline.id,
                    EntityType::CrmPipeline,
                    pipeline.team_id,
                )
                .await?
            }
        }
        transaction.commit().await?;
        Ok(())
    }
}
