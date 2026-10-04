//! Operator migration storage. CRM SQL stays here; property snapshots and
//! database provisioning go through their owning domains' transactional ports.

use super::pipelines::PgPipelineRepo;
use crate::domain::{
    pipelines::PipelineBlueprint,
    sales_backfill::{LegacySalesCompany, LegacySalesTeam, SalesBackfillStore},
};
use databases::domain::provisioning::{ManagedDatabaseSeeder, ProvisionedRow};
use macro_user_id::{cowlike::CowLike, user_id::MacroUserIdStr};
use models_properties::EntityType;
use properties::domain::property_snapshot_reader::{
    PropertySnapshotReader, SelectDefinitionSnapshot,
};
use sqlx::{PgPool, Postgres, Transaction};
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

/// Composed only into the operator CLI and integration tests.
pub struct PgSalesBackfillStore<D, P> {
    pipelines: PgPipelineRepo<D>,
    properties: P,
}

impl<D, P> PgSalesBackfillStore<D, P> {
    /// Wire the owning-domain adapters at the composition root.
    pub fn new(pool: PgPool, database: D, properties: P) -> Self {
        Self {
            pipelines: PgPipelineRepo::new(pool, database),
            properties,
        }
    }
}

impl<D, P> SalesBackfillStore for PgSalesBackfillStore<D, P>
where
    D: ManagedDatabaseSeeder<Transaction = Transaction<'static, Postgres>>,
    P: PropertySnapshotReader<Transaction = Transaction<'static, Postgres>>,
{
    type Transaction = Transaction<'static, Postgres>;

    async fn teams(&self, after: Option<Uuid>, limit: i64) -> Result<Vec<Uuid>, rootcause::Report> {
        Ok(sqlx::query_scalar!(
            "SELECT team_id FROM team_crm_settings WHERE crm_enabled AND ($1::uuid IS NULL OR team_id > $1) ORDER BY team_id LIMIT $2",
            after, limit,
        ).fetch_all(&self.pipelines.pool).await?)
    }

    async fn begin(&self) -> Result<Self::Transaction, rootcause::Report> {
        let mut tx = self.pipelines.pool.begin().await?;
        // Also serializes retries. Take the canonical team-share guard first,
        // as ownership and membership writers do; preserve READ COMMITTED.
        entity_access_db_utils::team_share::acquire_guard(&mut tx).await?;
        Ok(tx)
    }

    async fn team(
        &self,
        tx: &mut Self::Transaction,
        team: Uuid,
    ) -> Result<Option<LegacySalesTeam>, rootcause::Report> {
        let row = sqlx::query!(
            r#"SELECT t.id, t.owner_id, s.crm_enabled,
                 s.legacy_stage_ids AS "legacy_stage_ids: sqlx::types::Json<BTreeMap<Uuid, Uuid>>",
                 EXISTS(SELECT 1 FROM crm_sales_backfills b WHERE b.team_id = t.id) AS "completed!"
               FROM team t JOIN team_crm_settings s ON s.team_id = t.id
               WHERE t.id = $1 FOR UPDATE OF t, s"#,
            team,
        )
        .fetch_optional(&mut **tx)
        .await?;
        row.map(|row| {
            Ok(LegacySalesTeam {
                id: row.id,
                owner: MacroUserIdStr::parse_from_str(&row.owner_id)?.into_owned(),
                enabled: row.crm_enabled,
                completed: row.completed,
                legacy_stage_ids: row.legacy_stage_ids.0,
            })
        })
        .transpose()
    }

    async fn stages(
        &self,
        tx: &mut Self::Transaction,
        team: Uuid,
    ) -> Result<Option<SelectDefinitionSnapshot>, rootcause::Report> {
        Ok(self
            .properties
            .team_select_definition_in(tx, team, system_properties::CRM_TEAM_STAGE_DEFINITION_NAME)
            .await?)
    }

    async fn companies(
        &self,
        tx: &mut Self::Transaction,
        team: Uuid,
        after: Option<Uuid>,
        limit: i64,
        definitions: &[Uuid],
    ) -> Result<Vec<LegacySalesCompany>, rootcause::Report> {
        let ids = sqlx::query_scalar!(
            "SELECT id FROM crm_companies WHERE team_id = $1 AND NOT hidden AND ($2::uuid IS NULL OR id > $2) ORDER BY id LIMIT $3",
            team, after, limit,
        ).fetch_all(&mut **tx).await?;
        let strings = ids.iter().map(Uuid::to_string).collect::<Vec<_>>();
        let mut values: HashMap<String, HashMap<Uuid, _>> = HashMap::new();
        for (entity, definition, value) in self
            .properties
            .property_values_in(tx, EntityType::Company, &strings, definitions)
            .await?
        {
            values.entry(entity).or_default().insert(definition, value);
        }
        Ok(ids
            .into_iter()
            .map(|id| LegacySalesCompany {
                id,
                values: values.remove(&id.to_string()).unwrap_or_default(),
            })
            .collect())
    }

    async fn create(
        &self,
        tx: &mut Self::Transaction,
        blueprint: &PipelineBlueprint,
    ) -> Result<(), rootcause::Report> {
        Ok(self.pipelines.create_in(tx, blueprint).await?)
    }

    async fn rows(
        &self,
        tx: &mut Self::Transaction,
        blueprint: &PipelineBlueprint,
        rows: &[ProvisionedRow],
    ) -> Result<(), rootcause::Report> {
        Ok(self
            .pipelines
            .database
            .seed_managed_rows_in(tx, &blueprint.database, rows)
            .await?)
    }

    async fn checkpoint(
        &self,
        tx: &mut Self::Transaction,
        team: Uuid,
        pipeline: Uuid,
        count: i64,
    ) -> Result<(), rootcause::Report> {
        let inserted = sqlx::query!(
            "INSERT INTO crm_sales_backfills (team_id, pipeline_id, company_count) VALUES ($1, $2, $3) ON CONFLICT (team_id) DO NOTHING",
            team, pipeline, count,
        ).execute(&mut **tx).await?.rows_affected();
        if inserted != 1 {
            return Err(rootcause::report!(
                "team {team} was already migrated; rolling back duplicate import"
            ));
        }
        Ok(())
    }

    async fn finish(
        &self,
        mut tx: Self::Transaction,
        check: bool,
    ) -> Result<(), rootcause::Report> {
        // Check mode must also exercise deferred primary-reference constraints.
        sqlx::query!("SET CONSTRAINTS ALL IMMEDIATE")
            .execute(&mut *tx)
            .await?;
        if check {
            tx.rollback().await?;
        } else {
            tx.commit().await?;
        }
        Ok(())
    }
}
