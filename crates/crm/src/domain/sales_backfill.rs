//! One-time, operator-authorized copy of legacy CRM data. No end-user route
//! exposes this capability. Each team commits its pipeline and checkpoint
//! together; original records and properties remain intact for rollback.

use super::pipelines::{
    CreatePipeline, PipelineBlueprint, PipelineRecordType, PipelineSharing, pipeline_blueprint,
};
use databases::domain::provisioning::ProvisionedRow;
use macro_user_id::user_id::MacroUserIdStr;
use models_databases::{RowId, position::keys_between};
use models_properties::{EntityReference, EntityType, service::property_value::PropertyValue};
use properties::domain::property_snapshot_reader::SelectDefinitionSnapshot;
use rootcause::prelude::ResultExt as _;
use std::collections::{BTreeMap, HashMap};
use system_properties::{StageOption, SystemPropertyKey};
use uuid::Uuid;

const BATCH_SIZE: i64 = 250;

/// Team facts read under the backfill's transaction lock.
pub struct LegacySalesTeam {
    /// Owning CRM team.
    pub id: Uuid,
    /// Existing team owner; owns the imported pipeline.
    pub owner: MacroUserIdStr<'static>,
    /// CRM enabled state, rechecked after locking.
    pub enabled: bool,
    /// A completed backfill, including a subsequently deleted pipeline.
    pub completed: bool,
    /// Mapping retained when teams customized or renamed system stages.
    pub legacy_stage_ids: BTreeMap<Uuid, Uuid>,
}

/// One visible company and only its shared CRM deal properties.
pub struct LegacySalesCompany {
    /// Primary company reference.
    pub id: Uuid,
    /// Explicitly allowlisted source values; private custom properties are excluded.
    pub values: HashMap<Uuid, PropertyValue>,
}

/// Storage capabilities for an atomic, bounded-memory import per team.
pub trait SalesBackfillStore: Send + Sync {
    /// Adapter-owned transaction, rolled back on drop.
    type Transaction: Send;
    /// List CRM-enabled teams, in identity order, whether completed or pending.
    fn teams(
        &self,
        after: Option<Uuid>,
        limit: i64,
    ) -> impl Future<Output = Result<Vec<Uuid>, rootcause::Report>> + Send;
    /// Serialize backfills and team ownership/sharing changes before reading facts.
    fn begin(&self) -> impl Future<Output = Result<Self::Transaction, rootcause::Report>> + Send;
    /// Read the team, current settings and completion marker.
    fn team(
        &self,
        tx: &mut Self::Transaction,
        team: Uuid,
    ) -> impl Future<Output = Result<Option<LegacySalesTeam>, rootcause::Report>> + Send;
    /// Snapshot the team's optional custom stage definition.
    fn stages(
        &self,
        tx: &mut Self::Transaction,
        team: Uuid,
    ) -> impl Future<Output = Result<Option<SelectDefinitionSnapshot>, rootcause::Report>> + Send;
    /// Read one page of visible companies and explicitly selected properties.
    fn companies(
        &self,
        tx: &mut Self::Transaction,
        team: Uuid,
        after: Option<Uuid>,
        limit: i64,
        definitions: &[Uuid],
    ) -> impl Future<Output = Result<Vec<LegacySalesCompany>, rootcause::Report>> + Send;
    /// Provision the pipeline and its access grants in this transaction.
    fn create(
        &self,
        tx: &mut Self::Transaction,
        blueprint: &PipelineBlueprint,
    ) -> impl Future<Output = Result<(), rootcause::Report>> + Send;
    /// Populate initial database rows through the database-owning port.
    fn rows(
        &self,
        tx: &mut Self::Transaction,
        blueprint: &PipelineBlueprint,
        rows: &[ProvisionedRow],
    ) -> impl Future<Output = Result<(), rootcause::Report>> + Send;
    /// Write a completion marker, committed atomically with the import.
    fn checkpoint(
        &self,
        tx: &mut Self::Transaction,
        team: Uuid,
        pipeline: Uuid,
        count: i64,
    ) -> impl Future<Output = Result<(), rootcause::Report>> + Send;
    /// Commit a run, or roll back all writes for a validation run.
    fn finish(
        &self,
        tx: Self::Transaction,
        check: bool,
    ) -> impl Future<Output = Result<(), rootcause::Report>> + Send;
}

/// Outcome for a team, with no company data in operator logs.
#[derive(Debug, serde::Serialize)]
pub struct SalesBackfillReport {
    /// Team being processed.
    pub team_id: Uuid,
    /// Number of companies copied, or zero for a skipped team.
    pub companies: i64,
    /// Why nothing was copied, when applicable.
    pub skipped: Option<&'static str>,
    /// Whether the transaction was rolled back after validation.
    pub check: bool,
}

/// Domain orchestration for the operator CLI; not wired into HTTP or AI tools.
pub struct SalesBackfillService<S>(pub S);

impl<S: SalesBackfillStore> SalesBackfillService<S> {
    /// Next bounded page of teams.
    pub async fn teams(&self, after: Option<Uuid>) -> Result<Vec<Uuid>, rootcause::Report> {
        self.0.teams(after, BATCH_SIZE).await
    }

    /// Copy each visible company, including closed and unstaged records. A
    /// completed team is never modified again, even after rename/trash/deletion.
    #[tracing::instrument(skip(self), err)]
    pub async fn migrate_team(
        &self,
        team_id: Uuid,
        check: bool,
    ) -> Result<SalesBackfillReport, rootcause::Report> {
        let mut tx = self.0.begin().await?;
        let team = self.0.team(&mut tx, team_id).await?;
        let skipped = match &team {
            None => Some("team not found"),
            Some(team) if !team.enabled => Some("CRM disabled"),
            Some(team) if team.completed => Some("already migrated"),
            _ => None,
        };
        let mut report = SalesBackfillReport {
            team_id,
            companies: 0,
            skipped,
            check,
        };
        if report.skipped.is_some() {
            self.0.finish(tx, true).await?;
            return Ok(report);
        }
        let team = team.expect("missing teams return above");
        let stages = self.0.stages(&mut tx, team_id).await?;
        let source = stages
            .as_ref()
            .map(|set| set.options.clone())
            .unwrap_or_else(|| {
                use StageOption::*;
                [Lead, Qualified, Demo, Trial, Negotiation, Customer, Churned]
                    .into_iter()
                    .map(|stage| (stage.uuid(), stage.display_value().to_owned()))
                    .collect()
            });
        let blueprint = pipeline_blueprint(
            team_id,
            team.owner.clone(),
            CreatePipeline {
                name: "Sales".into(),
                record_type: PipelineRecordType::Company,
                sharing: PipelineSharing::Team,
            },
            source.iter().map(|(_, label)| label.clone()).collect(),
        );
        let stage_options = source
            .iter()
            .zip(&blueprint.database.columns[1].options)
            .map(|((source, _), (target, _))| (*source, *target))
            .collect::<HashMap<_, _>>();
        let mut definitions = vec![
            SystemPropertyKey::Stage.uuid(),
            SystemPropertyKey::CompanyOwner.uuid(),
            SystemPropertyKey::Revenue.uuid(),
        ];
        if let Some(stages) = &stages {
            definitions.push(stages.id);
        }
        let mut after = None;
        let mut last_position = None;
        // Create only once there is data; empty CRMs may receive data before a rerun.
        loop {
            let companies = self
                .0
                .companies(&mut tx, team_id, after, BATCH_SIZE, &definitions)
                .await?;
            if companies.is_empty() {
                break;
            }
            if report.companies == 0 {
                self.0.create(&mut tx, &blueprint).await?;
            }
            let positions = keys_between(last_position.as_ref(), None, companies.len())?;
            last_position = positions.last().cloned();
            after = companies.last().map(|company| company.id);
            let rows = companies.into_iter().zip(positions).map(|(company, position)| {
                let stage = resolve_stage(&company.values, stages.as_ref(), &source, &team.legacy_stage_ids)
                    .context(format!("company {} in team {team_id}", company.id))?;
                let columns = &blueprint.database.columns;
                let mut cells = vec![(columns[0].id, PropertyValue::EntityRef(vec![EntityReference {
                    entity_type: EntityType::Company, entity_id: company.id.to_string(), specific_message_id: None,
                }]))];
                if let Some(stage) = stage {
                    let option = stage_options.get(&stage).ok_or_else(|| rootcause::report!("company {} has an unknown stage", company.id))?;
                    cells.push((columns[1].id, PropertyValue::SelectOption(vec![*option])));
                }
                if let Some(value) = company.values.get(&SystemPropertyKey::CompanyOwner.uuid()) {
                    if !matches!(value, PropertyValue::EntityRef(refs) if refs.len() <= 1 && refs.iter().all(|r| r.entity_type == EntityType::User)) {
                        return Err(rootcause::report!("company {} has an invalid owner", company.id));
                    }
                    cells.push((columns[2].id, value.clone()));
                }
                if let Some(value) = company.values.get(&SystemPropertyKey::Revenue.uuid()) {
                    if !matches!(value, PropertyValue::Num(_)) {
                        return Err(rootcause::report!("company {} has invalid revenue", company.id));
                    }
                    cells.push((columns[3].id, value.clone()));
                }
                Ok(ProvisionedRow { id: RowId::new(), position, cells })
            }).collect::<Result<Vec<_>, rootcause::Report>>()?;
            self.0.rows(&mut tx, &blueprint, &rows).await?;
            report.companies += rows.len() as i64;
        }
        if report.companies == 0 {
            report.skipped = Some("no visible companies");
        } else {
            self.0
                .checkpoint(&mut tx, team_id, blueprint.id, report.companies)
                .await?;
        }
        self.0.finish(tx, check).await?;
        Ok(report)
    }
}

fn selected(
    values: &HashMap<Uuid, PropertyValue>,
    definition: Uuid,
) -> Result<Option<Uuid>, rootcause::Report> {
    match values.get(&definition) {
        None => Ok(None),
        Some(PropertyValue::SelectOption(options)) if options.len() <= 1 => {
            Ok(options.first().copied())
        }
        _ => Err(rootcause::report!(
            "invalid single-select stage property {definition}"
        )),
    }
}

/// Match the CRM's direct stage, explicit legacy mapping, then label fallback.
/// An unmappable populated legacy stage blocks the import rather than losing data.
fn resolve_stage(
    values: &HashMap<Uuid, PropertyValue>,
    custom: Option<&SelectDefinitionSnapshot>,
    options: &[(Uuid, String)],
    mapping: &BTreeMap<Uuid, Uuid>,
) -> Result<Option<Uuid>, rootcause::Report> {
    let definition = custom.map_or(SystemPropertyKey::Stage.uuid(), |set| set.id);
    let direct = selected(values, definition)?;
    if direct.is_some_and(|id| options.iter().any(|(option, _)| *option == id)) {
        return Ok(direct);
    }
    let legacy = selected(values, SystemPropertyKey::Stage.uuid())?;
    if custom.is_some()
        && let Some(legacy) = legacy
    {
        if let Some(mapped) = mapping
            .get(&legacy)
            .filter(|mapped| options.iter().any(|(option, _)| option == *mapped))
        {
            return Ok(Some(*mapped));
        }
        if let Some(label) = StageOption::from_uuid(legacy).map(|stage| stage.display_value())
            && let Some((id, _)) = options
                .iter()
                .find(|(_, current)| current.eq_ignore_ascii_case(label))
        {
            return Ok(Some(*id));
        }
    }
    if direct.is_some() || legacy.is_some() {
        return Err(rootcause::report!(
            "unmapped company stage; repair the team's stage mapping before retrying"
        ));
    }
    Ok(None)
}
