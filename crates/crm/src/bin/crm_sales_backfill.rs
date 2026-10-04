//! Operator-only entrypoint for the additive Sales migration.

use clap::Parser;
use crm::{
    domain::sales_backfill::SalesBackfillService, outbound::sales_backfill::PgSalesBackfillStore,
};
use database_env_vars::DatabaseUrl;
use databases::outbound::pg_databases_repo::PgDatabasesRepo;
use macro_entrypoint::MacroEntrypoint;
use macro_env::Environment;
use properties::PropertiesPgRepo;
use rootcause::prelude::ResultExt as _;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(
    about = "Copy legacy CRM companies into team-shared Sales pipelines; pause legacy CRM writers during cutover"
)]
struct Args {
    /// Validate the complete import and roll back each team's transaction.
    #[arg(long)]
    check: bool,
    /// Restrict to one team; otherwise process CRM-enabled teams in pages.
    #[arg(long)]
    team: Option<Uuid>,
}

#[tokio::main]
async fn main() -> Result<(), rootcause::Report> {
    let args = Args::parse();
    let entrypoint = MacroEntrypoint::new(Environment::Local).init();
    let url = DatabaseUrl::new()?;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(url.as_ref())
        .await?;
    let service = SalesBackfillService(PgSalesBackfillStore::new(
        pool.clone(),
        PgDatabasesRepo::new(pool.clone(), PropertiesPgRepo::new(pool.clone())),
        PropertiesPgRepo::new(pool.clone()),
    ));
    if let Some(team) = args.team {
        println!(
            "{}",
            serde_json::to_string(
                &service
                    .migrate_team(team, args.check)
                    .await
                    .context(format!("Sales backfill failed for team {team}"))?
            )?
        );
    } else {
        let mut after = None;
        loop {
            let teams = service.teams(after).await?;
            if teams.is_empty() {
                break;
            }
            for team in teams {
                println!(
                    "{}",
                    serde_json::to_string(
                        &service
                            .migrate_team(team, args.check)
                            .await
                            .context(format!("Sales backfill failed for team {team}"))?
                    )?
                );
                after = Some(team);
            }
        }
    }
    pool.close().await;
    entrypoint.shutdown();
    Ok(())
}
