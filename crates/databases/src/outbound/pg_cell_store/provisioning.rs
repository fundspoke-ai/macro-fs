use super::*;
use crate::domain::models::ColumnId;
use crate::domain::provisioning::{DatabaseStorageProvisioner, StorageBlueprint};
use crate::domain::provisioning::{ProvisionedRow, TransactionalDatabaseStorage};
use crate::outbound::pg_databases_repo::insert_column;
use macro_user_id::user_id::MacroUserIdStr;
use models_databases::position::key_between;
use models_properties::EntityReference;
use properties::domain::database_cell_writer::DatabaseCellWriter;

impl<Properties> TransactionalDatabaseStorage for PgCellStore<Properties>
where
    Properties: PropertiesRepo<Err = anyhow::Error>
        + DatabaseDefinitionWriter<Transaction = Transaction<'static, Postgres>>
        + DatabaseCellWriter<Transaction = Transaction<'static, Postgres>>
        + DatabaseOptionWriter
        + Send
        + Sync
        + 'static,
{
    async fn seed_storage_rows_in(
        &self,
        transaction: &mut <Self as DatabaseStorageProvisioner>::Transaction,
        database: &StorageBlueprint,
        rows: &[ProvisionedRow],
        creator: &MacroUserIdStr<'_>,
    ) -> Result<(), <Self as crate::domain::ports::DatabaseStorage>::Error> {
        let columns = sqlx::query!(
            "SELECT id, property_definition_id FROM database_columns WHERE table_id = $1",
            database.table_id.into_uuid(),
        )
        .fetch_all(&mut **transaction)
        .await?
        .into_iter()
        .map(|column| {
            (
                ColumnId::from_uuid(column.id),
                column.property_definition_id,
            )
        })
        .collect::<HashMap<_, _>>();
        let ids = rows
            .iter()
            .map(|row| row.id.into_uuid())
            .collect::<Vec<_>>();
        let positions = rows
            .iter()
            .map(|row| row.position.as_str().to_owned())
            .collect::<Vec<_>>();
        sqlx::query!(
            "INSERT INTO database_rows (id, table_id, position, created_by) SELECT id, $1, position, $2 FROM UNNEST($3::uuid[], $4::text[]) AS input(id, position)",
            database.table_id.into_uuid(), creator.as_ref(), &ids, &positions,
        ).execute(&mut **transaction).await?;
        for row in rows {
            let entity = EntityReference {
                entity_type: models_properties::EntityType::DatabaseRow,
                entity_id: row.id.to_string(),
                specific_message_id: None,
            };
            for (column, value) in &row.cells {
                let definition = columns.get(column).ok_or(sqlx::Error::RowNotFound)?;
                self.properties
                    .upsert_entity_property_in(
                        transaction,
                        &entity,
                        *definition,
                        Some(value.clone()),
                    )
                    .await
                    .map_err(|error| PgCellStoreError::Cells(Box::new(error)))?;
            }
        }
        Ok(())
    }
}

impl<Properties> DatabaseStorageProvisioner for PgCellStore<Properties>
where
    Properties: PropertiesRepo<Err = anyhow::Error>
        + DatabaseDefinitionWriter<Transaction = Transaction<'static, Postgres>>
        + DatabaseCellWriter<Transaction = Transaction<'static, Postgres>>
        + DatabaseOptionWriter
        + Send
        + Sync
        + 'static,
{
    type Transaction = Transaction<'static, Postgres>;

    #[tracing::instrument(skip_all, err)]
    async fn create_storage_in(
        &self,
        transaction: &mut Self::Transaction,
        database: &StorageBlueprint,
    ) -> Result<(), Self::Error> {
        sqlx::query!(
            "INSERT INTO databases (id) VALUES ($1)",
            database.id.into_uuid()
        )
        .execute(&mut **transaction)
        .await?;
        let position = key_between(None, None).map_err(PgDatabasesRepoError::from)?;
        sqlx::query!(
            "INSERT INTO database_tables (id, database_id, name, position) VALUES ($1, $2, 'Records', $3)",
            database.table_id.into_uuid(), database.id.into_uuid(), position.as_str(),
        ).execute(&mut **transaction).await?;
        let positions =
            models_databases::position::keys_between(None, None, database.columns.len())
                .map_err(PgDatabasesRepoError::from)?;
        for (column, position) in database.columns.iter().zip(positions) {
            let definition = self
                .properties
                .create_database_definition_in(
                    transaction,
                    NewDatabaseDefinition {
                        id: macro_uuid::generate_uuid_v7(),
                        database_id: database.id.into_uuid(),
                        name: &column.name,
                        data_type: column.data_type,
                        is_multi_select: false,
                        specific_entity_type: column.entity_type,
                        options: &column.options,
                    },
                )
                .await
                .map_err(|error| PgCellStoreError::Cells(Box::new(error)))?;
            insert_column(
                transaction,
                column.id,
                database.table_id,
                definition.definition.id,
                &position,
                false,
            )
            .await?;
        }
        Ok(())
    }
}
