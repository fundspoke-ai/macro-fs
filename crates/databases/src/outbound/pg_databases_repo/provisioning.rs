use super::*;
use crate::domain::provisioning::{ManagedDatabase, ManagedDatabaseInfo, ManagedDatabaseWriter};
use crate::domain::provisioning::{ManagedDatabaseSeeder, ProvisionedRow};
use models_properties::EntityReference;
use properties::domain::database_cell_writer::DatabaseCellWriter;

impl<Properties> ManagedDatabaseSeeder for PgDatabasesRepo<Properties>
where
    Properties: DatabaseDefinitionWriter<Transaction = Transaction<'static, Postgres>>
        + DatabaseCellWriter<Transaction = Transaction<'static, Postgres>>,
{
    async fn seed_managed_rows_in(
        &self,
        transaction: &mut <Self as ManagedDatabaseWriter>::Transaction,
        database: &ManagedDatabase,
        rows: &[ProvisionedRow],
    ) -> Result<(), <Self as ManagedDatabaseWriter>::Error> {
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
            database.table_id.into_uuid(), database.creator.as_ref(), &ids, &positions,
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
                    .map_err(|error| PgDatabasesRepoError::Properties(Box::new(error)))?;
            }
        }
        Ok(())
    }
}

impl<Properties> ManagedDatabaseWriter for PgDatabasesRepo<Properties>
where
    Properties: DatabaseDefinitionWriter<Transaction = Transaction<'static, Postgres>>,
{
    type Transaction = Transaction<'static, Postgres>;
    type Error = PgDatabasesRepoError;

    async fn managed_databases(
        &self,
        ids: &[DatabaseId],
    ) -> Result<Vec<ManagedDatabaseInfo>, Self::Error> {
        let ids = ids.iter().map(|id| id.into_uuid()).collect::<Vec<_>>();
        Ok(sqlx::query!(
            "SELECT d.id, d.name, d.created_at, d.trashed_at, pc.column_id FROM databases d JOIN database_tables t ON t.database_id = d.id JOIN database_primary_columns pc ON pc.table_id = t.id WHERE d.id = ANY($1)",
            &ids,
        ).fetch_all(&self.pool).await?.into_iter().map(|row| ManagedDatabaseInfo {
            id: DatabaseId::from_uuid(row.id), name: row.name, created_at: row.created_at,
            trashed_at: row.trashed_at, primary_column_id: ColumnId::from_uuid(row.column_id),
        }).collect())
    }

    #[tracing::instrument(skip_all, err)]
    async fn create_managed_database_in(
        &self,
        transaction: &mut Self::Transaction,
        database: &ManagedDatabase,
    ) -> Result<(), Self::Error> {
        let access_id = database
            .access_entity
            .entity_id
            .parse::<uuid::Uuid>()
            .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        sqlx::query!(
            "INSERT INTO databases (id, name, owner_id, access_entity_id, access_entity_type) VALUES ($1, $2, $3, $4, $5)",
            database.id.into_uuid(), database.name, database.creator.as_ref(),
            access_id, database.access_entity.entity_type.as_ref(),
        ).execute(&mut **transaction).await?;
        let position = position_after(None)?;
        sqlx::query!(
            "INSERT INTO database_tables (id, database_id, name, position) VALUES ($1, $2, 'Records', $3)",
            database.table_id.into_uuid(), database.id.into_uuid(), position.as_str(),
        ).execute(&mut **transaction).await?;
        let positions =
            models_databases::position::keys_between(None, None, database.columns.len())?;
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
                .map_err(|error| PgDatabasesRepoError::Properties(Box::new(error)))?;
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
        sqlx::query!(
            "INSERT INTO database_primary_columns (table_id, column_id) VALUES ($1, $2)",
            database.table_id.into_uuid(),
            database.primary_column_id.into_uuid(),
        )
        .execute(&mut **transaction)
        .await?;
        Ok(())
    }
}
