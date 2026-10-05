//! Core storage uses the same planner, definitions, cells and journal as the app.

use super::*;
use crate::domain::catalog::{ColumnEntry, StorageTable};
use crate::domain::journal::Restoration;
use crate::domain::models::{AppliedOps, WritesOutcome};
use crate::domain::ports::DatabaseStorage;
use crate::domain::storage::{DatabaseStorageService, StorageRow, StorageRows};
use models_databases::{ColumnChange, ColumnKind, DatabaseOp, NewColumn};

impl<R, D, C, E, A, B> DatabasesServiceImpl<R, D, C, E, A, B>
where
    R: DatabasesRepo,
    D: ColumnDefinitionStore,
    C: CellStore,
    E: TableEventPublisher,
    A: AccessDirectory,
    B: MacroEventBroker,
{
    async fn storage_entries(&self, id: DatabaseId) -> Result<Vec<StorageTable>, DatabaseError> {
        let tables = self
            .repository
            .tables_for_databases(&[id])
            .await
            .map_err(repository_error)?;
        let ids: Vec<_> = tables.iter().map(|table| table.id).collect();
        let columns = self
            .repository
            .columns_for_tables(&ids)
            .await
            .map_err(repository_error)?;
        let definition_ids: Vec<_> = columns
            .iter()
            .map(|column| column.property_definition_id)
            .collect();
        let definitions: HashMap<_, _> = self
            .definitions
            .definitions(&definition_ids)
            .await
            .map_err(repository_error)?
            .into_iter()
            .map(|definition| (definition.definition.id, definition))
            .collect();
        let views = self
            .repository
            .views_for_tables(&ids)
            .await
            .map_err(repository_error)?;
        tables
            .into_iter()
            .map(|table| {
                let columns = columns
                    .iter()
                    .filter(|column| column.table_id == table.id)
                    .map(|column| {
                        Ok(ColumnEntry {
                            column: column.clone(),
                            definition: definitions
                                .get(&column.property_definition_id)
                                .cloned()
                                .ok_or(DatabaseError::NotFound)?,
                            writable: true,
                        })
                    })
                    .collect::<Result<_, DatabaseError>>()?;
                let views = views
                    .iter()
                    .filter(|view| view.table_id == table.id)
                    .cloned()
                    .collect();
                Ok(StorageTable {
                    table,
                    columns,
                    views,
                })
            })
            .collect()
    }
}

impl<R, D, C, E, A, B> DatabaseStorageService for DatabasesServiceImpl<R, D, C, E, A, B>
where
    R: DatabasesRepo,
    D: ColumnDefinitionStore,
    C: CellStore + DatabaseStorage,
    E: TableEventPublisher,
    A: AccessDirectory,
    B: MacroEventBroker,
{
    async fn storage_tables(&self, id: DatabaseId) -> Result<Vec<TableDetail>, DatabaseError> {
        Ok(self
            .storage_entries(id)
            .await?
            .into_iter()
            .map(|entry| TableDetail {
                sql_name: catalog::sql_identifier(&entry.table.name),
                table: entry.table,
                columns: entry
                    .columns
                    .into_iter()
                    .map(|column| ColumnDetail {
                        sql_name: catalog::sql_identifier(column.name()),
                        shared_outside_database: column.shared_outside(id),
                        column: column.column,
                        definition: column.definition,
                        writable: true,
                    })
                    .collect(),
                views: entry.views,
            })
            .collect())
    }

    async fn storage_rows(
        &self,
        id: DatabaseId,
        table: TableId,
        after: Option<RowId>,
    ) -> Result<StorageRows, DatabaseError> {
        const PAGE_SIZE: i64 = 500;
        let entry = self
            .storage_entries(id)
            .await?
            .into_iter()
            .find(|entry| entry.table.id == table)
            .ok_or(DatabaseError::NotFound)?;
        let mut refs = self
            .cells
            .storage_row_page(table, after, PAGE_SIZE + 1)
            .await
            .map_err(repository_error)?;
        let more = refs.len() > PAGE_SIZE as usize;
        refs.truncate(PAGE_SIZE as usize);
        let next = if more {
            refs.last().map(|row| row.id)
        } else {
            None
        };
        let ids: Vec<_> = refs.iter().map(|row| row.id).collect();
        let mut cells = self.cells.cells(&ids).await.map_err(repository_error)?;
        let rows = refs
            .into_iter()
            .map(|row| {
                let values = cells.remove(&row.id).unwrap_or_default();
                StorageRow {
                    row_id: row.id,
                    cells: entry
                        .columns
                        .iter()
                        .filter_map(|column| {
                            values
                                .get(&column.column.property_definition_id)
                                .cloned()
                                .map(|value| (column.column.id, value))
                        })
                        .collect(),
                }
            })
            .collect();
        let version = self
            .repository
            .table_versions(&[table])
            .await
            .map_err(repository_error)?
            .get(&table)
            .copied()
            .ok_or(DatabaseError::NotFound)?;
        if version != entry.table.version {
            return Err(DatabaseError::VersionConflict);
        }
        Ok(StorageRows {
            rows,
            next,
            version,
        })
    }

    async fn apply_storage_ops(
        &self,
        id: DatabaseId,
        viewer: Viewer,
        batch: OpBatch,
    ) -> Result<AppliedOps, DatabaseError> {
        for (index, op) in batch.ops.iter().enumerate() {
            if matches!(
                op,
                DatabaseOp::Column {
                    change: ColumnChange::Create {
                        definition: NewColumn::Existing { .. },
                        ..
                    },
                    ..
                } | DatabaseOp::Column {
                    change: ColumnChange::Create {
                        definition: NewColumn::New {
                            kind: ColumnKind::Relation { .. },
                            ..
                        },
                        ..
                    },
                    ..
                } | DatabaseOp::Column {
                    change: ColumnChange::ChangeType {
                        to: ColumnKind::Relation { .. }
                    },
                    ..
                }
            ) {
                return Err(DatabaseError::InvalidOp(crate::domain::models::OpRefusal {
                    op: index,
                    row: None,
                    column: None,
                    taken: None,
                    reason: "Storage consumers must authorize external bindings and relations"
                        .into(),
                }));
            }
        }
        for _ in 0..ops::MAX_PLANNING_ATTEMPTS {
            let entries = self.storage_entries(id).await?;
            let (writes, planner) = self
                .prepare_storage_batch(id, &entries, &viewer, &batch, &Restoration::default())
                .await?;
            let outcome = self
                .cells
                .apply_storage_writes(&writes)
                .await
                .map_err(repository_error)?;
            if matches!(outcome, WritesOutcome::SchemaMoved(_)) {
                continue;
            }
            return planner.finish(&entries, &batch, &writes, outcome);
        }
        Err(DatabaseError::VersionConflict)
    }
}
