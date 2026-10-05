//! Atomic database provisioning for domains that own a database's lifecycle.

use macro_user_id::user_id::MacroUserIdStr;
use models_databases::{ColumnId, DatabaseId, RowId, TableId, position::Position};
use models_properties::service::property_option::PropertyOptionValue;
use models_properties::service::property_value::PropertyValue;
use models_properties::{DataType, EntityType};
use uuid::Uuid;

/// A column to provision in core storage.
pub struct ProvisionedColumn {
    /// Stable placement identity.
    pub id: ColumnId,
    /// Display name.
    pub name: String,
    /// Cell type.
    pub data_type: DataType,
    /// Referenced entity type, for entity columns.
    pub entity_type: Option<EntityType>,
    /// Initial select options, copied into this database.
    pub options: Vec<(Uuid, PropertyOptionValue)>,
}

/// A complete initial schema; the owning domain supplies the policy.
pub struct StorageBlueprint {
    /// Database identity.
    pub id: DatabaseId,
    /// Initial table identity.
    pub table_id: TableId,
    /// Initial column definitions in display order.
    pub columns: Vec<ProvisionedColumn>,
}

/// An initial row copied into a newly provisioned database before it is visible.
pub struct ProvisionedRow {
    /// New row identity.
    pub id: RowId,
    /// Display order across all batches.
    pub position: Position,
    /// Values keyed by column placement.
    pub cells: Vec<(ColumnId, PropertyValue)>,
}

/// Transactional initial population. Call only in the transaction that creates
/// the database, before readers exist; subsequent edits use the journaled API.
pub trait TransactionalDatabaseStorage: DatabaseStorageProvisioner {
    /// Populate a bounded batch without exposing a partially imported database.
    fn seed_storage_rows_in(
        &self,
        transaction: &mut Self::Transaction,
        database: &StorageBlueprint,
        rows: &[ProvisionedRow],
        creator: &MacroUserIdStr<'_>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Transactional port used by an owning domain's repository.
pub trait DatabaseStorageProvisioner: super::ports::DatabaseStorage {
    /// Opaque transaction supplied by the adapter.
    type Transaction: Send;
    /// Provision all database-owned storage in the caller's transaction.
    fn create_storage_in(
        &self,
        transaction: &mut Self::Transaction,
        database: &StorageBlueprint,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
