//! Atomic database provisioning for domains that own a database's lifecycle.

use chrono::{DateTime, Utc};
use entity_access::domain::models::Entity;
use macro_user_id::user_id::MacroUserIdStr;
use models_databases::{ColumnId, DatabaseId, RowId, TableId, position::Position};
use models_properties::service::property_option::PropertyOptionValue;
use models_properties::service::property_value::PropertyValue;
use models_properties::{DataType, EntityType};
use uuid::Uuid;

/// Database-owned metadata exposed to the domain that manages its lifecycle.
pub struct ManagedDatabaseInfo {
    /// Database identity.
    pub id: DatabaseId,
    /// Current display name.
    pub name: String,
    /// Primary reference placement.
    pub primary_column_id: ColumnId,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Trash state.
    pub trashed_at: Option<DateTime<Utc>>,
}

/// A column to provision in a managed database.
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
pub struct ManagedDatabase {
    /// Database identity.
    pub id: DatabaseId,
    /// Display name.
    pub name: String,
    /// Creator, retained for provenance and existing database metadata.
    pub creator: MacroUserIdStr<'static>,
    /// Entity whose grants govern the database. No independent grants are created.
    pub access_entity: Entity,
    /// Initial table identity.
    pub table_id: TableId,
    /// Initial column definitions in display order.
    pub columns: Vec<ProvisionedColumn>,
    /// Required, unique, single entity-reference column.
    pub primary_column_id: ColumnId,
}

/// An initial row copied into a newly provisioned database before it is visible.
pub struct ProvisionedRow {
    /// New row identity; distinct from its primary CRM reference.
    pub id: RowId,
    /// Display order across all batches.
    pub position: Position,
    /// Values keyed by column placement, including the required primary reference.
    pub cells: Vec<(ColumnId, PropertyValue)>,
}

/// Transactional initial population. Call only in the transaction that creates
/// the database, before readers exist; subsequent edits use the journaled API.
pub trait ManagedDatabaseSeeder: ManagedDatabaseWriter {
    /// Populate a bounded batch without exposing a partially imported database.
    fn seed_managed_rows_in(
        &self,
        transaction: &mut Self::Transaction,
        database: &ManagedDatabase,
        rows: &[ProvisionedRow],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Transactional port used by an owning domain's repository.
pub trait ManagedDatabaseWriter: Send + Sync + 'static {
    /// Opaque transaction supplied by the adapter.
    type Transaction: Send;
    /// Persistence error.
    type Error: std::error::Error + Send + Sync + 'static;
    /// Resolve database metadata for a managing domain's known identities.
    fn managed_databases(
        &self,
        ids: &[DatabaseId],
    ) -> impl Future<Output = Result<Vec<ManagedDatabaseInfo>, Self::Error>> + Send;
    /// Provision all database-owned storage in the caller's transaction.
    fn create_managed_database_in(
        &self,
        transaction: &mut Self::Transaction,
        database: &ManagedDatabase,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
