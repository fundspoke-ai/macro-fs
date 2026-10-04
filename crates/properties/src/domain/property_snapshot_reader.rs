//! Strict property snapshots for operator migrations sharing a transaction.

use super::database_cell_writer::DatabaseWriteTransaction;
use models_properties::{EntityType, service::property_value::PropertyValue};
use uuid::Uuid;

/// A single-select string definition, with options in display order.
pub struct SelectDefinitionSnapshot {
    /// Definition identity.
    pub id: Uuid,
    /// Option identities and labels.
    pub options: Vec<(Uuid, String)>,
}

/// Reads only explicitly selected team definitions and entity properties.
/// Unlike best-effort display reads, corrupt values fail the transaction.
/// This is an operator capability, not an end-user authorization boundary.
pub trait PropertySnapshotReader: DatabaseWriteTransaction {
    /// Find a team's named single-select string definition. Ambiguity is an error.
    fn team_select_definition_in(
        &self,
        transaction: &mut Self::Transaction,
        team: Uuid,
        name: &str,
    ) -> impl Future<Output = Result<Option<SelectDefinitionSnapshot>, Self::Err>> + Send;

    /// Read a bounded set of entities and definitions. Null values are omitted.
    fn property_values_in(
        &self,
        transaction: &mut Self::Transaction,
        entity_type: EntityType,
        entities: &[String],
        definitions: &[Uuid],
    ) -> impl Future<Output = Result<Vec<(String, Uuid, PropertyValue)>, Self::Err>> + Send;
}
