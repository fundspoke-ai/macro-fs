use super::{properties_pg_repo::PropertiesPgRepo, query_error::PropertyQueryError};
use crate::domain::property_snapshot_reader::{PropertySnapshotReader, SelectDefinitionSnapshot};
use models_properties::{EntityType, service::property_value::PropertyValue};
use uuid::Uuid;

impl PropertySnapshotReader for PropertiesPgRepo {
    async fn team_select_definition_in(
        &self,
        transaction: &mut Self::Transaction,
        team: Uuid,
        name: &str,
    ) -> Result<Option<SelectDefinitionSnapshot>, Self::Err> {
        let definitions = sqlx::query!(
            "SELECT id, data_type::text AS data_type, is_multi_select FROM property_definitions WHERE team_id = $1 AND display_name = $2 ORDER BY id LIMIT 2",
            team, name,
        ).fetch_all(&mut **transaction).await?;
        let Some(definition) = definitions.first() else {
            return Ok(None);
        };
        if definitions.len() != 1
            || definition.data_type.as_deref() != Some("SELECT_STRING")
            || definition.is_multi_select
        {
            return Err(PropertyQueryError::Sqlx(sqlx::Error::Decode(
                "ambiguous or invalid team select definition".into(),
            )));
        }
        let options = sqlx::query!(
            "SELECT id, string_value FROM property_options WHERE property_definition_id = $1 ORDER BY display_order, id",
            definition.id,
        ).fetch_all(&mut **transaction).await?;
        let options = options
            .into_iter()
            .map(|option| {
                option
                    .string_value
                    .map(|label| (option.id, label))
                    .ok_or_else(|| {
                        PropertyQueryError::Sqlx(sqlx::Error::Decode(
                            "select option has no string value".into(),
                        ))
                    })
            })
            .collect::<Result<_, _>>()?;
        Ok(Some(SelectDefinitionSnapshot {
            id: definition.id,
            options,
        }))
    }

    async fn property_values_in(
        &self,
        transaction: &mut Self::Transaction,
        entity_type: EntityType,
        entities: &[String],
        definitions: &[Uuid],
    ) -> Result<Vec<(String, Uuid, PropertyValue)>, Self::Err> {
        let rows = sqlx::query!(
            r#"SELECT entity_id, property_definition_id, values AS "value!: sqlx::types::Json<PropertyValue>"
               FROM entity_properties WHERE entity_type = $1 AND entity_id = ANY($2)
               AND property_definition_id = ANY($3) AND values IS NOT NULL AND values <> 'null'::jsonb"#,
            entity_type as EntityType, entities, definitions,
        ).fetch_all(&mut **transaction).await?;
        Ok(rows
            .into_iter()
            .map(|row| (row.entity_id, row.property_definition_id, row.value.0))
            .collect())
    }
}
