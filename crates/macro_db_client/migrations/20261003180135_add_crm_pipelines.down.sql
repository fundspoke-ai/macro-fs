-- Preserve pipeline data as ordinary databases if this feature is rolled back.
INSERT INTO entity_access (entity_id, entity_type, source_id, source_type, access_level)
SELECT p.database_id, 'database', ea.source_id, ea.source_type, ea.access_level
FROM crm_pipelines p JOIN entity_access ea ON ea.entity_id = p.id AND ea.entity_type = 'crm_pipeline'
ON CONFLICT DO NOTHING;
DELETE FROM entity_access WHERE entity_type = 'crm_pipeline' AND entity_id IN (SELECT id FROM crm_pipelines);
DROP TRIGGER crm_pipeline_cleanup ON crm_pipelines;
DROP TRIGGER crm_pipeline_record ON database_primary_values;
DROP FUNCTION check_crm_pipeline_record();
DROP TABLE crm_pipelines;
DROP FUNCTION clean_up_crm_pipeline();
DROP TRIGGER database_primary_definition_type ON property_definitions;
DROP TRIGGER database_primary_column_type ON database_columns;
DROP FUNCTION protect_database_primary_column();
DROP TRIGGER database_row_requires_primary ON database_rows;
DROP TRIGGER database_primary_value_required ON database_primary_values;
DROP FUNCTION require_database_primary_value();
DROP TRIGGER database_primary_value_insert ON entity_properties;
DROP TRIGGER database_primary_value_write ON entity_properties;
DROP TRIGGER database_primary_value_delete ON entity_properties;
DROP FUNCTION maintain_database_primary_value();
DROP TABLE database_primary_values;
DROP TABLE database_primary_columns;
DROP INDEX databases_access_entity;
ALTER TABLE databases DROP CONSTRAINT database_access_owner;
ALTER TABLE databases DROP COLUMN access_entity_id;
ALTER TABLE databases DROP COLUMN access_entity_type;
