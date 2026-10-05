-- Preserve pipeline content as ordinary database app entities on rollback.
INSERT INTO database_entities (database_id, name, user_id, created_at, updated_at, trashed_at)
SELECT database_id, name, user_id, created_at, updated_at, trashed_at FROM crm_pipeline_entities
ON CONFLICT DO NOTHING;
DROP TABLE crm_sales_backfills;
INSERT INTO entity_access (entity_id, entity_type, source_id, source_type, access_level)
SELECT p.database_id, 'database', ea.source_id, ea.source_type, ea.access_level
FROM crm_pipeline_entities p JOIN entity_access ea ON ea.entity_id = p.id AND ea.entity_type = 'crm_pipeline'
ON CONFLICT DO NOTHING;
DELETE FROM entity_access WHERE entity_type = 'crm_pipeline' AND entity_id IN (SELECT id FROM crm_pipeline_entities);
DROP TRIGGER crm_pipeline_cleanup ON crm_pipeline_entities;
DROP TRIGGER crm_pipeline_record ON crm_pipeline_primary_values;
DROP FUNCTION check_crm_pipeline_record();
DROP TABLE crm_pipeline_entities;
DROP FUNCTION clean_up_crm_pipeline();
DROP TRIGGER crm_pipeline_primary_definition_type ON property_definitions;
DROP TRIGGER crm_pipeline_primary_column_type ON database_columns;
DROP FUNCTION protect_crm_pipeline_primary_column();
DROP TRIGGER crm_pipeline_row_requires_primary ON database_rows;
DROP TRIGGER crm_pipeline_primary_value_required ON crm_pipeline_primary_values;
DROP FUNCTION require_crm_pipeline_primary_value();
DROP TRIGGER crm_pipeline_primary_value_insert ON entity_properties;
DROP TRIGGER crm_pipeline_primary_value_write ON entity_properties;
DROP TRIGGER crm_pipeline_primary_value_delete ON entity_properties;
DROP FUNCTION maintain_crm_pipeline_primary_value();
DROP TABLE crm_pipeline_primary_values;
DROP TABLE crm_pipeline_primary_columns;
