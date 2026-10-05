-- Refuse rollback if repeated references exist; never discard pipeline entries.
ALTER TABLE crm_pipeline_primary_values
    ADD CONSTRAINT crm_pipeline_primary_values_table_id_entity_type_entity_id_key
    UNIQUE (table_id, entity_type, entity_id);

DROP INDEX crm_pipeline_primary_values_record;
