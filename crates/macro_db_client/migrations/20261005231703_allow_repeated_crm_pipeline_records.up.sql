-- A company or contact may have multiple independent entries in one pipeline.
-- Keep reference lookups and cascade cleanup indexed without enforcing uniqueness.
CREATE INDEX crm_pipeline_primary_values_record
    ON crm_pipeline_primary_values (table_id, entity_type, entity_id);

ALTER TABLE crm_pipeline_primary_values
    DROP CONSTRAINT crm_pipeline_primary_values_table_id_entity_type_entity_id_key;
