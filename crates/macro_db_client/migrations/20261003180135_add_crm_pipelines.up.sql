-- A managed database obtains all its access from its owning entity.
ALTER TABLE databases ADD COLUMN access_entity_id UUID;
ALTER TABLE databases ADD COLUMN access_entity_type TEXT;
ALTER TABLE databases ADD CONSTRAINT database_access_owner CHECK (
    (access_entity_id IS NULL) = (access_entity_type IS NULL)
);
CREATE INDEX databases_access_entity ON databases (
    (COALESCE(access_entity_id, id)), (COALESCE(access_entity_type, 'database'))
);

-- A primary reference is required and unique within its table. Keep its
-- index relational even though the editable value lives in entity_properties.
CREATE TABLE database_primary_columns (
    table_id UUID PRIMARY KEY REFERENCES database_tables(id) ON DELETE CASCADE,
    column_id UUID NOT NULL UNIQUE REFERENCES database_columns(id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE database_primary_values (
    row_id UUID PRIMARY KEY REFERENCES database_rows(id) ON DELETE CASCADE,
    table_id UUID NOT NULL REFERENCES database_primary_columns(table_id) ON DELETE CASCADE,
    entity_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    UNIQUE (table_id, entity_type, entity_id)
);

CREATE TABLE crm_pipelines (
    id UUID PRIMARY KEY,
    team_id UUID NOT NULL REFERENCES team(id) ON DELETE CASCADE,
    database_id UUID NOT NULL UNIQUE REFERENCES databases(id) ON DELETE CASCADE,
    table_id UUID NOT NULL UNIQUE REFERENCES database_primary_columns(table_id) DEFERRABLE INITIALLY DEFERRED,
    record_type TEXT NOT NULL CHECK (record_type IN ('company', 'contact'))
);
CREATE INDEX crm_pipelines_team ON crm_pipelines(team_id);

CREATE FUNCTION maintain_database_primary_value() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    primary_table UUID;
    primary_row UUID;
    reference JSONB;
BEGIN
    IF TG_OP = 'DELETE' OR (TG_OP = 'UPDATE' AND (OLD.entity_id IS DISTINCT FROM NEW.entity_id
        OR OLD.entity_type IS DISTINCT FROM NEW.entity_type
        OR OLD.property_definition_id IS DISTINCT FROM NEW.property_definition_id)) THEN
        BEGIN primary_row := OLD.entity_id::uuid;
        EXCEPTION WHEN invalid_text_representation THEN primary_row := NULL; END;
        DELETE FROM database_primary_values WHERE row_id = primary_row
          AND EXISTS (SELECT 1 FROM database_primary_columns p
                      JOIN database_columns c ON c.id = p.column_id
                      WHERE c.property_definition_id = OLD.property_definition_id
                        AND p.table_id = database_primary_values.table_id);
        IF TG_OP = 'DELETE' THEN RETURN NULL; END IF;
    END IF;
    IF NEW.entity_type != 'DATABASE_ROW' THEN RETURN NULL; END IF;
    BEGIN primary_row := NEW.entity_id::uuid;
    EXCEPTION WHEN invalid_text_representation THEN RETURN NULL; END;
    SELECT p.table_id INTO primary_table FROM database_primary_columns p
    JOIN database_columns c ON c.id = p.column_id
    JOIN database_rows r ON r.table_id = p.table_id AND r.id = primary_row
    WHERE c.property_definition_id = NEW.property_definition_id;
    IF primary_table IS NULL THEN RETURN NULL; END IF;
    IF NEW.values ->> 'type' IS DISTINCT FROM 'EntityReference'
       OR jsonb_typeof(NEW.values -> 'value') IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'The primary column requires a record' USING ERRCODE = '23514', CONSTRAINT = 'database_primary_record_required';
    END IF;
    IF jsonb_array_length(NEW.values -> 'value') != 1 THEN
        RAISE EXCEPTION 'The primary column requires exactly one record' USING ERRCODE = '23514', CONSTRAINT = 'database_primary_record_required';
    END IF;
    reference := NEW.values -> 'value' -> 0;
    IF COALESCE(reference ->> 'entity_id', '') = '' THEN
        RAISE EXCEPTION 'The primary column requires a record' USING ERRCODE = '23514', CONSTRAINT = 'database_primary_record_required';
    END IF;
    INSERT INTO database_primary_values (row_id, table_id, entity_id, entity_type)
    VALUES (NEW.entity_id::uuid, primary_table, reference ->> 'entity_id', reference ->> 'entity_type')
    ON CONFLICT (row_id) DO UPDATE SET entity_id = EXCLUDED.entity_id, entity_type = EXCLUDED.entity_type;
    RETURN NULL;
END $$;
CREATE TRIGGER database_primary_value_insert AFTER INSERT ON entity_properties
FOR EACH ROW WHEN (NEW.entity_type = 'DATABASE_ROW') EXECUTE FUNCTION maintain_database_primary_value();
CREATE TRIGGER database_primary_value_write AFTER UPDATE ON entity_properties
FOR EACH ROW WHEN (OLD.entity_type = 'DATABASE_ROW' OR NEW.entity_type = 'DATABASE_ROW') EXECUTE FUNCTION maintain_database_primary_value();
CREATE TRIGGER database_primary_value_delete AFTER DELETE ON entity_properties
FOR EACH ROW WHEN (OLD.entity_type = 'DATABASE_ROW') EXECUTE FUNCTION maintain_database_primary_value();

CREATE FUNCTION require_database_primary_value() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE target UUID;
BEGIN
    IF TG_TABLE_NAME = 'database_rows' THEN target := NEW.id;
    ELSE target := OLD.row_id; END IF;
    IF EXISTS (SELECT 1 FROM database_rows r JOIN database_primary_columns p ON p.table_id = r.table_id
               WHERE r.id = target AND NOT EXISTS (SELECT 1 FROM database_primary_values v WHERE v.row_id = r.id)) THEN
        RAISE EXCEPTION 'The primary column requires a record' USING ERRCODE = '23514', CONSTRAINT = 'database_primary_record_required';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER database_row_requires_primary AFTER INSERT OR UPDATE ON database_rows
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_database_primary_value();
CREATE CONSTRAINT TRIGGER database_primary_value_required AFTER DELETE ON database_primary_values
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_database_primary_value();

CREATE FUNCTION protect_database_primary_column() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_TABLE_NAME = 'database_columns' THEN
        IF EXISTS (SELECT 1 FROM database_primary_columns WHERE column_id = OLD.id)
           AND (NEW.property_definition_id != OLD.property_definition_id OR NEW.table_id != OLD.table_id
                OR NEW.config IS DISTINCT FROM OLD.config) THEN
            RAISE EXCEPTION 'The primary column cannot change its reference type' USING ERRCODE = '23514', CONSTRAINT = 'database_primary_column_protected';
        END IF;
    ELSE
        IF EXISTS (SELECT 1 FROM database_primary_columns p JOIN database_columns c ON c.id = p.column_id
                   WHERE c.property_definition_id = OLD.id)
           AND (NEW.data_type IS DISTINCT FROM OLD.data_type
                OR NEW.is_multi_select IS DISTINCT FROM OLD.is_multi_select
                OR NEW.specific_entity_type IS DISTINCT FROM OLD.specific_entity_type) THEN
            RAISE EXCEPTION 'The primary column cannot change its reference type' USING ERRCODE = '23514', CONSTRAINT = 'database_primary_column_protected';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER database_primary_column_type BEFORE UPDATE ON database_columns
FOR EACH ROW EXECUTE FUNCTION protect_database_primary_column();
CREATE TRIGGER database_primary_definition_type BEFORE UPDATE ON property_definitions
FOR EACH ROW EXECUTE FUNCTION protect_database_primary_column();

-- The CRM owns the referenced record boundary; a pipeline cannot contain
-- records from another team's CRM, including through SQL or bulk writes.
CREATE FUNCTION check_crm_pipeline_record() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE pipeline crm_pipelines; record_id UUID;
BEGIN
    SELECT * INTO pipeline FROM crm_pipelines WHERE table_id = NEW.table_id;
    IF pipeline.id IS NULL THEN RETURN NEW; END IF;
    BEGIN
        record_id := NEW.entity_id::uuid;
    EXCEPTION WHEN invalid_text_representation THEN
        RAISE EXCEPTION 'Choose a valid CRM record' USING ERRCODE = '23514', CONSTRAINT = 'database_primary_record_invalid';
    END;
    NEW.entity_id := record_id::text;
    IF pipeline.record_type = 'company' THEN
        IF NEW.entity_type != 'COMPANY' OR NOT EXISTS (
            SELECT 1 FROM crm_companies WHERE id = record_id AND team_id = pipeline.team_id
        ) THEN RAISE EXCEPTION 'Choose a company from this CRM' USING ERRCODE = '23514', CONSTRAINT = 'database_primary_record_invalid'; END IF;
    ELSE
        IF NEW.entity_type != 'CONTACT' OR NOT EXISTS (
            SELECT 1 FROM crm_contacts contact JOIN crm_companies company ON company.id = contact.company_id
            WHERE contact.id = record_id AND company.team_id = pipeline.team_id
        ) THEN RAISE EXCEPTION 'Choose a contact from this CRM' USING ERRCODE = '23514', CONSTRAINT = 'database_primary_record_invalid'; END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER crm_pipeline_record BEFORE INSERT OR UPDATE ON database_primary_values
FOR EACH ROW EXECUTE FUNCTION check_crm_pipeline_record();

CREATE FUNCTION clean_up_crm_pipeline() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM entity_access WHERE entity_id = OLD.id AND entity_type = 'crm_pipeline';
    DELETE FROM databases WHERE id = OLD.database_id;
    RETURN NULL;
END $$;
CREATE TRIGGER crm_pipeline_cleanup AFTER DELETE ON crm_pipelines
FOR EACH ROW EXECUTE FUNCTION clean_up_crm_pipeline();
