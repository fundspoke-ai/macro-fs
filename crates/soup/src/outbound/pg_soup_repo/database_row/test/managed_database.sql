-- The database inherits its pipeline's grants. A stale grant on the backing
-- database must not grant access, even when it is stronger than a pipeline grant.
UPDATE databases
SET access_entity_id = 'a1000000-0000-0000-0000-000000000001',
    access_entity_type = 'crm_pipeline'
WHERE id = 'db000000-0000-0000-0000-000000000001';
DELETE FROM entity_access
WHERE entity_id = 'db000000-0000-0000-0000-000000000001';

INSERT INTO team (id, name, owner_id) VALUES
('ae000000-0000-0000-0000-000000000001', 'Sales team', 'macro|owner@databases.test');
INSERT INTO team_user (team_id, user_id, team_role) VALUES
('ae000000-0000-0000-0000-000000000001', 'macro|viewer@databases.test', 'member');
INSERT INTO entity_access (entity_id, entity_type, source_id, source_type, access_level) VALUES
('a1000000-0000-0000-0000-000000000001', 'crm_pipeline', 'macro|owner@databases.test', 'user', 'owner'),
('a1000000-0000-0000-0000-000000000001', 'crm_pipeline', 'ae000000-0000-0000-0000-000000000001', 'team', 'edit'),
('db000000-0000-0000-0000-000000000001', 'database', 'macro|stranger@databases.test', 'user', 'owner');
