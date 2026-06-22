BEGIN;

-- Revert channel name uniqueness back to global.
DROP INDEX IF EXISTS channels_tenant_name_key;
CREATE UNIQUE INDEX channels_name_key ON channels (lower(name));

-- Remove tenant isolation from channels.
ALTER TABLE channels DROP COLUMN IF EXISTS tenant_id;

-- Drop new tables.
DROP TABLE IF EXISTS api_keys;
DROP TABLE IF EXISTS tenants;

COMMIT;
