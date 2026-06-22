BEGIN;

-- Core tenants table for customer isolation and quota definitions.
CREATE TABLE tenants (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    channel_limit INT NOT NULL DEFAULT 10,
    ws_limit INT NOT NULL DEFAULT 100,
    rate_limit INT NOT NULL DEFAULT 100,
    rate_window INT NOT NULL DEFAULT 60,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- API keys for authentication. We store SHA-256 hashes, not plaintext keys.
CREATE TABLE api_keys (
    key_hash TEXT PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Ensure a default tenant exists to attach any existing data.
INSERT INTO tenants (id, name) VALUES ('00000000-0000-0000-0000-000000000000', 'Default Tenant');

-- Alter channels to belong to a tenant.
ALTER TABLE channels ADD COLUMN tenant_id UUID REFERENCES tenants(id) ON DELETE CASCADE;

-- Backfill existing channels to the default tenant.
UPDATE channels SET tenant_id = '00000000-0000-0000-0000-000000000000' WHERE tenant_id IS NULL;

-- Make tenant_id NOT NULL after backfilling.
ALTER TABLE channels ALTER COLUMN tenant_id SET NOT NULL;

-- Update unique constraint to be unique per tenant, rather than globally unique.
DROP INDEX IF EXISTS channels_name_key;
CREATE UNIQUE INDEX channels_tenant_name_key ON channels (tenant_id, lower(name));

COMMIT;
