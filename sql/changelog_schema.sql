-- Tourism Database Change Tracking Schema
-- Comprehensive audit log tables for tracking all changes to core entities

-- Update runs metadata table
CREATE TABLE IF NOT EXISTS update_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR(20) NOT NULL CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED')),
    source_file_url TEXT,
    source_file_hash VARCHAR(64),
    source_file_size BIGINT,
    records_added INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_deleted INTEGER DEFAULT 0,
    error_message TEXT,
    created_by VARCHAR(100) DEFAULT 'system'
);

-- Index for update runs by status and date
CREATE INDEX IF NOT EXISTS idx_update_runs_status_date ON update_runs(status, started_at DESC);

-- Logies changelog table
CREATE TABLE IF NOT EXISTS logies_changelog (
    changelog_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id UUID NOT NULL,
    operation_type VARCHAR(10) NOT NULL CHECK (operation_type IN ('INSERT', 'UPDATE', 'DELETE')),
    changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    changed_by VARCHAR(100) DEFAULT 'system',
    run_id UUID REFERENCES update_runs(run_id),
    old_values JSONB,
    new_values JSONB,
    change_source VARCHAR(50) DEFAULT 'ttl_update',
    change_description TEXT
);

-- Addresses changelog table
CREATE TABLE IF NOT EXISTS addresses_changelog (
    changelog_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id UUID NOT NULL,
    operation_type VARCHAR(10) NOT NULL CHECK (operation_type IN ('INSERT', 'UPDATE', 'DELETE')),
    changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    changed_by VARCHAR(100) DEFAULT 'system',
    run_id UUID REFERENCES update_runs(run_id),
    old_values JSONB,
    new_values JSONB,
    change_source VARCHAR(50) DEFAULT 'ttl_update',
    change_description TEXT
);

-- Contact points changelog table
CREATE TABLE IF NOT EXISTS contact_points_changelog (
    changelog_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id UUID NOT NULL,
    operation_type VARCHAR(10) NOT NULL CHECK (operation_type IN ('INSERT', 'UPDATE', 'DELETE')),
    changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    changed_by VARCHAR(100) DEFAULT 'system',
    run_id UUID REFERENCES update_runs(run_id),
    old_values JSONB,
    new_values JSONB,
    change_source VARCHAR(50) DEFAULT 'ttl_update',
    change_description TEXT
);

-- Geometries changelog table
CREATE TABLE IF NOT EXISTS geometries_changelog (
    changelog_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id UUID NOT NULL,
    operation_type VARCHAR(10) NOT NULL CHECK (operation_type IN ('INSERT', 'UPDATE', 'DELETE')),
    changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    changed_by VARCHAR(100) DEFAULT 'system',
    run_id UUID REFERENCES update_runs(run_id),
    old_values JSONB,
    new_values JSONB,
    change_source VARCHAR(50) DEFAULT 'ttl_update',
    change_description TEXT
);

-- Identifiers changelog table
CREATE TABLE IF NOT EXISTS identifiers_changelog (
    changelog_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id UUID NOT NULL,
    operation_type VARCHAR(10) NOT NULL CHECK (operation_type IN ('INSERT', 'UPDATE', 'DELETE')),
    changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    changed_by VARCHAR(100) DEFAULT 'system',
    run_id UUID REFERENCES update_runs(run_id),
    old_values JSONB,
    new_values JSONB,
    change_source VARCHAR(50) DEFAULT 'ttl_update',
    change_description TEXT
);

-- Indexes for changelog tables (optimized for common queries)
CREATE INDEX IF NOT EXISTS idx_logies_changelog_entity_date ON logies_changelog(entity_id, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_logies_changelog_operation_date ON logies_changelog(operation_type, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_logies_changelog_run_id ON logies_changelog(run_id);

CREATE INDEX IF NOT EXISTS idx_addresses_changelog_entity_date ON addresses_changelog(entity_id, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_addresses_changelog_operation_date ON addresses_changelog(operation_type, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_addresses_changelog_run_id ON addresses_changelog(run_id);

CREATE INDEX IF NOT EXISTS idx_contact_points_changelog_entity_date ON contact_points_changelog(entity_id, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_contact_points_changelog_operation_date ON contact_points_changelog(operation_type, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_contact_points_changelog_run_id ON contact_points_changelog(run_id);

CREATE INDEX IF NOT EXISTS idx_geometries_changelog_entity_date ON geometries_changelog(entity_id, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_geometries_changelog_operation_date ON geometries_changelog(operation_type, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_geometries_changelog_run_id ON geometries_changelog(run_id);

CREATE INDEX IF NOT EXISTS idx_identifiers_changelog_entity_date ON identifiers_changelog(entity_id, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_identifiers_changelog_operation_date ON identifiers_changelog(operation_type, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_identifiers_changelog_run_id ON identifiers_changelog(run_id);

-- Views for easy change tracking queries

-- Recent changes view (last 30 days)
CREATE OR replace VIEW recent_changes AS
SELECT
    'logies' as table_name,
    entity_id,
    operation_type,
    changed_at,
    changed_by,
    run_id
FROM logies_changelog
WHERE changed_at >= CURRENT_DATE - INTERVAL '30 days'
UNION ALL
SELECT
    'addresses' as table_name,
    entity_id,
    operation_type,
    changed_at,
    changed_by,
    run_id
FROM addresses_changelog
WHERE changed_at >= CURRENT_DATE - INTERVAL '30 days'
UNION ALL
SELECT
    'contact_points' as table_name,
    entity_id,
    operation_type,
    changed_at,
    changed_by,
    run_id
FROM contact_points_changelog
WHERE changed_at >= CURRENT_DATE - INTERVAL '30 days'
UNION ALL
SELECT
    'geometries' as table_name,
    entity_id,
    operation_type,
    changed_at,
    changed_by,
    run_id
FROM geometries_changelog
WHERE changed_at >= CURRENT_DATE - INTERVAL '30 days'
UNION ALL
SELECT
    'identifiers' as table_name,
    entity_id,
    operation_type,
    changed_at,
    changed_by,
    run_id
FROM identifiers_changelog
WHERE changed_at >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY changed_at DESC;

-- Change summary by run view
CREATE OR replace VIEW change_summary_by_run AS
SELECT
    ur.run_id,
    ur.started_at,
    ur.completed_at,
    ur.status,
    ur.source_file_url,
    COALESCE(ur.records_added, 0) as records_added,
    COALESCE(ur.records_updated, 0) as records_updated,
    COALESCE(ur.records_deleted, 0) as records_deleted,
    (COALESCE(ur.records_added, 0) + COALESCE(ur.records_updated, 0) + COALESCE(ur.records_deleted, 0)) as total_changes
FROM update_runs ur
ORDER BY ur.started_at DESC;

-- Entity change history view
CREATE OR replace VIEW entity_change_history AS
SELECT
    'logies' as table_name,
    entity_id,
    operation_type,
    changed_at,
    old_values,
    new_values,
    change_description,
    run_id
FROM logies_changelog
UNION ALL
SELECT
    'addresses' as table_name,
    entity_id,
    operation_type,
    changed_at,
    old_values,
    new_values,
    change_description,
    run_id
FROM addresses_changelog
UNION ALL
SELECT
    'contact_points' as table_name,
    entity_id,
    operation_type,
    changed_at,
    old_values,
    new_values,
    change_description,
    run_id
FROM contact_points_changelog
UNION ALL
SELECT
    'geometries' as table_name,
    entity_id,
    operation_type,
    changed_at,
    old_values,
    new_values,
    change_description,
    run_id
FROM geometries_changelog
UNION ALL
SELECT
    'identifiers' as table_name,
    entity_id,
    operation_type,
    changed_at,
    old_values,
    new_values,
    change_description,
    run_id
FROM identifiers_changelog
ORDER BY changed_at DESC;

-- Comments for documentation
COMMENT ON TABLE update_runs IS 'Tracks metadata for each TTL update run including timing, status, and change counts';
COMMENT ON TABLE logies_changelog IS 'Audit log of all changes to the logies table with before/after values';
COMMENT ON TABLE addresses_changelog IS 'Audit log of all changes to the addresses table with before/after values';
COMMENT ON TABLE contact_points_changelog IS 'Audit log of all changes to the contact_points table with before/after values';
COMMENT ON TABLE geometries_changelog IS 'Audit log of all changes to the geometries table with before/after values';
COMMENT ON TABLE identifiers_changelog IS 'Audit log of all changes to the identifiers table with before/after values';

COMMENT ON VIEW recent_changes IS 'Shows all changes across all tables from the last 30 days';
COMMENT ON VIEW change_summary_by_run IS 'Summarizes change counts for each update run';
COMMENT ON VIEW entity_change_history IS 'Complete change history for all entities with full details';