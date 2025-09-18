-- Tourism Database Change Tracking Triggers
-- Automatic audit logging for all DML operations on core tables

-- Function to capture row changes as JSONB
CREATE OR REPLACE FUNCTION row_to_jsonb(record RECORD) RETURNS JSONB AS $$
BEGIN
    RETURN to_jsonb(record);
END;
$$ LANGUAGE plpgsql;

-- Generic audit trigger function
CREATE OR REPLACE FUNCTION audit_trigger_function() RETURNS TRIGGER AS $$
DECLARE
    table_name_var TEXT;
    operation_type_var TEXT;
    old_values_var JSONB;
    new_values_var JSONB;
    change_description_var TEXT;
BEGIN
    -- Get table name from trigger context
    table_name_var := TG_TABLE_NAME;
    operation_type_var := TG_OP;

    -- Capture old and new values based on operation
    IF TG_OP = 'DELETE' THEN
        old_values_var := row_to_jsonb(OLD);
        new_values_var := NULL;
        change_description_var := 'Record deleted';
    ELSIF TG_OP = 'INSERT' THEN
        old_values_var := NULL;
        new_values_var := row_to_jsonb(NEW);
        change_description_var := 'New record created';
    ELSIF TG_OP = 'UPDATE' THEN
        old_values_var := row_to_jsonb(OLD);
        new_values_var := row_to_jsonb(NEW);
        change_description_var := 'Record updated';
    END IF;

    -- Insert into appropriate changelog table
    IF table_name_var = 'logies' THEN
        INSERT INTO logies_changelog (
            entity_id, operation_type, old_values, new_values, change_description
        ) VALUES (
            COALESCE(NEW.id, OLD.id),
            operation_type_var,
            old_values_var,
            new_values_var,
            change_description_var
        );
    ELSIF table_name_var = 'addresses' THEN
        INSERT INTO addresses_changelog (
            entity_id, operation_type, old_values, new_values, change_description
        ) VALUES (
            COALESCE(NEW.id, OLD.id),
            operation_type_var,
            old_values_var,
            new_values_var,
            change_description_var
        );
    ELSIF table_name_var = 'contact_points' THEN
        INSERT INTO contact_points_changelog (
            entity_id, operation_type, old_values, new_values, change_description
        ) VALUES (
            COALESCE(NEW.id, OLD.id),
            operation_type_var,
            old_values_var,
            new_values_var,
            change_description_var
        );
    ELSIF table_name_var = 'geometries' THEN
        INSERT INTO geometries_changelog (
            entity_id, operation_type, old_values, new_values, change_description
        ) VALUES (
            COALESCE(NEW.id, OLD.id),
            operation_type_var,
            old_values_var,
            new_values_var,
            change_description_var
        );
    ELSIF table_name_var = 'identifiers' THEN
        INSERT INTO identifiers_changelog (
            entity_id, operation_type, old_values, new_values, change_description
        ) VALUES (
            COALESCE(NEW.id, OLD.id),
            operation_type_var,
            old_values_var,
            new_values_var,
            change_description_var
        );
    END IF;

    -- Return appropriate record
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Enhanced audit trigger function with run_id support
CREATE OR REPLACE FUNCTION audit_trigger_with_run_id() RETURNS TRIGGER AS $$
DECLARE
    table_name_var TEXT;
    operation_type_var TEXT;
    old_values_var JSONB;
    new_values_var JSONB;
    change_description_var TEXT;
    current_run_id UUID;
BEGIN
    -- Get table name from trigger context
    table_name_var := TG_TABLE_NAME;
    operation_type_var := TG_OP;

    -- Try to get current run_id from session variable
    BEGIN
        current_run_id := current_setting('tourism_db.current_run_id')::UUID;
    EXCEPTION WHEN OTHERS THEN
        current_run_id := NULL;
    END;

    -- Capture old and new values based on operation
    IF TG_OP = 'DELETE' THEN
        old_values_var := row_to_jsonb(OLD);
        new_values_var := NULL;
        change_description_var := 'Record deleted';
    ELSIF TG_OP = 'INSERT' THEN
        old_values_var := NULL;
        new_values_var := row_to_jsonb(NEW);
        change_description_var := 'New record created';
    ELSIF TG_OP = 'UPDATE' THEN
        old_values_var := row_to_jsonb(OLD);
        new_values_var := row_to_jsonb(NEW);
        change_description_var := 'Record updated';
    END IF;

    -- Insert into appropriate changelog table
    IF table_name_var = 'logies' THEN
        INSERT INTO logies_changelog (
            entity_id, operation_type, old_values, new_values, change_description, run_id
        ) VALUES (
            COALESCE(NEW.id, OLD.id),
            operation_type_var,
            old_values_var,
            new_values_var,
            change_description_var,
            current_run_id
        );
    ELSIF table_name_var = 'addresses' THEN
        INSERT INTO addresses_changelog (
            entity_id, operation_type, old_values, new_values, change_description, run_id
        ) VALUES (
            COALESCE(NEW.id, OLD.id),
            operation_type_var,
            old_values_var,
            new_values_var,
            change_description_var,
            current_run_id
        );
    ELSIF table_name_var = 'contact_points' THEN
        INSERT INTO contact_points_changelog (
            entity_id, operation_type, old_values, new_values, change_description, run_id
        ) VALUES (
            COALESCE(NEW.id, OLD.id),
            operation_type_var,
            old_values_var,
            new_values_var,
            change_description_var,
            current_run_id
        );
    ELSIF table_name_var = 'geometries' THEN
        INSERT INTO geometries_changelog (
            entity_id, operation_type, old_values, new_values, change_description, run_id
        ) VALUES (
            COALESCE(NEW.id, OLD.id),
            operation_type_var,
            old_values_var,
            new_values_var,
            change_description_var,
            current_run_id
        );
    ELSIF table_name_var = 'identifiers' THEN
        INSERT INTO identifiers_changelog (
            entity_id, operation_type, old_values, new_values, change_description, run_id
        ) VALUES (
            COALESCE(NEW.id, OLD.id),
            operation_type_var,
            old_values_var,
            new_values_var,
            change_description_var,
            current_run_id
        );
    END IF;

    -- Return appropriate record
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Create triggers for each core table
-- Note: Using AFTER triggers to ensure the main operation completes first

-- Logies table triggers
DROP TRIGGER IF EXISTS logies_audit_trigger ON logies;
CREATE TRIGGER logies_audit_trigger
    AFTER INSERT OR UPDATE OR DELETE ON logies
    FOR EACH ROW
    EXECUTE FUNCTION audit_trigger_with_run_id();

-- Addresses table triggers
DROP TRIGGER IF EXISTS addresses_audit_trigger ON addresses;
CREATE TRIGGER addresses_audit_trigger
    AFTER INSERT OR UPDATE OR DELETE ON addresses
    FOR EACH ROW
    EXECUTE FUNCTION audit_trigger_with_run_id();

-- Contact points table triggers
DROP TRIGGER IF EXISTS contact_points_audit_trigger ON contact_points;
CREATE TRIGGER contact_points_audit_trigger
    AFTER INSERT OR UPDATE OR DELETE ON contact_points
    FOR EACH ROW
    EXECUTE FUNCTION audit_trigger_with_run_id();

-- Geometries table triggers
DROP TRIGGER IF EXISTS geometries_audit_trigger ON geometries;
CREATE TRIGGER geometries_audit_trigger
    AFTER INSERT OR UPDATE OR DELETE ON geometries
    FOR EACH ROW
    EXECUTE FUNCTION audit_trigger_with_run_id();

-- Identifiers table triggers
DROP TRIGGER IF EXISTS identifiers_audit_trigger ON identifiers;
CREATE TRIGGER identifiers_audit_trigger
    AFTER INSERT OR UPDATE OR DELETE ON identifiers
    FOR EACH ROW
    EXECUTE FUNCTION audit_trigger_with_run_id();

-- Utility functions for managing audit triggers

-- Function to disable all audit triggers (for bulk operations)
CREATE OR REPLACE FUNCTION disable_audit_triggers() RETURNS VOID AS $$
BEGIN
    ALTER TABLE logies DISABLE TRIGGER logies_audit_trigger;
    ALTER TABLE addresses DISABLE TRIGGER addresses_audit_trigger;
    ALTER TABLE contact_points DISABLE TRIGGER contact_points_audit_trigger;
    ALTER TABLE geometries DISABLE TRIGGER geometries_audit_trigger;
    ALTER TABLE identifiers DISABLE TRIGGER identifiers_audit_trigger;
    RAISE NOTICE 'All audit triggers disabled';
END;
$$ LANGUAGE plpgsql;

-- Function to enable all audit triggers
CREATE OR REPLACE FUNCTION enable_audit_triggers() RETURNS VOID AS $$
BEGIN
    ALTER TABLE logies ENABLE TRIGGER logies_audit_trigger;
    ALTER TABLE addresses ENABLE TRIGGER addresses_audit_trigger;
    ALTER TABLE contact_points ENABLE TRIGGER contact_points_audit_trigger;
    ALTER TABLE geometries ENABLE TRIGGER geometries_audit_trigger;
    ALTER TABLE identifiers ENABLE TRIGGER identifiers_audit_trigger;
    RAISE NOTICE 'All audit triggers enabled';
END;
$$ LANGUAGE plpgsql;

-- Function to set current run ID for tracking update operations
CREATE OR REPLACE FUNCTION set_current_run_id(run_id UUID) RETURNS VOID AS $$
BEGIN
    PERFORM set_config('tourism_db.current_run_id', run_id::TEXT, false);
    RAISE NOTICE 'Current run ID set to: %', run_id;
END;
$$ LANGUAGE plpgsql;

-- Function to clear current run ID
CREATE OR REPLACE FUNCTION clear_current_run_id() RETURNS VOID AS $$
BEGIN
    PERFORM set_config('tourism_db.current_run_id', '', false);
    RAISE NOTICE 'Current run ID cleared';
END;
$$ LANGUAGE plpgsql;

-- Function to get current run ID
CREATE OR REPLACE FUNCTION get_current_run_id() RETURNS UUID AS $$
DECLARE
    current_run_id UUID;
BEGIN
    BEGIN
        current_run_id := current_setting('tourism_db.current_run_id')::UUID;
        RETURN current_run_id;
    EXCEPTION WHEN OTHERS THEN
        RETURN NULL;
    END;
END;
$$ LANGUAGE plpgsql;

-- Comments for documentation
COMMENT ON FUNCTION audit_trigger_with_run_id() IS 'Primary audit trigger function that captures all DML operations with optional run_id tracking';
COMMENT ON FUNCTION disable_audit_triggers() IS 'Utility function to disable all audit triggers for bulk operations';
COMMENT ON FUNCTION enable_audit_triggers() IS 'Utility function to re-enable all audit triggers after bulk operations';
COMMENT ON FUNCTION set_current_run_id(UUID) IS 'Sets the current update run ID for tracking changes during update operations';
COMMENT ON FUNCTION clear_current_run_id() IS 'Clears the current run ID after update operations complete';
COMMENT ON FUNCTION get_current_run_id() IS 'Returns the current run ID if set, NULL otherwise';