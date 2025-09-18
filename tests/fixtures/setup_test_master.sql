-- Test Master Database Setup
-- Creates a clean tourism_test_master database with sample data for testing

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create core tables (simplified version for testing)

-- Logies table
CREATE TABLE IF NOT EXISTS logies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    uri TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    alternative_name TEXT,
    description TEXT,
    sleeping_places INTEGER NOT NULL DEFAULT 1,
    rental_units_count INTEGER NOT NULL DEFAULT 1,
    accessibility_summary TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Addresses table
CREATE TABLE IF NOT EXISTS addresses (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    street_name TEXT,
    house_number TEXT,
    postal_code TEXT,
    municipality TEXT NOT NULL,
    country TEXT DEFAULT 'Belgium',
    full_address TEXT,
    logies_id UUID REFERENCES logies(id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Contact points table
CREATE TABLE IF NOT EXISTS contact_points (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email TEXT,
    telephone TEXT,
    fax TEXT,
    website TEXT,
    contact_type TEXT,
    logies_id UUID REFERENCES logies(id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Geometries table
CREATE TABLE IF NOT EXISTS geometries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    geometry_type TEXT DEFAULT 'Point',
    coordinate_system TEXT DEFAULT 'WGS84',
    logies_id UUID REFERENCES logies(id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Identifiers table
CREATE TABLE IF NOT EXISTS identifiers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    identifier_value TEXT NOT NULL,
    identifier_type TEXT,
    notation TEXT,
    related_entity_id UUID,
    related_entity_type TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_logies_name ON logies(name);
CREATE INDEX IF NOT EXISTS idx_addresses_municipality ON addresses(municipality);
CREATE INDEX IF NOT EXISTS idx_addresses_logies_id ON addresses(logies_id);
CREATE INDEX IF NOT EXISTS idx_contact_points_logies_id ON contact_points(logies_id);
CREATE INDEX IF NOT EXISTS idx_geometries_logies_id ON geometries(logies_id);
CREATE INDEX IF NOT EXISTS idx_identifiers_related_entity ON identifiers(related_entity_id, related_entity_type);

-- Insert sample test data

-- Sample logies (accommodations)
INSERT INTO logies (id, uri, name, description, sleeping_places, rental_units_count) VALUES
('11111111-1111-1111-1111-111111111111', 'https://data.vlaanderen.be/id/logies/test-hotel-1', 'Test Hotel Brussels', 'Comfortable hotel in the heart of Brussels', 4, 1),
('22222222-2222-2222-2222-222222222222', 'https://data.vlaanderen.be/id/logies/test-bnb-1', 'Cozy B&B Antwerp', 'Charming bed and breakfast in historic Antwerp', 2, 1),
('33333333-3333-3333-3333-333333333333', 'https://data.vlaanderen.be/id/logies/test-apartment-1', 'Modern Apartment Ghent', 'Contemporary apartment near Ghent University', 6, 1),
('44444444-4444-4444-4444-444444444444', 'https://data.vlaanderen.be/id/logies/test-hostel-1', 'Budget Hostel Bruges', 'Affordable accommodation for backpackers', 8, 2),
('55555555-5555-5555-5555-555555555555', 'https://data.vlaanderen.be/id/logies/test-villa-1', 'Luxury Villa Leuven', 'Exclusive villa with premium amenities', 10, 1)
ON CONFLICT (id) DO NOTHING;

-- Sample addresses
INSERT INTO addresses (id, street_name, house_number, postal_code, municipality, full_address, logies_id) VALUES
('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'Grand Place', '1', '1000', 'Brussels', 'Grand Place 1, 1000 Brussels', '11111111-1111-1111-1111-111111111111'),
('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'Grote Markt', '15', '2000', 'Antwerpen', 'Grote Markt 15, 2000 Antwerpen', '22222222-2222-2222-2222-222222222222'),
('cccccccc-cccc-cccc-cccc-cccccccccccc', 'Sint-Pietersnieuwstraat', '45', '9000', 'Gent', 'Sint-Pietersnieuwstraat 45, 9000 Gent', '33333333-3333-3333-3333-333333333333'),
('dddddddd-dddd-dddd-dddd-dddddddddddd', 'Langestraat', '8', '8000', 'Brugge', 'Langestraat 8, 8000 Brugge', '44444444-4444-4444-4444-444444444444'),
('eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee', 'Bondgenotenlaan', '101', '3000', 'Leuven', 'Bondgenotenlaan 101, 3000 Leuven', '55555555-5555-5555-5555-555555555555')
ON CONFLICT (id) DO NOTHING;

-- Sample contact points
INSERT INTO contact_points (id, email, telephone, website, contact_type, logies_id) VALUES
('1a1a1a1a-1a1a-1a1a-1a1a-1a1a1a1a1a1a', 'info@testhotelbrussels.be', '+32 2 123 4567', 'https://testhotelbrussels.be', 'reception', '11111111-1111-1111-1111-111111111111'),
('2b2b2b2b-2b2b-2b2b-2b2b-2b2b2b2b2b2b', 'welcome@cozybnbantwerp.be', '+32 3 234 5678', 'https://cozybnbantwerp.be', 'host', '22222222-2222-2222-2222-222222222222'),
('3c3c3c3c-3c3c-3c3c-3c3c-3c3c3c3c3c3c', 'booking@modernapartmentghent.be', '+32 9 345 6789', 'https://modernapartmentghent.be', 'booking', '33333333-3333-3333-3333-333333333333'),
('4d4d4d4d-4d4d-4d4d-4d4d-4d4d4d4d4d4d', 'stay@budgethostelbruges.be', '+32 50 456 7890', 'https://budgethostelbruges.be', 'reception', '44444444-4444-4444-4444-444444444444'),
('5e5e5e5e-5e5e-5e5e-5e5e-5e5e5e5e5e5e', 'concierge@luxuryvillaleuven.be', '+32 16 567 8901', 'https://luxuryvillaleuven.be', 'concierge', '55555555-5555-5555-5555-555555555555')
ON CONFLICT (id) DO NOTHING;

-- Sample geometries
INSERT INTO geometries (id, latitude, longitude, logies_id) VALUES
('a1a1a1a1-a1a1-a1a1-a1a1-a1a1a1a1a1a1', 50.8476, 4.3572, '11111111-1111-1111-1111-111111111111'),  -- Brussels
('b2b2b2b2-b2b2-b2b2-b2b2-b2b2b2b2b2b2', 51.2194, 4.4025, '22222222-2222-2222-2222-222222222222'),  -- Antwerp
('c3c3c3c3-c3c3-c3c3-c3c3-c3c3c3c3c3c3', 51.0543, 3.7174, '33333333-3333-3333-3333-333333333333'),  -- Ghent
('d4d4d4d4-d4d4-d4d4-d4d4-d4d4d4d4d4d4', 51.2093, 3.2247, '44444444-4444-4444-4444-444444444444'),  -- Bruges
('e5e5e5e5-e5e5-e5e5-e5e5-e5e5e5e5e5e5', 50.8798, 4.7005, '55555555-5555-5555-5555-555555555555')   -- Leuven
ON CONFLICT (id) DO NOTHING;

-- Sample identifiers
INSERT INTO identifiers (id, identifier_value, identifier_type, related_entity_id, related_entity_type) VALUES
('1a1a1a1a-1a1a-1a1a-1a1a-1a1a1a1a1a1a', 'TVL001', 'tourism_flanders_id', '11111111-1111-1111-1111-111111111111', 'logies'),
('2b2b2b2b-2b2b-2b2b-2b2b-2b2b2b2b2b2b', 'TVL002', 'tourism_flanders_id', '22222222-2222-2222-2222-222222222222', 'logies'),
('3c3c3c3c-3c3c-3c3c-3c3c-3c3c3c3c3c3c', 'TVL003', 'tourism_flanders_id', '33333333-3333-3333-3333-333333333333', 'logies'),
('4d4d4d4d-4d4d-4d4d-4d4d-4d4d4d4d4d4d', 'TVL004', 'tourism_flanders_id', '44444444-4444-4444-4444-444444444444', 'logies'),
('5e5e5e5e-5e5e-5e5e-5e5e-5e5e5e5e5e5e', 'TVL005', 'tourism_flanders_id', '55555555-5555-5555-5555-555555555555', 'logies')
ON CONFLICT (id) DO NOTHING;

-- Create summary view for easy testing
CREATE OR REPLACE VIEW test_data_summary AS
SELECT
    'logies' as table_name, COUNT(*) as record_count
FROM logies
UNION ALL
SELECT 'addresses', COUNT(*) FROM addresses
UNION ALL
SELECT 'contact_points', COUNT(*) FROM contact_points
UNION ALL
SELECT 'geometries', COUNT(*) FROM geometries
UNION ALL
SELECT 'identifiers', COUNT(*) FROM identifiers;

-- Display summary
SELECT * FROM test_data_summary;