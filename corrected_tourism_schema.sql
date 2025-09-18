-- Corrected Tourism Database Schema
-- Based on official Logies Basis Application Profile
-- https://data.vlaanderen.be/doc/applicatieprofiel/logies-basis/

CREATE DATABASE tourism_flanders_corrected;

\c tourism_flanders_corrected;

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- CORE ENTITIES BASED ON OFFICIAL LOGIES MODEL
-- ============================================================================

-- Logies (Accommodations) - Primary entity for overnight stays
CREATE TABLE logies (
    id UUID PRIMARY KEY,
    uri TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL, -- MANDATORY in official model
    alternative_name TEXT,
    description TEXT,
    sleeping_places INTEGER NOT NULL, -- MANDATORY: total sleeping capacity
    rental_units_count INTEGER NOT NULL, -- MANDATORY: number of rental units
    accessibility_summary TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tourist Attractions (separate from Logies)
CREATE TABLE tourist_attractions (
    id UUID PRIMARY KEY,
    uri TEXT UNIQUE NOT NULL,
    name TEXT,
    alternative_name TEXT,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Rental Units (Verhuureenheden) - MANDATORY 1..* relationship with Logies
CREATE TABLE rental_units (
    id UUID PRIMARY KEY,
    uri TEXT UNIQUE NOT NULL,
    logies_id UUID REFERENCES logies(id) ON DELETE CASCADE NOT NULL,
    name TEXT,
    description TEXT,
    sleeping_places INTEGER,
    number_of_rooms INTEGER,
    unit_type TEXT, -- apartment, house, room, etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Rooms (Ruimtes) - Individual rooms within rental units
CREATE TABLE rooms (
    id UUID PRIMARY KEY,
    uri TEXT UNIQUE NOT NULL,
    rental_unit_id UUID REFERENCES rental_units(id) ON DELETE CASCADE,
    name TEXT,
    description TEXT,
    room_type TEXT, -- bedroom, bathroom, living_room, etc.
    sleeping_places INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Facilities (Faciliteiten)
CREATE TABLE facilities (
    id UUID PRIMARY KEY,
    uri TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    facility_type TEXT, -- amenity, service, equipment, etc.
    availability_info TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Quality Labels (Kwaliteitslabels)
CREATE TABLE quality_labels (
    id UUID PRIMARY KEY,
    uri TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    issuing_organization TEXT,
    label_type TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- MULTILINGUAL SUPPORT (TaalString implementation)
-- ============================================================================

CREATE TABLE multilingual_texts (
    id UUID PRIMARY KEY,
    entity_id UUID NOT NULL,
    entity_type TEXT NOT NULL, -- 'logies', 'tourist_attraction', 'facility', etc.
    field_name TEXT NOT NULL, -- 'name', 'description', 'alternative_name'
    language TEXT NOT NULL, -- 'nl', 'en', 'fr', 'de'
    text_value TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(entity_id, entity_type, field_name, language)
);

-- ============================================================================
-- SUPPORTING ENTITIES (Updated)
-- ============================================================================

-- Addresses (enhanced with proper address types)
CREATE TABLE addresses (
    id UUID PRIMARY KEY,
    uri TEXT UNIQUE NOT NULL,
    country TEXT,
    municipality TEXT,
    street_name TEXT,
    house_number TEXT,
    house_number_suffix TEXT,
    postal_code TEXT,
    full_address TEXT,
    province TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Geographic coordinates
CREATE TABLE geometries (
    id UUID PRIMARY KEY,
    uri TEXT UNIQUE NOT NULL,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    geometry_type TEXT DEFAULT 'Point',
    wkt_geometry TEXT, -- Well-Known Text format
    gml_geometry TEXT, -- Geography Markup Language
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Contact points (enhanced)
CREATE TABLE contact_points (
    id UUID PRIMARY KEY,
    uri TEXT UNIQUE NOT NULL,
    telephone TEXT,
    email TEXT,
    website TEXT,
    fax TEXT,
    contact_type TEXT DEFAULT 'general',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Registrations (corrected - linked to Logies, not tourist_attractions)
CREATE TABLE registrations (
    id UUID PRIMARY KEY,
    uri TEXT UNIQUE NOT NULL,
    logies_id UUID REFERENCES logies(id) ON DELETE CASCADE NOT NULL,
    registration_type TEXT NOT NULL, -- MANDATORY in official model
    registration_status TEXT,
    registration_number TEXT,
    valid_from DATE,
    valid_until DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Ratings/Reviews (enhanced)
CREATE TABLE ratings (
    id UUID PRIMARY KEY,
    uri TEXT UNIQUE NOT NULL,
    rated_entity_id UUID NOT NULL,
    rated_entity_type TEXT NOT NULL, -- 'logies', 'tourist_attraction'
    rating_value DECIMAL(3, 2),
    best_rating DECIMAL(3, 2),
    worst_rating DECIMAL(3, 2),
    rating_scale TEXT, -- 1-5, 1-10, etc.
    review_text TEXT,
    author TEXT,
    rating_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Media objects (enhanced)
CREATE TABLE media_objects (
    id UUID PRIMARY KEY,
    uri TEXT UNIQUE NOT NULL,
    related_entity_id UUID NOT NULL,
    related_entity_type TEXT NOT NULL, -- 'logies', 'tourist_attraction', 'rental_unit'
    content_url TEXT,
    media_type TEXT, -- image, video, audio, document
    media_format TEXT, -- jpeg, png, mp4, pdf
    title TEXT,
    description TEXT,
    alt_text TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tourism regions
CREATE TABLE tourism_regions (
    id UUID PRIMARY KEY,
    uri TEXT UNIQUE NOT NULL,
    name TEXT,
    description TEXT,
    region_type TEXT, -- province, municipality, tourist_region
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Identifiers (enhanced with proper constraints)
CREATE TABLE identifiers (
    id UUID PRIMARY KEY,
    uri TEXT UNIQUE NOT NULL,
    identifier_value TEXT NOT NULL,
    notation TEXT,
    identifier_type TEXT NOT NULL,
    schema_agency TEXT, -- issuing organization
    related_entity_id UUID NOT NULL,
    related_entity_type TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(identifier_value, identifier_type, schema_agency)
);

-- ============================================================================
-- RELATIONSHIP TABLES (Many-to-Many)
-- ============================================================================

-- Logies to addresses (with proper address types)
CREATE TABLE logies_addresses (
    logies_id UUID REFERENCES logies(id) ON DELETE CASCADE,
    address_id UUID REFERENCES addresses(id) ON DELETE CASCADE,
    address_type TEXT NOT NULL, -- 'onthaal_address' (reception), 'location_address'
    PRIMARY KEY (logies_id, address_id, address_type)
);

-- Tourist attractions to addresses
CREATE TABLE attraction_addresses (
    tourist_attraction_id UUID REFERENCES tourist_attractions(id) ON DELETE CASCADE,
    address_id UUID REFERENCES addresses(id) ON DELETE CASCADE,
    address_type TEXT DEFAULT 'location_address',
    PRIMARY KEY (tourist_attraction_id, address_id, address_type)
);

-- Logies to geometries
CREATE TABLE logies_geometries (
    logies_id UUID REFERENCES logies(id) ON DELETE CASCADE,
    geometry_id UUID REFERENCES geometries(id) ON DELETE CASCADE,
    geometry_type TEXT, -- 'onthaal_location', 'location'
    PRIMARY KEY (logies_id, geometry_id)
);

-- Tourist attractions to geometries
CREATE TABLE attraction_geometries (
    tourist_attraction_id UUID REFERENCES tourist_attractions(id) ON DELETE CASCADE,
    geometry_id UUID REFERENCES geometries(id) ON DELETE CASCADE,
    PRIMARY KEY (tourist_attraction_id, geometry_id)
);

-- Logies to contact points
CREATE TABLE logies_contacts (
    logies_id UUID REFERENCES logies(id) ON DELETE CASCADE,
    contact_point_id UUID REFERENCES contact_points(id) ON DELETE CASCADE,
    PRIMARY KEY (logies_id, contact_point_id)
);

-- Tourist attractions to contact points
CREATE TABLE attraction_contacts (
    tourist_attraction_id UUID REFERENCES tourist_attractions(id) ON DELETE CASCADE,
    contact_point_id UUID REFERENCES contact_points(id) ON DELETE CASCADE,
    PRIMARY KEY (tourist_attraction_id, contact_point_id)
);

-- Logies to facilities
CREATE TABLE logies_facilities (
    logies_id UUID REFERENCES logies(id) ON DELETE CASCADE,
    facility_id UUID REFERENCES facilities(id) ON DELETE CASCADE,
    availability_period TEXT, -- when facility is available
    additional_info TEXT,
    PRIMARY KEY (logies_id, facility_id)
);

-- Rental units to facilities
CREATE TABLE rental_unit_facilities (
    rental_unit_id UUID REFERENCES rental_units(id) ON DELETE CASCADE,
    facility_id UUID REFERENCES facilities(id) ON DELETE CASCADE,
    PRIMARY KEY (rental_unit_id, facility_id)
);

-- Logies to quality labels
CREATE TABLE logies_quality_labels (
    logies_id UUID REFERENCES logies(id) ON DELETE CASCADE,
    quality_label_id UUID REFERENCES quality_labels(id) ON DELETE CASCADE,
    awarded_date DATE,
    valid_until DATE,
    PRIMARY KEY (logies_id, quality_label_id)
);

-- Logies to tourism regions
CREATE TABLE logies_regions (
    logies_id UUID REFERENCES logies(id) ON DELETE CASCADE,
    tourism_region_id UUID REFERENCES tourism_regions(id) ON DELETE CASCADE,
    PRIMARY KEY (logies_id, tourism_region_id)
);

-- Tourist attractions to tourism regions
CREATE TABLE attraction_regions (
    tourist_attraction_id UUID REFERENCES tourist_attractions(id) ON DELETE CASCADE,
    tourism_region_id UUID REFERENCES tourism_regions(id) ON DELETE CASCADE,
    PRIMARY KEY (tourist_attraction_id, tourism_region_id)
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Core entity indexes
CREATE INDEX idx_logies_name ON logies(name);
CREATE INDEX idx_logies_sleeping_places ON logies(sleeping_places);
CREATE INDEX idx_tourist_attractions_name ON tourist_attractions(name);

-- Multilingual support indexes
CREATE INDEX idx_multilingual_texts_entity ON multilingual_texts(entity_id, entity_type);
CREATE INDEX idx_multilingual_texts_language ON multilingual_texts(language);

-- Rental units and rooms
CREATE INDEX idx_rental_units_logies_id ON rental_units(logies_id);
CREATE INDEX idx_rooms_rental_unit_id ON rooms(rental_unit_id);

-- Support entity indexes
CREATE INDEX idx_addresses_municipality ON addresses(municipality);
CREATE INDEX idx_addresses_postal_code ON addresses(postal_code);
CREATE INDEX idx_geometries_lat_lng ON geometries(latitude, longitude);
CREATE INDEX idx_contact_points_email ON contact_points(email);

-- Registration and rating indexes
CREATE INDEX idx_registrations_logies_id ON registrations(logies_id);
CREATE INDEX idx_ratings_entity ON ratings(rated_entity_id, rated_entity_type);

-- Media and identifier indexes
CREATE INDEX idx_media_objects_entity ON media_objects(related_entity_id, related_entity_type);
CREATE INDEX idx_identifiers_entity ON identifiers(related_entity_id, related_entity_type);
CREATE INDEX idx_identifiers_value ON identifiers(identifier_value);

-- ============================================================================
-- CONSTRAINTS TO ENFORCE OFFICIAL MODEL REQUIREMENTS
-- ============================================================================

-- Logies must have at least one rental unit (enforced at application level)
-- This could be implemented as a trigger, but we'll handle it in application logic

-- Add check constraints for valid values
ALTER TABLE logies ADD CONSTRAINT chk_logies_sleeping_places CHECK (sleeping_places > 0);
ALTER TABLE logies ADD CONSTRAINT chk_logies_rental_units CHECK (rental_units_count > 0);
ALTER TABLE rental_units ADD CONSTRAINT chk_rental_sleeping_places CHECK (sleeping_places >= 0);
ALTER TABLE rooms ADD CONSTRAINT chk_room_sleeping_places CHECK (sleeping_places >= 0);

-- ============================================================================
-- FUNCTIONS AND TRIGGERS
-- ============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Add triggers for updated_at
CREATE TRIGGER update_logies_updated_at BEFORE UPDATE ON logies FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_tourist_attractions_updated_at BEFORE UPDATE ON tourist_attractions FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- VIEWS FOR COMMON QUERIES
-- ============================================================================

-- Complete logies view with all related information
CREATE VIEW logies_complete AS
SELECT
    l.id,
    l.uri,
    l.name,
    l.alternative_name,
    l.description,
    l.sleeping_places,
    l.rental_units_count,
    -- Address information (onthaal/reception address prioritized)
    a.full_address,
    a.street_name,
    a.house_number,
    a.postal_code,
    a.municipality,
    a.province,
    -- Contact information
    cp.telephone,
    cp.email,
    cp.website,
    -- Location
    g.latitude,
    g.longitude,
    -- Registration info
    r.registration_type,
    r.registration_status
FROM logies l
LEFT JOIN logies_addresses la ON l.id = la.logies_id AND la.address_type = 'onthaal_address'
LEFT JOIN addresses a ON la.address_id = a.id
LEFT JOIN logies_contacts lc ON l.id = lc.logies_id
LEFT JOIN contact_points cp ON lc.contact_point_id = cp.id
LEFT JOIN logies_geometries lg ON l.id = lg.logies_id
LEFT JOIN geometries g ON lg.geometry_id = g.id
LEFT JOIN registrations r ON l.id = r.logies_id;

COMMENT ON TABLE logies IS 'Accommodations - primary entity for overnight stays in Flanders tourism data';
COMMENT ON TABLE rental_units IS 'Rental units within accommodations - mandatory 1..* relationship';
COMMENT ON TABLE multilingual_texts IS 'Multilingual text support for TaalString fields';
COMMENT ON VIEW logies_complete IS 'Complete view of accommodations with address, contact and location data';