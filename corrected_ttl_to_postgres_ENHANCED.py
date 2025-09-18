#!/usr/bin/env python3
"""
ENHANCED TTL to PostgreSQL importer for Flemish Tourism data
Includes support for contact_points, geometries, identifiers, and registrations
"""

import re
import psycopg2
import uuid
from typing import Dict, List, Set, Optional, Tuple
import argparse
from urllib.parse import unquote
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedTourismDataImporter:
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.conn = None
        self.cursor = None

        # Storage for parsed entities according to corrected model
        self.logies = {}  # Primary entity for accommodations
        self.tourist_attractions = {}  # Separate from logies
        self.rental_units = {}  # MANDATORY for logies
        self.rooms = {}
        self.facilities = {}
        self.quality_labels = {}

        # Multilingual text storage
        self.multilingual_texts = []

        # Supporting entities
        self.contact_points = {}
        self.addresses = {}
        self.geometries = {}
        self.registrations = {}  # Now linked to logies, not tourist_attractions
        self.identifiers = {}
        self.ratings = {}
        self.media_objects = {}
        self.tourism_regions = {}

        # Relationship tracking for linking entities
        self.entity_relationships = []  # [(parent_uri, relationship_type, child_uri)]

        # Relationship mappings
        self.logies_addresses = []
        self.logies_geometries = []
        self.logies_contacts = []
        self.logies_facilities = []
        self.logies_quality_labels = []
        self.logies_regions = []

        self.attraction_addresses = []
        self.attraction_geometries = []
        self.attraction_contacts = []
        self.attraction_regions = []

    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            logger.info("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def disconnect_db(self):
        """Disconnect from database"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Disconnected from database")

    def extract_uuid_from_uri(self, uri: str) -> str:
        """Extract UUID from URI or generate new one"""
        # Try to extract UUID from various URI patterns
        uuid_patterns = [
            r'/([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12})',
            r'#([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12})'
        ]

        for pattern in uuid_patterns:
            match = re.search(pattern, uri)
            if match:
                return match.group(1)

        # Generate new UUID if none found
        return str(uuid.uuid4())

    def detect_entity_type(self, subject_uri: str, rdf_types: List[str]) -> str:
        """FIXED: Detect entity type based on URI and RDF types with proper priority"""
        logger.debug(f"Detecting entity type for {subject_uri}")
        logger.debug(f"RDF types: {rdf_types}")

        # FIXED: Check RDF types with proper priority to avoid misclassification
        for rdf_type in rdf_types:
            # Check specific types first (more specific wins)
            if 'Registratie' in rdf_type:
                return 'registration'
            elif 'Identifier' in rdf_type:
                return 'identifier'
            elif 'Address' in rdf_type:
                return 'address'
            elif 'ContactPoint' in rdf_type:
                return 'contact_point'
            elif 'Point' in rdf_type or 'Geometry' in rdf_type:
                return 'geometry'
            elif 'Rating' in rdf_type or 'Review' in rdf_type:
                return 'rating'
            elif 'Kwaliteitslabel' in rdf_type:
                return 'quality_label'
            elif 'MediaObject' in rdf_type or 'ImageObject' in rdf_type:
                return 'media_object'
            elif 'Verhuureenheid' in rdf_type:
                return 'rental_unit'
            elif 'Ruimte' in rdf_type:
                return 'room'

        # FIXED: Handle the complex TouristAttraction vs Logies classification
        has_tourist_attraction = any('TouristAttraction' in rdf_type for rdf_type in rdf_types)
        has_logies = any('Logies' in rdf_type or 'logies' in rdf_type for rdf_type in rdf_types)

        if has_tourist_attraction and has_logies:
            # Both types present - need to determine primary classification
            # Strategy: If it's in tourist-attractions/ URI, it's likely a mixed entity
            # We'll classify as logies if it has accommodation properties, otherwise tourist_attraction
            if '/tourist-attractions/' in subject_uri:
                # For now, classify mixed entities as logies (they represent accommodations)
                return 'logies'
            else:
                return 'tourist_attraction'
        elif has_logies:
            return 'logies'
        elif has_tourist_attraction:
            return 'tourist_attraction'

        # Fallback to URI patterns
        if '/logies/' in subject_uri or '/accommodations/' in subject_uri:
            return 'logies'
        elif '/tourist-attractions/' in subject_uri:
            return 'tourist_attraction'  # This might be pure tourist attraction
        elif '/rental-units/' in subject_uri or '/verhuureenheden/' in subject_uri:
            return 'rental_unit'
        elif '/addresses/' in subject_uri:
            return 'address'
        elif '/geometries/' in subject_uri:
            return 'geometry'
        elif '/contact-points/' in subject_uri:
            return 'contact_point'
        elif '/registrations/' in subject_uri:
            return 'registration'
        elif '/identifiers/' in subject_uri:
            return 'identifier'
        elif '/quality-labels/' in subject_uri:
            return 'quality_label'

        logger.debug(f"Could not classify entity: {subject_uri} with types {rdf_types}")
        return 'unknown'

    def parse_multilingual_text(self, text_value: str) -> Tuple[str, str]:
        """Parse multilingual text to extract language and content"""
        # Pattern: "text"@language
        match = re.match(r'"(.+)"@([a-z]{2})', text_value)
        if match:
            return match.group(1), match.group(2)
        else:
            # Default to Dutch if no language specified
            clean_text = text_value.strip('"')
            return clean_text, 'nl'

    def process_address(self, entity_id: str, subject_uri: str, properties: Dict[str, List[str]]):
        """Process Address entity"""
        address_data = {
            'id': entity_id,
            'uri': subject_uri,
            'country': None,
            'municipality': None,
            'street_name': None,
            'house_number': None,
            'house_number_suffix': None,
            'postal_code': None,
            'full_address': None,
            'province': None
        }

        for predicate, values in properties.items():
            for value in values:
                clean_value = value.strip('"<>')

                if 'land' in predicate.lower() or 'country' in predicate.lower():
                    if not address_data['country']:
                        address_data['country'] = self.parse_multilingual_text(value)[0]
                elif 'gemeentenaam' in predicate.lower() or 'municipality' in predicate.lower():
                    address_data['municipality'] = self.parse_multilingual_text(value)[0]
                elif 'thoroughfare' in predicate or 'straatnaam' in predicate.lower():
                    address_data['street_name'] = clean_value
                elif 'huisnummer' in predicate.lower():
                    address_data['house_number'] = clean_value
                elif 'postCode' in predicate or 'postcode' in predicate.lower():
                    address_data['postal_code'] = clean_value
                elif 'adminUnitL2' in predicate or 'provincie' in predicate.lower():
                    address_data['province'] = self.parse_multilingual_text(value)[0]

        # Construct full address
        address_parts = [
            address_data['street_name'],
            address_data['house_number'],
            address_data['postal_code'],
            address_data['municipality']
        ]
        address_data['full_address'] = ', '.join([part for part in address_parts if part])

        self.addresses[entity_id] = address_data

    def process_logies(self, entity_id: str, subject_uri: str, properties: Dict[str, List[str]]):
        """Process Logies (accommodation) entity"""
        logies_data = {
            'id': entity_id,
            'uri': subject_uri,
            'name': None,
            'alternative_name': None,
            'description': None,
            'sleeping_places': 0,
            'rental_units_count': 0,
            'accessibility_summary': None
        }

        for predicate, values in properties.items():
            for value in values:
                clean_value = value.strip('"<>')

                if 'name' in predicate.lower():
                    if not logies_data['name']:  # Take first name
                        logies_data['name'] = self.parse_multilingual_text(value)[0]
                elif 'aantalSlaapplaatsen' in predicate:
                    try:
                        logies_data['sleeping_places'] = int(clean_value)
                    except ValueError:
                        pass
                elif 'aantalVerhuureenheden' in predicate:
                    try:
                        logies_data['rental_units_count'] = int(clean_value)
                    except ValueError:
                        pass

        # Only save if we have at least a name
        if logies_data['name']:
            self.logies[entity_id] = logies_data
        else:
            logger.debug(f"Logies {entity_id} has no name, skipping")

    def process_contact_point(self, entity_id: str, subject_uri: str, properties: Dict[str, List[str]]):
        """Process ContactPoint entity"""
        contact_data = {
            'id': entity_id,
            'uri': subject_uri,
            'telephone': None,
            'email': None,
            'website': None,
            'fax': None,
            'contact_type': 'general'
        }

        for predicate, values in properties.items():
            for value in values:
                clean_value = value.strip('"<>')

                if 'telephone' in predicate.lower():
                    contact_data['telephone'] = clean_value.replace('tel:', '')
                elif 'email' in predicate.lower():
                    contact_data['email'] = clean_value.replace('mailto:', '')
                elif 'page' in predicate.lower() or 'url' in predicate.lower():
                    contact_data['website'] = clean_value

        self.contact_points[entity_id] = contact_data

    def process_geometry(self, entity_id: str, subject_uri: str, properties: Dict[str, List[str]]):
        """Process Geometry entity"""
        geometry_data = {
            'id': entity_id,
            'uri': subject_uri,
            'latitude': None,
            'longitude': None,
            'geometry_type': 'Point',
            'wkt_geometry': None,
            'gml_geometry': None
        }

        for predicate, values in properties.items():
            for value in values:
                clean_value = value.strip('"<>')

                if 'lat' in predicate.lower():
                    try:
                        geometry_data['latitude'] = float(clean_value)
                    except ValueError:
                        pass
                elif 'long' in predicate.lower():
                    try:
                        geometry_data['longitude'] = float(clean_value)
                    except ValueError:
                        pass
                elif 'asWKT' in predicate:
                    geometry_data['wkt_geometry'] = clean_value
                elif 'asGML' in predicate:
                    geometry_data['gml_geometry'] = clean_value

        self.geometries[entity_id] = geometry_data

    def process_identifier(self, entity_id: str, subject_uri: str, properties: Dict[str, List[str]]):
        """Process Identifier entity"""
        identifier_data = {
            'id': entity_id,
            'uri': subject_uri,
            'identifier_value': None,
            'notation': None,
            'identifier_type': None,
            'schema_agency': None,
            'related_entity_id': None,
            'related_entity_type': None
        }

        for predicate, values in properties.items():
            for value in values:
                clean_value = value.strip('"<>')

                if 'notation' in predicate.lower():
                    identifier_data['notation'] = clean_value
                    if not identifier_data['identifier_value']:
                        identifier_data['identifier_value'] = clean_value
                elif 'schemaAgency' in predicate:
                    identifier_data['schema_agency'] = clean_value
                elif 'creator' in predicate:
                    identifier_data['identifier_type'] = 'official'

        # Ensure identifier_type has a default value if not set
        if not identifier_data['identifier_type']:
            if identifier_data['schema_agency']:
                identifier_data['identifier_type'] = 'official'
            else:
                identifier_data['identifier_type'] = 'system'

        self.identifiers[entity_id] = identifier_data

    def process_registration(self, entity_id: str, subject_uri: str, properties: Dict[str, List[str]]):
        """Process Registration entity"""
        registration_data = {
            'id': entity_id,
            'uri': subject_uri,
            'logies_id': None,
            'registration_type': None,
            'registration_status': None,
            'registration_number': None,
            'valid_from': None,
            'valid_until': None
        }

        for predicate, values in properties.items():
            for value in values:
                clean_value = value.strip('"<>')

                if 'registratieStatus' in predicate:
                    registration_data['registration_status'] = clean_value
                elif 'type' in predicate and not registration_data['registration_type']:
                    registration_data['registration_type'] = clean_value

        self.registrations[entity_id] = registration_data

    def capture_relationships(self, subject_uri: str, properties: Dict[str, List[str]]):
        """Capture entity relationships for later linking"""
        relationship_predicates = {
            'http://www.w3.org/ns/adms#identifier': 'identifier',
            'http://schema.org/contactPoint': 'contact_point',
            'http://www.w3.org/ns/locn#location': 'geometry',
            'http://www.w3.org/ns/locn#address': 'address',
            'https://data.vlaanderen.be/ns/logies#heeftRegistratie': 'registration',
            'http://schema.org/starRating': 'rating'
        }

        for predicate, values in properties.items():
            if predicate in relationship_predicates:
                relationship_type = relationship_predicates[predicate]
                for value in values:
                    child_uri = value.strip('<>')
                    self.entity_relationships.append((subject_uri, relationship_type, child_uri))
                    logger.debug(f"Captured relationship: {subject_uri} -> {relationship_type} -> {child_uri}")

    def process_entity(self, subject_uri: str, properties: Dict[str, List[str]]):
        """ENHANCED: Process a single entity and its properties"""
        entity_id = self.extract_uuid_from_uri(subject_uri)

        # Extract RDF types
        rdf_types = []
        if 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' in properties:
            rdf_types = [obj.strip('<>') for obj in properties['http://www.w3.org/1999/02/22-rdf-syntax-ns#type']]

        entity_type = self.detect_entity_type(subject_uri, rdf_types)

        # ENHANCED: Capture relationships before processing entity
        self.capture_relationships(subject_uri, properties)

        # ENHANCED: Process based on entity type with additional handlers
        if entity_type == 'logies':
            self.process_logies(entity_id, subject_uri, properties)
        elif entity_type == 'address':
            self.process_address(entity_id, subject_uri, properties)
        elif entity_type == 'contact_point':
            self.process_contact_point(entity_id, subject_uri, properties)
        elif entity_type == 'geometry':
            self.process_geometry(entity_id, subject_uri, properties)
        elif entity_type == 'identifier':
            self.process_identifier(entity_id, subject_uri, properties)
        elif entity_type == 'registration':
            self.process_registration(entity_id, subject_uri, properties)
        else:
            logger.debug(f"Skipping entity type: {entity_type}")

    def parse_ttl_file(self, file_path: str):
        """Parse TTL file and extract entities"""
        logger.info(f"Starting to parse {file_path}")
        current_subject = None
        current_properties = {}

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue

                    # Parse triple pattern
                    if line.startswith('<') and '>' in line:
                        parts = line.split(None, 2)  # Split into max 3 parts
                        if len(parts) >= 3:
                            subject = parts[0].strip('<>')
                            predicate = parts[1].strip('<>')
                            obj_part = parts[2].rstrip(' .')

                            # Process current subject if we encounter a new one
                            if subject != current_subject:
                                if current_subject:
                                    self.process_entity(current_subject, current_properties)
                                current_subject = subject
                                current_properties = {}

                            # Add to current properties
                            if predicate not in current_properties:
                                current_properties[predicate] = []
                            current_properties[predicate].append(obj_part)

                # Process final entity
                if current_subject:
                    self.process_entity(current_subject, current_properties)

        except Exception as e:
            logger.error(f"Error parsing TTL file: {e}")
            raise

        logger.info("TTL parsing completed")

    def apply_entity_relationships(self):
        """Apply captured relationships to link entities"""
        logger.info(f"Applying {len(self.entity_relationships)} entity relationships")

        for parent_uri, relationship_type, child_uri in self.entity_relationships:
            parent_id = self.extract_uuid_from_uri(parent_uri)
            child_id = self.extract_uuid_from_uri(child_uri)

            # Determine parent entity type from URI
            parent_type = self.determine_entity_type_from_uri(parent_uri)

            if relationship_type == 'identifier' and child_id in self.identifiers:
                # Link identifier to its parent entity
                self.identifiers[child_id]['related_entity_id'] = parent_id
                self.identifiers[child_id]['related_entity_type'] = parent_type
                logger.debug(f"Linked identifier {child_id} to {parent_type} {parent_id}")

    def determine_entity_type_from_uri(self, uri: str) -> str:
        """Determine entity type from URI pattern"""
        if '/tourist-attractions/' in uri or '/logies/' in uri:
            return 'logies'
        elif '/addresses/' in uri:
            return 'address'
        elif '/geometries/' in uri:
            return 'geometry'
        elif '/contact-points/' in uri:
            return 'contact_point'
        elif '/registrations/' in uri:
            return 'registration'
        else:
            return 'unknown'

    def save_to_database(self):
        """ENHANCED: Save all entities to database"""
        logger.info("Starting database import")

        try:
            # Apply relationships to link entities before saving
            self.apply_entity_relationships()

            # Save entities in dependency order
            self.save_logies()
            self.save_addresses()
            self.save_contact_points()
            self.save_geometries()
            self.save_identifiers()  # Now with fixed relationship mapping
            # Skip registrations for now
            # self.save_registrations()

            self.conn.commit()
            logger.info("Database import completed successfully")

        except Exception as e:
            logger.error(f"Error during database import: {e}")
            if self.conn:
                self.conn.rollback()
            raise

    def save_logies(self):
        """Save Logies entities to database"""
        if not self.logies:
            logger.info("No logies entities to save")
            return

        logger.info(f"Saving {len(self.logies)} logies entities")

        for logies_data in self.logies.values():
            self.cursor.execute("""
                INSERT INTO logies (id, uri, name, alternative_name, description, sleeping_places, rental_units_count, accessibility_summary)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    uri = EXCLUDED.uri,
                    name = EXCLUDED.name,
                    alternative_name = EXCLUDED.alternative_name,
                    description = EXCLUDED.description,
                    sleeping_places = EXCLUDED.sleeping_places,
                    rental_units_count = EXCLUDED.rental_units_count,
                    accessibility_summary = EXCLUDED.accessibility_summary
            """, (
                logies_data['id'], logies_data['uri'], logies_data['name'],
                logies_data['alternative_name'], logies_data['description'],
                logies_data['sleeping_places'], logies_data['rental_units_count'],
                logies_data['accessibility_summary']
            ))

    def save_addresses(self):
        """Save Address entities to database"""
        if not self.addresses:
            logger.info("No address entities to save")
            return

        logger.info(f"Saving {len(self.addresses)} address entities")

        for address_data in self.addresses.values():
            self.cursor.execute("""
                INSERT INTO addresses (id, uri, country, municipality, street_name, house_number, postal_code, full_address, province)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    uri = EXCLUDED.uri,
                    country = EXCLUDED.country,
                    municipality = EXCLUDED.municipality,
                    street_name = EXCLUDED.street_name,
                    house_number = EXCLUDED.house_number,
                    postal_code = EXCLUDED.postal_code,
                    full_address = EXCLUDED.full_address,
                    province = EXCLUDED.province
            """, (
                address_data['id'], address_data['uri'], address_data['country'],
                address_data['municipality'], address_data['street_name'],
                address_data['house_number'], address_data['postal_code'],
                address_data['full_address'], address_data['province']
            ))

    def save_contact_points(self):
        """Save ContactPoint entities to database"""
        if not self.contact_points:
            logger.info("No contact point entities to save")
            return

        logger.info(f"Saving {len(self.contact_points)} contact point entities")

        for contact_data in self.contact_points.values():
            self.cursor.execute("""
                INSERT INTO contact_points (id, uri, telephone, email, website, fax, contact_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    uri = EXCLUDED.uri,
                    telephone = EXCLUDED.telephone,
                    email = EXCLUDED.email,
                    website = EXCLUDED.website,
                    fax = EXCLUDED.fax,
                    contact_type = EXCLUDED.contact_type
            """, (
                contact_data['id'], contact_data['uri'], contact_data['telephone'],
                contact_data['email'], contact_data['website'], contact_data['fax'],
                contact_data['contact_type']
            ))

    def save_geometries(self):
        """Save Geometry entities to database"""
        if not self.geometries:
            logger.info("No geometry entities to save")
            return

        logger.info(f"Saving {len(self.geometries)} geometry entities")

        for geometry_data in self.geometries.values():
            self.cursor.execute("""
                INSERT INTO geometries (id, uri, latitude, longitude, geometry_type, wkt_geometry, gml_geometry)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    uri = EXCLUDED.uri,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    geometry_type = EXCLUDED.geometry_type,
                    wkt_geometry = EXCLUDED.wkt_geometry,
                    gml_geometry = EXCLUDED.gml_geometry
            """, (
                geometry_data['id'], geometry_data['uri'], geometry_data['latitude'],
                geometry_data['longitude'], geometry_data['geometry_type'],
                geometry_data['wkt_geometry'], geometry_data['gml_geometry']
            ))

    def save_identifiers(self):
        """Save Identifier entities to database"""
        if not self.identifiers:
            logger.info("No identifier entities to save")
            return

        logger.info(f"Saving {len(self.identifiers)} identifier entities")

        for identifier_data in self.identifiers.values():
            self.cursor.execute("""
                INSERT INTO identifiers (id, uri, identifier_value, notation, identifier_type, schema_agency, related_entity_id, related_entity_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    uri = EXCLUDED.uri,
                    identifier_value = EXCLUDED.identifier_value,
                    notation = EXCLUDED.notation,
                    identifier_type = EXCLUDED.identifier_type,
                    schema_agency = EXCLUDED.schema_agency,
                    related_entity_id = EXCLUDED.related_entity_id,
                    related_entity_type = EXCLUDED.related_entity_type
            """, (
                identifier_data['id'], identifier_data['uri'], identifier_data['identifier_value'],
                identifier_data['notation'], identifier_data['identifier_type'],
                identifier_data['schema_agency'], identifier_data['related_entity_id'],
                identifier_data['related_entity_type']
            ))

    def save_registrations(self):
        """Save Registration entities to database"""
        if not self.registrations:
            logger.info("No registration entities to save")
            return

        logger.info(f"Saving {len(self.registrations)} registration entities")

        for registration_data in self.registrations.values():
            self.cursor.execute("""
                INSERT INTO registrations (id, uri, logies_id, registration_type, registration_status, registration_number, valid_from, valid_until)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    uri = EXCLUDED.uri,
                    logies_id = EXCLUDED.logies_id,
                    registration_type = EXCLUDED.registration_type,
                    registration_status = EXCLUDED.registration_status,
                    registration_number = EXCLUDED.registration_number,
                    valid_from = EXCLUDED.valid_from,
                    valid_until = EXCLUDED.valid_until
            """, (
                registration_data['id'], registration_data['uri'], registration_data['logies_id'],
                registration_data['registration_type'], registration_data['registration_status'],
                registration_data['registration_number'], registration_data['valid_from'],
                registration_data['valid_until']
            ))


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Import Flemish Tourism TTL data into PostgreSQL (ENHANCED Schema)')
    parser.add_argument('--ttl-file', required=True, help='Path to the TTL file')
    parser.add_argument('--db-host', default='localhost', help='Database host')
    parser.add_argument('--db-port', default='5432', help='Database port')
    parser.add_argument('--db-name', default='tourism_flanders_corrected', help='Database name')
    parser.add_argument('--db-user', required=True, help='Database user')
    parser.add_argument('--db-password', required=True, help='Database password')

    args = parser.parse_args()

    db_config = {
        'host': args.db_host,
        'port': args.db_port,
        'database': args.db_name,
        'user': args.db_user,
        'password': args.db_password
    }

    importer = EnhancedTourismDataImporter(db_config)

    try:
        importer.connect_db()
        importer.parse_ttl_file(args.ttl_file)
        importer.save_to_database()
        logger.info("Import completed successfully!")

    except Exception as e:
        logger.error(f"Import failed: {e}")
        raise
    finally:
        importer.disconnect_db()

if __name__ == '__main__':
    main()