#!/usr/bin/env python3
"""
FIXED TTL to PostgreSQL importer for Flemish Tourism data
Fixed the critical entity classification bug
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

class FixedTourismDataImporter:
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
            elif 'Point' in rdf_type or 'Geometry' in rdf_type:
                return 'geometry'
            elif 'ContactPoint' in rdf_type:
                return 'contact_point'
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

        logger.warning(f"Could not classify entity: {subject_uri} with types {rdf_types}")
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

                if 'name' in predicate.lower() and 'alternative' not in predicate.lower():
                    if not logies_data['name']:  # Take first name
                        logies_data['name'] = self.parse_multilingual_text(value)[0]
                elif 'alternativeName' in predicate or 'altLabel' in predicate:
                    if not logies_data['alternative_name']:
                        logies_data['alternative_name'] = self.parse_multilingual_text(value)[0]
                elif 'description' in predicate or 'comment' in predicate:
                    if not logies_data['description']:
                        logies_data['description'] = self.parse_multilingual_text(value)[0]
                elif 'aantalSlaapplaatsen' in predicate:
                    try:
                        # FIXED: Handle XML Schema datatype format "4"^^<type>
                        numeric_value = value.split('^^')[0].strip('"')
                        logies_data['sleeping_places'] = int(numeric_value)
                    except (ValueError, IndexError):
                        pass
                elif 'aantalVerhuureenheden' in predicate:
                    try:
                        # FIXED: Handle XML Schema datatype format "1"^^<type>
                        numeric_value = value.split('^^')[0].strip('"')
                        logies_data['rental_units_count'] = int(numeric_value)
                    except (ValueError, IndexError):
                        pass
                # Process relationships
                elif 'address' in predicate.lower() or 'onthaalAdres' in predicate:
                    address_id = self.extract_uuid_from_uri(clean_value)
                    self.logies_addresses.append({'logies_id': entity_id, 'address_id': address_id})
                elif 'contactPoint' in predicate:
                    contact_id = self.extract_uuid_from_uri(clean_value)
                    self.logies_contacts.append({'logies_id': entity_id, 'contact_id': contact_id})
                elif 'location' in predicate or 'onthaalLocatie' in predicate:
                    geometry_id = self.extract_uuid_from_uri(clean_value)
                    self.logies_geometries.append({'logies_id': entity_id, 'geometry_id': geometry_id})
                elif 'heeftKwaliteitslabel' in predicate:
                    quality_label_id = self.extract_uuid_from_uri(clean_value)
                    self.logies_quality_labels.append({'logies_id': entity_id, 'quality_label_id': quality_label_id})
                elif 'behoortTotToeristischeRegio' in predicate:
                    region_id = self.extract_uuid_from_uri(clean_value)
                    self.logies_regions.append({'logies_id': entity_id, 'region_id': region_id})

        # Only save if we have at least a name
        if logies_data['name']:
            self.logies[entity_id] = logies_data
        else:
            logger.warning(f"Logies {entity_id} has no name, skipping")

    def process_tourist_attraction(self, entity_id: str, subject_uri: str, properties: Dict[str, List[str]]):
        """Process TouristAttraction entity"""
        attraction_data = {
            'id': entity_id,
            'uri': subject_uri,
            'name': None,
            'alternative_name': None,
            'description': None,
            'category': None
        }

        for predicate, values in properties.items():
            for value in values:
                clean_value = value.strip('"<>')

                if 'name' in predicate.lower() and 'alternative' not in predicate.lower():
                    if not attraction_data['name']:
                        attraction_data['name'] = self.parse_multilingual_text(value)[0]
                elif 'alternativeName' in predicate or 'altLabel' in predicate:
                    if not attraction_data['alternative_name']:
                        attraction_data['alternative_name'] = self.parse_multilingual_text(value)[0]
                elif 'description' in predicate or 'comment' in predicate:
                    if not attraction_data['description']:
                        attraction_data['description'] = self.parse_multilingual_text(value)[0]
                # Process relationships
                elif 'address' in predicate.lower():
                    address_id = self.extract_uuid_from_uri(clean_value)
                    self.attraction_addresses.append({'attraction_id': entity_id, 'address_id': address_id})
                elif 'contactPoint' in predicate:
                    contact_id = self.extract_uuid_from_uri(clean_value)
                    self.attraction_contacts.append({'attraction_id': entity_id, 'contact_id': contact_id})
                elif 'location' in predicate:
                    geometry_id = self.extract_uuid_from_uri(clean_value)
                    self.attraction_geometries.append({'attraction_id': entity_id, 'geometry_id': geometry_id})
                elif 'behoortTotToeristischeRegio' in predicate:
                    region_id = self.extract_uuid_from_uri(clean_value)
                    self.attraction_regions.append({'attraction_id': entity_id, 'region_id': region_id})

        # Only save if we have at least a name
        if attraction_data['name']:
            self.tourist_attractions[entity_id] = attraction_data
        else:
            logger.warning(f"TouristAttraction {entity_id} has no name, skipping")

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

                if 'telephone' in predicate.lower() or 'phone' in predicate.lower():
                    if not contact_data['telephone']:
                        contact_data['telephone'] = clean_value
                elif 'email' in predicate.lower():
                    if not contact_data['email']:
                        contact_data['email'] = clean_value
                elif 'website' in predicate.lower() or 'url' in predicate.lower():
                    if not contact_data['website']:
                        contact_data['website'] = clean_value
                elif 'fax' in predicate.lower():
                    if not contact_data['fax']:
                        contact_data['fax'] = clean_value

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

                if 'lat' in predicate.lower() and 'latitude' not in predicate.lower():
                    try:
                        geometry_data['latitude'] = float(clean_value)
                    except ValueError:
                        pass
                elif 'long' in predicate.lower() or 'lng' in predicate.lower():
                    try:
                        geometry_data['longitude'] = float(clean_value)
                    except ValueError:
                        pass
                elif 'asWKT' in predicate:
                    geometry_data['wkt_geometry'] = clean_value
                elif 'asGML' in predicate:
                    geometry_data['gml_geometry'] = clean_value
                elif 'Point' in value:
                    geometry_data['geometry_type'] = 'Point'

        self.geometries[entity_id] = geometry_data

    def process_entity(self, subject_uri: str, properties: Dict[str, List[str]]):
        """Process a single entity and its properties"""
        entity_id = self.extract_uuid_from_uri(subject_uri)

        # Extract RDF types
        rdf_types = []
        if 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' in properties:
            rdf_types = [obj.strip('<>') for obj in properties['http://www.w3.org/1999/02/22-rdf-syntax-ns#type']]

        entity_type = self.detect_entity_type(subject_uri, rdf_types)

        # Process based on entity type
        if entity_type == 'logies':
            self.process_logies(entity_id, subject_uri, properties)
        elif entity_type == 'tourist_attraction':
            self.process_tourist_attraction(entity_id, subject_uri, properties)
        elif entity_type == 'address':
            self.process_address(entity_id, subject_uri, properties)
        elif entity_type == 'contact_point':
            self.process_contact_point(entity_id, subject_uri, properties)
        elif entity_type == 'geometry':
            self.process_geometry(entity_id, subject_uri, properties)
        # Add other entity types as needed
        else:
            logger.debug(f"Skipping entity type: {entity_type}")

    def parse_ttl_file(self, file_path: str):
        """Parse TTL file and extract entities - FIXED to handle interleaved entities"""
        logger.info(f"Starting to parse {file_path}")

        # FIXED: Collect ALL triples first, then process entities
        all_entity_properties = {}

        try:
            # First pass: Collect all triples for all entities
            logger.info("First pass: Collecting all triples...")
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

                            # Add to entity properties
                            if subject not in all_entity_properties:
                                all_entity_properties[subject] = {}

                            if predicate not in all_entity_properties[subject]:
                                all_entity_properties[subject][predicate] = []
                            all_entity_properties[subject][predicate].append(obj_part)

                    if line_num % 100000 == 0:
                        logger.info(f"Processed {line_num:,} lines...")

            logger.info(f"First pass complete. Found {len(all_entity_properties):,} entities")

            # Second pass: Process all entities with complete property sets
            logger.info("Second pass: Processing entities...")
            processed_count = 0
            for subject_uri, properties in all_entity_properties.items():
                self.process_entity(subject_uri, properties)
                processed_count += 1

                if processed_count % 10000 == 0:
                    logger.info(f"Processed {processed_count:,} entities...")

            logger.info(f"TTL parsing completed. Processed {processed_count:,} entities")

        except Exception as e:
            logger.error(f"Error parsing TTL file: {e}")
            raise

    def save_to_database(self):
        """Save all entities to database"""
        logger.info("Starting database import")

        try:
            # Save entities
            self.save_logies()
            self.save_tourist_attractions()
            self.save_addresses()
            self.save_contact_points()
            self.save_geometries()

            # Save relationships
            self.save_logies_relationships()
            self.save_attraction_relationships()

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

    def save_tourist_attractions(self):
        """Save TouristAttraction entities to database"""
        if not self.tourist_attractions:
            logger.info("No tourist attraction entities to save")
            return

        logger.info(f"Saving {len(self.tourist_attractions)} tourist attraction entities")

        for attraction_data in self.tourist_attractions.values():
            self.cursor.execute("""
                INSERT INTO tourist_attractions (id, uri, name, alternative_name, description, category)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    uri = EXCLUDED.uri,
                    name = EXCLUDED.name,
                    alternative_name = EXCLUDED.alternative_name,
                    description = EXCLUDED.description,
                    category = EXCLUDED.category
            """, (
                attraction_data['id'], attraction_data['uri'], attraction_data['name'],
                attraction_data['alternative_name'], attraction_data['description'],
                attraction_data['category']
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

    def save_logies_relationships(self):
        """Save Logies relationship tables"""
        # Save logies_addresses
        if self.logies_addresses:
            logger.info(f"Saving {len(self.logies_addresses)} logies-address relationships")
            for rel in self.logies_addresses:
                self.cursor.execute("""
                    INSERT INTO logies_addresses (logies_id, address_id)
                    VALUES (%s, %s)
                    ON CONFLICT (logies_id, address_id) DO NOTHING
                """, (rel['logies_id'], rel['address_id']))

        # Save logies_contacts
        if self.logies_contacts:
            logger.info(f"Saving {len(self.logies_contacts)} logies-contact relationships")
            for rel in self.logies_contacts:
                self.cursor.execute("""
                    INSERT INTO logies_contacts (logies_id, contact_id)
                    VALUES (%s, %s)
                    ON CONFLICT (logies_id, contact_id) DO NOTHING
                """, (rel['logies_id'], rel['contact_id']))

        # Save logies_geometries
        if self.logies_geometries:
            logger.info(f"Saving {len(self.logies_geometries)} logies-geometry relationships")
            for rel in self.logies_geometries:
                self.cursor.execute("""
                    INSERT INTO logies_geometries (logies_id, geometry_id)
                    VALUES (%s, %s)
                    ON CONFLICT (logies_id, geometry_id) DO NOTHING
                """, (rel['logies_id'], rel['geometry_id']))

        # Save logies_quality_labels
        if self.logies_quality_labels:
            logger.info(f"Saving {len(self.logies_quality_labels)} logies-quality label relationships")
            for rel in self.logies_quality_labels:
                self.cursor.execute("""
                    INSERT INTO logies_quality_labels (logies_id, quality_label_id)
                    VALUES (%s, %s)
                    ON CONFLICT (logies_id, quality_label_id) DO NOTHING
                """, (rel['logies_id'], rel['quality_label_id']))

        # Save logies_regions
        if self.logies_regions:
            logger.info(f"Saving {len(self.logies_regions)} logies-region relationships")
            for rel in self.logies_regions:
                self.cursor.execute("""
                    INSERT INTO logies_regions (logies_id, region_id)
                    VALUES (%s, %s)
                    ON CONFLICT (logies_id, region_id) DO NOTHING
                """, (rel['logies_id'], rel['region_id']))

    def save_attraction_relationships(self):
        """Save TouristAttraction relationship tables"""
        # Save attraction_addresses
        if self.attraction_addresses:
            logger.info(f"Saving {len(self.attraction_addresses)} attraction-address relationships")
            for rel in self.attraction_addresses:
                self.cursor.execute("""
                    INSERT INTO attraction_addresses (attraction_id, address_id)
                    VALUES (%s, %s)
                    ON CONFLICT (attraction_id, address_id) DO NOTHING
                """, (rel['attraction_id'], rel['address_id']))

        # Save attraction_contacts
        if self.attraction_contacts:
            logger.info(f"Saving {len(self.attraction_contacts)} attraction-contact relationships")
            for rel in self.attraction_contacts:
                self.cursor.execute("""
                    INSERT INTO attraction_contacts (attraction_id, contact_id)
                    VALUES (%s, %s)
                    ON CONFLICT (attraction_id, contact_id) DO NOTHING
                """, (rel['attraction_id'], rel['contact_id']))

        # Save attraction_geometries
        if self.attraction_geometries:
            logger.info(f"Saving {len(self.attraction_geometries)} attraction-geometry relationships")
            for rel in self.attraction_geometries:
                self.cursor.execute("""
                    INSERT INTO attraction_geometries (attraction_id, geometry_id)
                    VALUES (%s, %s)
                    ON CONFLICT (attraction_id, geometry_id) DO NOTHING
                """, (rel['attraction_id'], rel['geometry_id']))

        # Save attraction_regions
        if self.attraction_regions:
            logger.info(f"Saving {len(self.attraction_regions)} attraction-region relationships")
            for rel in self.attraction_regions:
                self.cursor.execute("""
                    INSERT INTO attraction_regions (attraction_id, region_id)
                    VALUES (%s, %s)
                    ON CONFLICT (attraction_id, region_id) DO NOTHING
                """, (rel['attraction_id'], rel['region_id']))


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Import Flemish Tourism TTL data into PostgreSQL (FIXED Schema)')
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

    importer = FixedTourismDataImporter(db_config)

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