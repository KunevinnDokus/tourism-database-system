#!/usr/bin/env python3
"""
Trace parser flow to find where sleeping_places and rental_units_count are being lost
"""

import re
from typing import Dict, List

def trace_entity_processing():
    """Trace how a specific entity gets processed through the FIXED parser logic"""

    print("🔍 TRACE: Following entity processing in FIXED parser")
    print("=" * 70)

    # Test the exact same entity
    test_entity = "b3bb7490-e37c-11ed-b6ca-e5a176a460b1"
    test_uri = f"http://linked.toerismevlaanderen.be/id/tourist-attractions/{test_entity}"

    # Simulate parsing this entity
    current_subject = None
    current_properties = {}
    found_test_entity = False

    with open('toeristische-attracties.ttl', 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            # Parse triple pattern (same logic as FIXED parser)
            if line.startswith('<') and '>' in line:
                parts = line.split(None, 2)  # Split into max 3 parts
                if len(parts) >= 3:
                    subject = parts[0].strip('<>')
                    predicate = parts[1].strip('<>')
                    obj_part = parts[2].rstrip(' .')

                    # Process current subject if we encounter a new one
                    if subject != current_subject:
                        if current_subject and current_subject == test_uri:
                            # Process our test entity
                            print(f"\n🎯 PROCESSING TEST ENTITY: {test_entity}")
                            print(f"    Triggered by encountering new subject: {subject}")
                            process_test_entity(current_subject, current_properties)
                            found_test_entity = True
                            break

                        current_subject = subject
                        current_properties = {}

                    # Add to current properties
                    if current_subject == test_uri:
                        print(f"    📝 Line {line_num}: {predicate} -> {obj_part}")

                    if predicate not in current_properties:
                        current_properties[predicate] = []
                    current_properties[predicate].append(obj_part)

            # Stop after reasonable number of lines
            if line_num > 100000:
                break

    if not found_test_entity and current_subject == test_uri:
        print(f"\n🎯 PROCESSING TEST ENTITY (end of file): {test_entity}")
        process_test_entity(current_subject, current_properties)

def extract_uuid_from_uri(uri: str) -> str:
    """Same UUID extraction logic as FIXED parser"""
    uuid_patterns = [
        r'/([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12})',
        r'#([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12})'
    ]

    for pattern in uuid_patterns:
        match = re.search(pattern, uri)
        if match:
            return match.group(1)

    return str(uuid.uuid4())

def detect_entity_type(subject_uri: str, rdf_types: List[str]) -> str:
    """Same entity type detection as FIXED parser"""
    print(f"  🔍 Detecting entity type for {subject_uri}")
    print(f"  📋 RDF types: {rdf_types}")

    # Check RDF types with proper priority
    for rdf_type in rdf_types:
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

    # Handle TouristAttraction vs Logies classification
    has_tourist_attraction = any('TouristAttraction' in rdf_type for rdf_type in rdf_types)
    has_logies = any('Logies' in rdf_type or 'logies' in rdf_type for rdf_type in rdf_types)

    print(f"  🏛️ Has TouristAttraction: {has_tourist_attraction}")
    print(f"  🏨 Has Logies: {has_logies}")

    if has_tourist_attraction and has_logies:
        if '/tourist-attractions/' in subject_uri:
            print(f"  ✅ Mixed entity in tourist-attractions URI -> classifying as logies")
            return 'logies'
        else:
            return 'tourist_attraction'
    elif has_logies:
        print(f"  ✅ Pure logies entity")
        return 'logies'
    elif has_tourist_attraction:
        print(f"  ✅ Pure tourist attraction entity")
        return 'tourist_attraction'

    print(f"  ❌ Could not classify entity")
    return 'unknown'

def parse_multilingual_text(text_value: str):
    """Same multilingual parsing as FIXED parser"""
    match = re.match(r'"(.+)"@([a-z]{2})', text_value)
    if match:
        return match.group(1), match.group(2)
    else:
        clean_text = text_value.strip('"')
        return clean_text, 'nl'

def process_test_entity(subject_uri: str, properties: Dict[str, List[str]]):
    """Trace how the test entity gets processed"""
    entity_id = extract_uuid_from_uri(subject_uri)
    print(f"  🆔 Extracted entity ID: {entity_id}")

    print(f"  📋 ALL COLLECTED PROPERTIES:")
    for predicate, values in properties.items():
        if 'aantalSlaapplaatsen' in predicate or 'aantalVerhuureenheden' in predicate:
            print(f"    🎯 {predicate}: {values}")
        else:
            print(f"    📝 {predicate}: {values[:1]}...")  # Just show first value

    # Extract RDF types
    rdf_types = []
    if 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' in properties:
        rdf_types = [obj.strip('<>') for obj in properties['http://www.w3.org/1999/02/22-rdf-syntax-ns#type']]

    entity_type = detect_entity_type(subject_uri, rdf_types)
    print(f"  🏷️ Detected entity type: {entity_type}")

    if entity_type == 'logies':
        print(f"  🏨 Processing as LOGIES entity")
        process_logies_trace(entity_id, subject_uri, properties)
    else:
        print(f"  ❌ NOT processing as logies (type: {entity_type})")
        print(f"  🚨 THIS IS THE PROBLEM! Entity should be logies but is classified as {entity_type}")

def process_logies_trace(entity_id: str, subject_uri: str, properties: Dict[str, List[str]]):
    """Trace logies processing with same logic as FIXED parser"""
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

    print(f"    🏗️ Created logies_data structure")

    for predicate, values in properties.items():
        for value in values:
            clean_value = value.strip('"<>')

            if 'name' in predicate.lower():
                if not logies_data['name']:
                    logies_data['name'] = parse_multilingual_text(value)[0]
                    print(f"    📝 Set name: {logies_data['name']}")
            elif 'aantalSlaapplaatsen' in predicate:
                print(f"    🛏️ Found sleeping places predicate: {predicate}")
                print(f"    🛏️ Raw value: {value}")
                print(f"    🛏️ Clean value: {clean_value}")
                try:
                    logies_data['sleeping_places'] = int(clean_value)
                    print(f"    ✅ Set sleeping_places: {logies_data['sleeping_places']}")
                except ValueError as e:
                    print(f"    ❌ Failed to parse sleeping_places: {e}")
            elif 'aantalVerhuureenheden' in predicate:
                print(f"    🏠 Found rental units predicate: {predicate}")
                print(f"    🏠 Raw value: {value}")
                print(f"    🏠 Clean value: {clean_value}")
                try:
                    logies_data['rental_units_count'] = int(clean_value)
                    print(f"    ✅ Set rental_units_count: {logies_data['rental_units_count']}")
                except ValueError as e:
                    print(f"    ❌ Failed to parse rental_units_count: {e}")

    print(f"\n    📊 FINAL LOGIES DATA:")
    for key, value in logies_data.items():
        print(f"      {key}: {value}")

    # Check if it would be saved
    if logies_data['name']:
        print(f"    ✅ Entity would be SAVED (has name)")
    else:
        print(f"    ❌ Entity would be SKIPPED (no name)")

if __name__ == "__main__":
    trace_entity_processing()