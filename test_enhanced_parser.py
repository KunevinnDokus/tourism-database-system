#!/usr/bin/env python3
"""
Test the enhanced parser on our test entity to verify all fields are populated
"""

from corrected_ttl_to_postgres_FIXED import FixedTourismDataImporter

def test_enhanced_parser():
    print("🧪 Testing enhanced parser with comprehensive data extraction")
    print("=" * 70)

    # Create importer for test database
    db_config = {
        'host': 'localhost',
        'port': '5432',
        'database': 'tourism_test',
        'user': 'lieven',
        'password': ''
    }

    importer = FixedTourismDataImporter(db_config)

    # Parse just a small subset for testing
    print("Parsing TTL file...")
    importer.parse_ttl_file('toeristische-attracties.ttl')

    print(f"\n📊 PARSING RESULTS:")
    print(f"   Logies entities: {len(importer.logies):,}")
    print(f"   Tourist attractions: {len(importer.tourist_attractions):,}")
    print(f"   Addresses: {len(importer.addresses):,}")
    print(f"   Contact points: {len(importer.contact_points):,}")
    print(f"   Geometries: {len(importer.geometries):,}")

    print(f"\n🔗 RELATIONSHIP COUNTS:")
    print(f"   Logies-Address links: {len(importer.logies_addresses):,}")
    print(f"   Logies-Contact links: {len(importer.logies_contacts):,}")
    print(f"   Logies-Geometry links: {len(importer.logies_geometries):,}")
    print(f"   Logies-Quality labels: {len(importer.logies_quality_labels):,}")
    print(f"   Logies-Regions: {len(importer.logies_regions):,}")

    # Check our test entity
    test_entity_id = "b3bb7490-e37c-11ed-b6ca-e5a176a460b1"

    if test_entity_id in importer.logies:
        logies = importer.logies[test_entity_id]
        print(f"\n🎯 TEST ENTITY RESULTS ({test_entity_id}):")
        print(f"   Name: {logies['name']}")
        print(f"   Alternative name: {logies['alternative_name']}")
        print(f"   Description: {logies['description']}")
        print(f"   Sleeping places: {logies['sleeping_places']}")
        print(f"   Rental units: {logies['rental_units_count']}")

        # Check relationships for test entity
        entity_addresses = [rel for rel in importer.logies_addresses if rel['logies_id'] == test_entity_id]
        entity_contacts = [rel for rel in importer.logies_contacts if rel['logies_id'] == test_entity_id]
        entity_geometries = [rel for rel in importer.logies_geometries if rel['logies_id'] == test_entity_id]

        print(f"   Address links: {len(entity_addresses)}")
        print(f"   Contact links: {len(entity_contacts)}")
        print(f"   Geometry links: {len(entity_geometries)}")

        if entity_contacts:
            print(f"\n   📞 CONTACT DETAILS:")
            for rel in entity_contacts[:3]:  # Show first 3
                contact_id = rel['contact_id']
                if contact_id in importer.contact_points:
                    contact = importer.contact_points[contact_id]
                    print(f"     {contact['telephone'] or 'N/A'} | {contact['email'] or 'N/A'} | {contact['website'] or 'N/A'}")

    # Check a sample tourist attraction
    if importer.tourist_attractions:
        sample_attraction = next(iter(importer.tourist_attractions.values()))
        print(f"\n🏛️ SAMPLE TOURIST ATTRACTION:")
        print(f"   ID: {sample_attraction['id']}")
        print(f"   Name: {sample_attraction['name']}")
        print(f"   Alternative name: {sample_attraction['alternative_name']}")

    return importer

if __name__ == "__main__":
    test_enhanced_parser()