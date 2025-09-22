#!/usr/bin/env python3
"""
Test the FIXED parser to verify it collects all properties for interleaved entities
"""

from corrected_ttl_to_postgres_FIXED import FixedTourismDataImporter

def test_fixed_parser():
    print("🧪 Testing FIXED parser with interleaved entity handling")
    print("=" * 70)

    # Create importer (no need for actual DB connection for this test)
    db_config = {'host': 'localhost', 'port': '5432', 'database': 'test', 'user': 'test', 'password': 'test'}
    importer = FixedTourismDataImporter(db_config)

    # Override process_entity to intercept our test entity
    test_entity_id = "b3bb7490-e37c-11ed-b6ca-e5a176a460b1"
    original_process_entity = importer.process_entity

    def intercept_process_entity(subject_uri, properties):
        if test_entity_id in subject_uri:
            print(f"\n🎯 INTERCEPTED TEST ENTITY: {test_entity_id}")
            print(f"   Subject URI: {subject_uri}")
            print(f"   Total properties: {len(properties)}")

            # Check for our critical properties
            found_sleeping = False
            found_rental = False

            for predicate, values in properties.items():
                if 'aantalSlaapplaatsen' in predicate:
                    found_sleeping = True
                    print(f"   ✅ Found sleeping_places: {predicate} -> {values}")
                elif 'aantalVerhuureenheden' in predicate:
                    found_rental = True
                    print(f"   ✅ Found rental_units: {predicate} -> {values}")

            if found_sleeping and found_rental:
                print(f"   🎉 SUCCESS: Both critical properties found!")
            else:
                print(f"   ❌ FAILURE: Missing properties (sleeping: {found_sleeping}, rental: {found_rental})")

            # Also run the normal processing to see final result
            return original_process_entity(subject_uri, properties)
        else:
            return original_process_entity(subject_uri, properties)

    importer.process_entity = intercept_process_entity

    # Parse the file
    importer.parse_ttl_file('toeristische-attracties.ttl')

    print(f"\n📊 Final Results:")
    print(f"   Logies entities collected: {len(importer.logies)}")

    # Check if our test entity was saved correctly
    if test_entity_id in importer.logies:
        logies_data = importer.logies[test_entity_id]
        print(f"   Test entity data: {logies_data}")
        print(f"   Sleeping places: {logies_data['sleeping_places']}")
        print(f"   Rental units: {logies_data['rental_units_count']}")

        if logies_data['sleeping_places'] > 0 and logies_data['rental_units_count'] > 0:
            print(f"   🎉 PARSER FIX SUCCESSFUL!")
        else:
            print(f"   ❌ Parser fix failed - fields still zero")
    else:
        print(f"   ❌ Test entity not found in results")

if __name__ == "__main__":
    test_fixed_parser()