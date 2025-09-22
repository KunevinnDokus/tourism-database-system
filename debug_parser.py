#!/usr/bin/env python3
"""
Debug parser to identify why sleeping_places and rental_units_count are not being extracted
"""

import re

def debug_ttl_parsing():
    """Debug TTL parsing to find the issue with missing fields"""

    print("🔍 DEBUG: Analyzing TTL parsing for sleeping_places and rental_units_count")
    print("=" * 70)

    # Test specific entity
    test_entity = "b3bb7490-e37c-11ed-b6ca-e5a176a460b1"

    # Read and analyze the TTL file
    logies_found = 0
    sleeping_places_found = 0
    rental_units_found = 0
    test_entity_data = {}

    with open('toeristische-attracties.ttl', 'r', encoding='utf-8') as file:
        for line_num, line in enumerate(file, 1):
            line = line.strip()

            # Check if this line contains our test entity
            if test_entity in line:
                print(f"Line {line_num}: {line}")

                # Parse the line
                if 'aantalSlaapplaatsen' in line:
                    sleeping_places_found += 1
                    match = re.search(r'"(\d+)"', line)
                    if match:
                        value = match.group(1)
                        test_entity_data['sleeping_places'] = value
                        print(f"  → Found sleeping_places: {value}")

                if 'aantalVerhuureenheden' in line:
                    rental_units_found += 1
                    match = re.search(r'"(\d+)"', line)
                    if match:
                        value = match.group(1)
                        test_entity_data['rental_units'] = value
                        print(f"  → Found rental_units: {value}")

                if 'Logies>' in line and 'type' in line:
                    logies_found += 1
                    print(f"  → Found Logies type declaration")

            # Stop after reasonable number of lines for debugging
            if line_num > 50000:
                break

    print(f"\n📊 Debug Results for entity {test_entity}:")
    print(f"  Logies declarations: {logies_found}")
    print(f"  Sleeping places: {sleeping_places_found}")
    print(f"  Rental units: {rental_units_found}")
    print(f"  Extracted data: {test_entity_data}")

    # Now let's test the parser logic
    print(f"\n🧪 Testing Parser Logic:")

    # Simulate the parsing logic
    test_predicates = [
        "https://data.vlaanderen.be/ns/logies#aantalSlaapplaatsen",
        "https://data.vlaanderen.be/ns/logies#aantalVerhuureenheden"
    ]

    for predicate in test_predicates:
        if 'aantalSlaapplaatsen' in predicate:
            print(f"  ✅ Predicate '{predicate}' matches 'aantalSlaapplaatsen'")
        elif 'aantalVerhuureenheden' in predicate:
            print(f"  ✅ Predicate '{predicate}' matches 'aantalVerhuureenheden'")
        else:
            print(f"  ❌ Predicate '{predicate}' doesn't match")

    # Check actual predicate format in file
    print(f"\n🔍 Checking actual predicate formats in TTL:")
    with open('toeristische-attracties.ttl', 'r', encoding='utf-8') as file:
        for line_num, line in enumerate(file, 1):
            if 'aantalSlaapplaatsen' in line or 'aantalVerhuureenheden' in line:
                # Extract the predicate part
                parts = line.split(' ')
                if len(parts) >= 2:
                    predicate = parts[1]
                    print(f"  Predicate: {predicate}")
                break

            if line_num > 10000:
                break

if __name__ == "__main__":
    debug_ttl_parsing()