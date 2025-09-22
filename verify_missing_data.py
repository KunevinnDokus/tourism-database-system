#!/usr/bin/env python3
"""
Verify what data should exist by analyzing the TTL source file
"""

import re
from collections import defaultdict

def analyze_ttl_source():
    """Analyze TTL file to understand what data should be populated"""

    print("🔍 ANALYZING TTL SOURCE DATA")
    print("=" * 60)

    # Track what we find in the source
    found_predicates = defaultdict(int)
    found_relationships = defaultdict(int)
    sample_data = defaultdict(list)

    # Analyze our test entity first
    test_entity = "b3bb7490-e37c-11ed-b6ca-e5a176a460b1"
    test_entity_data = {}

    print(f"🎯 Analyzing test entity: {test_entity}")

    with open('toeristische-attracties.ttl', 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            if line.startswith('<') and '>' in line:
                parts = line.split(None, 2)
                if len(parts) >= 3:
                    subject = parts[0].strip('<>')
                    predicate = parts[1].strip('<>')
                    obj_part = parts[2].rstrip(' .')

                    # Track all predicates
                    predicate_name = predicate.split('#')[-1] if '#' in predicate else predicate.split('/')[-1]
                    found_predicates[predicate_name] += 1

                    # Check for our test entity
                    if test_entity in subject:
                        test_entity_data[predicate] = obj_part

                    # Look for key missing data types
                    if 'alternativeName' in predicate or 'altLabel' in predicate:
                        sample_data['alternative_names'].append((subject, obj_part))

                    elif 'description' in predicate or 'comment' in predicate:
                        sample_data['descriptions'].append((subject, obj_part))

                    elif 'accessibility' in predicate.lower():
                        sample_data['accessibility'].append((subject, obj_part))

                    elif 'address' in predicate.lower() and not predicate.endswith('address'):
                        found_relationships['address_relationships'] += 1
                        if len(sample_data['address_relationships']) < 5:
                            sample_data['address_relationships'].append((subject, predicate, obj_part))

                    elif 'contactPoint' in predicate:
                        found_relationships['contact_relationships'] += 1
                        if len(sample_data['contact_relationships']) < 5:
                            sample_data['contact_relationships'].append((subject, predicate, obj_part))

                    elif 'location' in predicate or 'geometry' in predicate.lower():
                        found_relationships['geometry_relationships'] += 1
                        if len(sample_data['geometry_relationships']) < 5:
                            sample_data['geometry_relationships'].append((subject, predicate, obj_part))

                    elif 'TouristAttraction' in obj_part and 'type' in predicate:
                        found_relationships['tourist_attractions'] += 1
                        if len(sample_data['tourist_attractions']) < 5:
                            sample_data['tourist_attractions'].append((subject, predicate, obj_part))

            # Stop after reasonable scan
            if line_num > 500000:
                break

    print(f"\n📊 ANALYSIS RESULTS:")
    print(f"=" * 40)

    print(f"\n🎯 Test Entity ({test_entity}) Properties:")
    for predicate, value in sorted(test_entity_data.items()):
        predicate_short = predicate.split('#')[-1] if '#' in predicate else predicate.split('/')[-1]
        print(f"   {predicate_short}: {value[:100]}...")

    print(f"\n📈 Key Predicate Counts:")
    key_predicates = ['alternativeName', 'altLabel', 'description', 'comment', 'accessibility']
    for pred in key_predicates:
        count = found_predicates.get(pred, 0)
        print(f"   {pred}: {count:,} occurrences")

    print(f"\n🔗 Relationship Counts:")
    for rel_type, count in found_relationships.items():
        print(f"   {rel_type}: {count:,} relationships")

    print(f"\n📝 Sample Data Found:")
    for data_type, samples in sample_data.items():
        if samples:
            print(f"\n   {data_type.upper()} ({len(samples)} samples):")
            for i, sample in enumerate(samples[:3], 1):
                if len(sample) == 2:
                    subject, value = sample
                    entity_id = subject.split('/')[-1]
                    print(f"     {i}. {entity_id}: {value[:100]}...")
                else:
                    subject, predicate, obj = sample
                    entity_id = subject.split('/')[-1]
                    pred_short = predicate.split('#')[-1] if '#' in predicate else predicate.split('/')[-1]
                    obj_short = obj.split('/')[-1] if obj.startswith('<') else obj[:50]
                    print(f"     {i}. {entity_id} --{pred_short}--> {obj_short}")

    # Check specifically for coordinates/geometry data
    print(f"\n🌍 GEOMETRY DATA ANALYSIS:")
    print(f"=" * 40)

    geometry_predicates = ['lat', 'long', 'latitude', 'longitude', 'Point', 'geometry', 'asWKT', 'asGML']

    with open('toeristische-attracties.ttl', 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if any(pred in line for pred in geometry_predicates):
                print(f"   Line {line_num}: {line}")
                if line_num > 10:  # Just show first few examples
                    break
            if line_num > 100000:
                break

    return test_entity_data

if __name__ == "__main__":
    analyze_ttl_source()