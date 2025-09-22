#!/usr/bin/env python3
"""
Comprehensive audit of all database tables to identify structurally empty fields
"""

import psycopg2
from typing import Dict, List, Tuple

def audit_database_empty_fields():
    """Audit all tables for structurally empty fields"""

    print("🔍 COMPREHENSIVE DATABASE AUDIT: Structurally Empty Fields")
    print("=" * 80)

    # Connect to production database
    conn = psycopg2.connect(
        host='localhost',
        port='5432',
        database='tourism_flanders_corrected',
        user='lieven',
        password=''
    )
    cursor = conn.cursor()

    # Get all tables
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)

    tables = [row[0] for row in cursor.fetchall()]
    print(f"Found {len(tables)} tables to audit")

    # Track empty fields by category
    completely_empty = []  # 100% NULL/empty
    mostly_empty = []      # >95% NULL/empty
    suspicious_zeros = []  # All zeros (could be missing data)

    for table in tables:
        print(f"\n📊 Auditing table: {table}")

        # Get table row count
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        total_rows = cursor.fetchone()[0]

        if total_rows == 0:
            print(f"   ⚠️  Table is completely empty ({total_rows} rows)")
            continue

        print(f"   Total rows: {total_rows:,}")

        # Get all columns for this table
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = %s
            ORDER BY ordinal_position
        """, (table,))

        columns = cursor.fetchall()

        for column_name, data_type, is_nullable in columns:
            # Skip primary key and foreign key columns
            if column_name in ['id', 'logies_id', 'attraction_id', 'entity_id']:
                continue

            # Check for NULL values
            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {column_name} IS NULL")
            null_count = cursor.fetchone()[0]
            null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0

            # Check for empty strings (for text fields)
            empty_count = 0
            if data_type in ['character varying', 'varchar', 'text']:
                cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {column_name} = ''")
                empty_count = cursor.fetchone()[0]

            # Check for zeros (for numeric fields)
            zero_count = 0
            if data_type in ['integer', 'bigint', 'numeric', 'decimal', 'real', 'double precision']:
                cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {column_name} = 0")
                zero_count = cursor.fetchone()[0]
                zero_percentage = (zero_count / total_rows) * 100 if total_rows > 0 else 0

            total_empty = null_count + empty_count
            empty_percentage = (total_empty / total_rows) * 100 if total_rows > 0 else 0

            # Categorize problematic fields
            if empty_percentage == 100:
                completely_empty.append((table, column_name, data_type, total_rows))
                print(f"   🚨 {column_name} ({data_type}): 100% empty ({total_empty:,}/{total_rows:,})")

            elif empty_percentage >= 95:
                mostly_empty.append((table, column_name, data_type, empty_percentage, total_empty, total_rows))
                print(f"   ⚠️  {column_name} ({data_type}): {empty_percentage:.1f}% empty ({total_empty:,}/{total_rows:,})")

            elif data_type in ['integer', 'bigint'] and zero_percentage >= 95:
                suspicious_zeros.append((table, column_name, data_type, zero_percentage, zero_count, total_rows))
                print(f"   🔢 {column_name} ({data_type}): {zero_percentage:.1f}% zeros ({zero_count:,}/{total_rows:,})")

            elif empty_percentage > 50:
                print(f"   📝 {column_name} ({data_type}): {empty_percentage:.1f}% empty ({total_empty:,}/{total_rows:,})")

    # Summary report
    print(f"\n" + "=" * 80)
    print(f"🚨 COMPLETELY EMPTY FIELDS (100% NULL/empty):")
    print(f"=" * 80)

    if not completely_empty:
        print("   ✅ No completely empty fields found!")
    else:
        for table, column, data_type, rows in completely_empty:
            print(f"   - {table}.{column} ({data_type}) - {rows:,} rows")

    print(f"\n⚠️  MOSTLY EMPTY FIELDS (≥95% NULL/empty):")
    print(f"=" * 50)

    if not mostly_empty:
        print("   ✅ No mostly empty fields found!")
    else:
        for table, column, data_type, pct, empty, total in mostly_empty:
            print(f"   - {table}.{column} ({data_type}) - {pct:.1f}% empty ({empty:,}/{total:,})")

    print(f"\n🔢 SUSPICIOUS ZERO FIELDS (≥95% zeros):")
    print(f"=" * 50)

    if not suspicious_zeros:
        print("   ✅ No suspicious zero fields found!")
    else:
        for table, column, data_type, pct, zeros, total in suspicious_zeros:
            print(f"   - {table}.{column} ({data_type}) - {pct:.1f}% zeros ({zeros:,}/{total:,})")

    # Focus on key business tables
    print(f"\n📊 KEY BUSINESS TABLE ANALYSIS:")
    print(f"=" * 50)

    key_tables = ['logies', 'tourist_attractions', 'addresses', 'contact_points', 'geometries']

    for table in key_tables:
        if table in tables:
            print(f"\n🔍 Detailed analysis of {table}:")

            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            total_rows = cursor.fetchone()[0]

            # Get sample data
            cursor.execute(f"SELECT * FROM {table} LIMIT 3")
            sample_data = cursor.fetchall()

            if sample_data:
                # Get column names
                cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                    AND table_name = %s
                    ORDER BY ordinal_position
                """, (table,))
                column_names = [row[0] for row in cursor.fetchall()]

                print(f"   Columns: {', '.join(column_names)}")
                print(f"   Sample data (first 3 rows):")
                for i, row in enumerate(sample_data, 1):
                    print(f"   Row {i}: {dict(zip(column_names, row))}")

    cursor.close()
    conn.close()

    return completely_empty, mostly_empty, suspicious_zeros

if __name__ == "__main__":
    audit_database_empty_fields()