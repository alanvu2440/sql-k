#!/usr/bin/env python3
"""
Extract PostgreSQL dump: full schema + 100 sample rows per table WITHOUT embeddings
"""

import re

def process_sql_dump(input_file, output_file, max_rows=100):
    with open(input_file, 'r', encoding='utf-8', errors='ignore') as infile:
        with open(output_file, 'w', encoding='utf-8') as outfile:
            
            in_copy_block = False
            current_table = None
            row_count = 0
            table_stats = {}
            embedding_col_index = None
            column_list = []
            
            for line in infile:
                # Detect COPY statement
                copy_match = re.match(r'COPY (?:public\.)?(\w+)\s*\((.*?)\)', line, re.IGNORECASE)
                
                if copy_match:
                    current_table = copy_match.group(1)
                    columns_str = copy_match.group(2)
                    column_list = [col.strip() for col in columns_str.split(',')]
                    
                    # Find embedding column index
                    try:
                        embedding_col_index = column_list.index('embedding')
                    except ValueError:
                        embedding_col_index = None
                    
                    in_copy_block = True
                    row_count = 0
                    table_stats[current_table] = 0
                    outfile.write(line)  # Write COPY statement as-is
                
                elif in_copy_block:
                    if line.strip() == r'\.':
                        # End of COPY block
                        outfile.write(line)
                        outfile.write(f"-- {current_table}: {table_stats[current_table]} rows (embeddings removed)\n\n")
                        in_copy_block = False
                        current_table = None
                        row_count = 0
                        embedding_col_index = None
                    elif row_count < max_rows:
                        # Process and write data row (strip embedding)
                        if embedding_col_index is not None:
                            # Split by tab (PostgreSQL COPY default delimiter)
                            parts = line.split('\t')
                            if len(parts) > embedding_col_index:
                                # Replace embedding with placeholder
                                parts[embedding_col_index] = '\\N'  # NULL in COPY format
                                line = '\t'.join(parts)
                        
                        outfile.write(line)
                        row_count += 1
                        table_stats[current_table] = row_count
                    # else: skip rows beyond limit
                
                else:
                    # Schema, indexes, triggers, etc. - always include
                    outfile.write(line)
            
            # Summary
            outfile.write(f"\n\n-- === EXTRACTION SUMMARY ===\n")
            outfile.write(f"-- Tables processed: {len(table_stats)}\n")
            for table, count in sorted(table_stats.items()):
                outfile.write(f"--   {table}: {count} rows\n")
            outfile.write(f"-- Embedding columns replaced with NULL to reduce file size\n")

if __name__ == "__main__":
    input_file = "pet_travel_db_export_Cloud_SQL_Export_2025-10-13 (17_00_40) 2.sql"
    output_file = "pet_travel_schema_sample_100rows.sql"
    
    print(f"Processing {input_file}...")
    print(f"Extracting: ALL schema + 100 rows/table (embeddings removed)")
    
    process_sql_dump(input_file, output_file, max_rows=100)
    
    print(f"✓ Done! Output: {output_file}")
