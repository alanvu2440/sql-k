#!/usr/bin/env python3
"""
Extract PostgreSQL dump: full schema + sample rows WITH embeddings (target ~30MB)
"""

import re
import os

def process_sql_dump(input_file, output_file, max_rows=500, target_size_mb=30):
    """Extract schema + data rows up to target file size"""
    
    target_bytes = target_size_mb * 1024 * 1024
    
    with open(input_file, 'r', encoding='utf-8', errors='ignore') as infile:
        with open(output_file, 'w', encoding='utf-8') as outfile:
            
            in_copy_block = False
            current_table = None
            row_count = 0
            table_stats = {}
            bytes_written = 0
            
            for line in infile:
                # Check file size limit
                if bytes_written > target_bytes and in_copy_block:
                    # Stop adding more data rows but finish current table
                    if line.strip() == r'\.':
                        outfile.write(line)
                        outfile.write(f"-- {current_table}: {table_stats[current_table]} rows (limit reached at ~{target_size_mb}MB)\n\n")
                        in_copy_block = False
                    continue
                
                # Detect COPY statement
                copy_match = re.match(r'COPY (?:public\.)?(\w+)\s*\((.*?)\)', line, re.IGNORECASE)
                
                if copy_match:
                    current_table = copy_match.group(1)
                    in_copy_block = True
                    row_count = 0
                    table_stats[current_table] = 0
                    outfile.write(line)
                    bytes_written += len(line.encode('utf-8'))
                
                elif in_copy_block:
                    if line.strip() == r'\.':
                        # End of COPY block
                        outfile.write(line)
                        bytes_written += len(line.encode('utf-8'))
                        outfile.write(f"-- {current_table}: {table_stats[current_table]} rows included\n\n")
                        in_copy_block = False
                        current_table = None
                        row_count = 0
                    elif row_count < max_rows:
                        # Include data row WITH embeddings
                        outfile.write(line)
                        bytes_written += len(line.encode('utf-8'))
                        row_count += 1
                        table_stats[current_table] = row_count
                    # else: skip rows beyond per-table limit
                
                else:
                    # Schema, indexes, triggers, etc. - always include
                    outfile.write(line)
                    bytes_written += len(line.encode('utf-8'))
            
            # Summary
            final_size_mb = bytes_written / (1024 * 1024)
            outfile.write(f"\n\n-- === EXTRACTION SUMMARY ===\n")
            outfile.write(f"-- File size: ~{final_size_mb:.1f} MB\n")
            outfile.write(f"-- Tables processed: {len(table_stats)}\n")
            for table, count in sorted(table_stats.items()):
                outfile.write(f"--   {table}: {count} rows\n")
            outfile.write(f"-- Embeddings included (768-dim vectors)\n")
            
            return final_size_mb

if __name__ == "__main__":
    input_file = "pet_travel_db_export_Cloud_SQL_Export_2025-10-13 (17_00_40) 2.sql"
    output_file = "pet_travel_schema_sample_30mb.sql"
    
    print(f"Processing {input_file}...")
    print(f"Extracting: ALL schema + data rows (target ~30MB with embeddings)")
    
    final_size = process_sql_dump(input_file, output_file, max_rows=500, target_size_mb=30)
    
    print(f"✓ Done! Output: {output_file}")
    print(f"  Final size: ~{final_size:.1f} MB")
