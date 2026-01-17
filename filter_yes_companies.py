#!/usr/bin/env python3
"""
Filter YES companies from industry filter output
"""

import csv
import sys

def main():
    input_file = 'industry_filter_output_batch1_rows1-50.csv'
    output_file = 'companies_industries_filtered.csv'
    
    print(f"Reading {input_file}...")
    
    yes_companies = []
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('is_target_industry', '').strip().upper() == 'YES':
                    yes_companies.append(row)
    except FileNotFoundError:
        print(f"Error: {input_file} not found!")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading {input_file}: {e}")
        sys.exit(1)
    
    print(f"Found {len(yes_companies)} YES companies out of total rows.")
    
    if len(yes_companies) == 0:
        print("No YES companies found. Exiting.")
        sys.exit(0)
    
    # Write filtered CSV
    print(f"Writing {len(yes_companies)} YES companies to {output_file}...")
    
    output_columns = [
        'company_name', 'matched_domain', 'website', 'is_target_industry',
        'matched_keywords', 'evidence_url', 'notes'
    ]
    
    try:
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=output_columns)
            writer.writeheader()
            writer.writerows(yes_companies)
    except Exception as e:
        print(f"Error writing {output_file}: {e}")
        sys.exit(1)
    
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"YES companies saved: {len(yes_companies)}")
    print()
    print("Companies:")
    for company in yes_companies:
        print(f"  - {company['company_name']}")
    
    print()
    print(f"Results saved to {output_file}")

if __name__ == "__main__":
    main()
