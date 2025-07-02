#!/usr/bin/env python3

"""
histogram.py - PolicyHarvester Child Count Histogram Generator

This script reads the CSV output from findParentChldren.py and creates a histogram
showing the distribution of UserHarvester child counts per PolicyHarvester job.

Usage:
    python3 histogram.py result.csv
    ./histogram.py result.csv

Input CSV Format (expected from findParentChldren.py):
    policyID,harvesterJobID,childUserHarvesterCount,childNONUserHarvesterCount
"""

import sys
import csv
from collections import defaultdict

def create_histogram(child_counts):
    """Create histogram buckets and count jobs in each bucket"""

    # Determine the range of data to create appropriate buckets
    if not child_counts:
        print("No data to analyze!")
        return

    max_count = max(child_counts)
    min_count = min(child_counts)

    print(f"Child Count Statistics:")
    print(f"  Min: {min_count}")
    print(f"  Max: {max_count}")
    print(f"  Total PolicyHarvester jobs: {len(child_counts)}")
    print(f"  Average children per job: {sum(child_counts) / len(child_counts):.1f}")
    print()

    # Define bucket ranges based on the data distribution
    if max_count <= 10:
        # Small numbers, use individual buckets
        buckets = [(i, i) for i in range(min_count, max_count + 1)]
    elif max_count <= 100:
        # Medium numbers, use ranges of 10
        buckets = [
            (0, 0),
            (1, 10),
            (11, 20),
            (21, 30),
            (31, 40),
            (41, 50),
            (51, 60),
            (61, 70),
            (71, 80),
            (81, 90),
            (91, 100)
        ]
    else:
        # Large numbers, use larger ranges
        buckets = [
            (0, 0),
            (1, 10),
            (11, 25),
            (26, 50),
            (51, 100),
            (101, 200),
            (201, 500),
            (501, 1000),
            (1001, float('inf'))
        ]

    # Count jobs in each bucket
    bucket_counts = defaultdict(int)

    for count in child_counts:
        for bucket_min, bucket_max in buckets:
            if bucket_min <= count <= bucket_max:
                bucket_counts[(bucket_min, bucket_max)] += 1
                break

    # Display histogram
    print("Histogram: UserHarvester Children per PolicyHarvester Job")
    print("=" * 60)

    max_bucket_count = max(bucket_counts.values()) if bucket_counts else 0
    bar_scale = max(1, max_bucket_count // 50)  # Scale bars to fit reasonably

    for bucket_min, bucket_max in buckets:
        count = bucket_counts.get((bucket_min, bucket_max), 0)

        if count == 0:
            continue  # Skip empty buckets

        # Format bucket label
        if bucket_max == float('inf'):
            bucket_label = f"{bucket_min}+"
        elif bucket_min == bucket_max:
            bucket_label = f"{bucket_min}"
        else:
            bucket_label = f"{bucket_min}-{bucket_max}"

        # Create bar visualization
        bar_length = count // bar_scale
        bar = "█" * bar_length

        # Print histogram line
        print(f"{bucket_label:>8} children: {count:>6} jobs {bar}")

    print("=" * 60)
    print(f"Scale: Each █ represents ~{bar_scale} job(s)")

def main():
    # Check command line arguments
    if len(sys.argv) != 2:
        print("Usage: python3 histogram.py <csv_file>")
        print("Example: python3 histogram.py result.csv")
        sys.exit(1)

    csv_file = sys.argv[1]

    try:
        child_counts = []

        with open(csv_file, 'r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)

            # Verify expected columns are present
            expected_columns = ['policyID', 'harvesterJobID', 'childUserHarvesterCount', 'childNONUserHarvesterCount']
            if not all(col in reader.fieldnames for col in expected_columns):
                print(f"Error: CSV file missing expected columns. Found: {reader.fieldnames}")
                sys.exit(1)

            # Read data and extract child counts
            for row_num, row in enumerate(reader, start=2):  # Start at 2 since header is line 1
                try:
                    child_count = int(row['childUserHarvesterCount'])
                    child_counts.append(child_count)
                except ValueError as e:
                    print(f"Warning: Invalid number in row {row_num}: {row['childUserHarvesterCount']}", file=sys.stderr)
                    continue

        if not child_counts:
            print("No valid data found in CSV file!")
            sys.exit(1)

        # Generate histogram
        create_histogram(child_counts)

    except FileNotFoundError:
        print(f"Error: File '{csv_file}' not found!")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()