#!/usr/bin/env python3

"""
findParentChldren.py - PolicyHarvester Parent-Child Job Analysis Tool

This script analyzes production log files to find PolicyHarvester jobs and count their children.
Reads from STDIN and outputs CSV with columns: policyID, harvesterJobID, childUserHarvesterCount, childNONUserHarvesterCount

Usage:
    cat syslog-20250702-1751432400 | ./findParentChldren.py
    cat DnZ9sV.log | python3 findParentChldren.py

Output CSV Format:
    policyID,harvesterJobID,childUserHarvesterCount,childNONUserHarvesterCount
    9533B6590EEB7261,3021486220533345705,36,0
"""

import sys
import re
import csv
from collections import defaultdict

def extract_policy_id_from_name(job_name):
    """Extract policyID from job name like 'www-prod/UserHarvester?policyID=...'"""
    match = re.search(r'policyID=([A-F0-9]+)', job_name)
    return match.group(1) if match else None

def extract_policy_id_from_extraparams(extraparams_str):
    """Extract policyID from extraParams like 'policyID=9533B6590EEB7261'"""
    match = re.search(r"policyID=([A-F0-9]+)", extraparams_str)
    return match.group(1) if match else None

def extract_parent_job_id(line):
    """Extract parentJobID from job lines"""
    match = re.search(r"parentJobID['\"]?\s*[:=]\s*['\"]?(\d+)", line)
    return match.group(1) if match else None

def extract_job_names_from_line(line):
    """Extract all job names from a line containing job definitions"""
    # Look for patterns like name: 'www-prod/UserHarvester?...' or name: "www-prod/UserHarvester?..."
    job_names = re.findall(r"name['\"]?\s*[:=]\s*['\"]([^'\"]+)['\"]", line)
    return job_names

def is_user_harvester_job(job_name):
    """Check if job name indicates a UserHarvester job"""
    return job_name and 'UserHarvester' in job_name

def extract_policy_harvester_from_data(line):
    """Extract policyHarvesterID and policyID from job data in lines"""
    # Look for policyHarvesterID in data
    harvester_id_match = re.search(r"policyHarvesterID['\"]?\s*[:=]\s*['\"]?(\d+)", line)
    # Look for policyID in data
    policy_id_match = re.search(r"policyID['\"]?\s*[:=]\s*['\"]?([A-F0-9]+)", line)

    if harvester_id_match and policy_id_match:
        return harvester_id_match.group(1), policy_id_match.group(1)
    return None, None

def safe_read_stdin():
    """Safely read from stdin with encoding error handling"""
    lines = []
    for line_bytes in sys.stdin.buffer:
        try:
            # Try to decode as UTF-8, replacing bad bytes
            line = line_bytes.decode('utf-8', errors='replace').strip()
            lines.append(line)
        except Exception as e:
            # If there's any other error, skip this line and continue
            print(f"Warning: Skipped line due to encoding error: {e}", file=sys.stderr)
            continue
    return lines

def main():
    # Read all lines first
    lines = safe_read_stdin()

    # Store PolicyHarvester jobs: {jobID: policyID}
    policy_harvesters = {}

    # Store child job counts: {parentJobID: {'user': count, 'non_user': count}}
    child_counts = defaultdict(lambda: {'user': 0, 'non_user': 0})

    # PASS 1: Collect all PolicyHarvester jobs
    for line_num, line in enumerate(lines, 1):
        # Method 1: Look for PolicyHarvester job starts - pattern: running job ~~ name: 'www-prod/PolicyHarvester' id: 'ID' extraParams: 'policyID=...'
        if "running job" in line and "www-prod/PolicyHarvester" in line:
            # Extract job ID
            id_match = re.search(r"id:\s*'(\d+)'", line)
            # Extract policyID from extraParams
            policy_match = re.search(r"extraParams:\s*'[^']*policyID=([A-F0-9]+)", line)

            if id_match and policy_match:
                harvester_job_id = id_match.group(1)
                policy_id = policy_match.group(1)
                policy_harvesters[harvester_job_id] = policy_id

        # Method 2: Extract PolicyHarvester info from child job data (policyHarvesterID references)
        if 'policyHarvesterID' in line:
            harvester_id, policy_id = extract_policy_harvester_from_data(line)
            if harvester_id and policy_id:
                policy_harvesters[harvester_id] = policy_id

        # Method 3: Look for PolicyHarvester creation via CreateJob/CreateJobs commands
        if 'PolicyHarvester' in line and ('CreateJob' in line or 'CreateJobs' in line):
            # Extract job names that contain PolicyHarvester
            job_names = extract_job_names_from_line(line)
            for job_name in job_names:
                if 'PolicyHarvester' in job_name:
                    # Try to extract policyID from the job name
                    policy_id = extract_policy_id_from_name(job_name)
                    if policy_id:
                        # For CreateJobs commands, we need to look for jobID in the response
                        # This is a bit tricky since we don't have the response here
                        # We'll mark this pattern for now but it may not capture the job ID
                        pass

    # PASS 2: Count child jobs
    for line_num, line in enumerate(lines, 1):
        # Look for child job creation lines that contain parentJobID references
        # These can be in "Create jobs" or "CreateJobs" command lines
        if 'parentJobID' in line and ('Create jobs' in line or 'CreateJobs' in line):
            # Extract all parentJobIDs from this line
            parent_job_ids = re.findall(r"parentJobID['\"]?\s*[:=]\s*['\"]?(\d+)", line)

            # Extract all job names from this line
            job_names = extract_job_names_from_line(line)

            # For each job name, find its corresponding parentJobID and count it
            for job_name in job_names:
                # For each parentJobID mentioned in this line
                for parent_job_id in parent_job_ids:
                    if parent_job_id in policy_harvesters:
                        if is_user_harvester_job(job_name):
                            child_counts[parent_job_id]['user'] += 1
                        else:
                            child_counts[parent_job_id]['non_user'] += 1
                        # We found a match, break to avoid double counting
                        break

    # Output CSV
    writer = csv.writer(sys.stdout)
    writer.writerow(['policyID', 'harvesterJobID', 'childUserHarvesterCount', 'childNONUserHarvesterCount'])

    for harvester_job_id, policy_id in policy_harvesters.items():
        counts = child_counts[harvester_job_id]
        writer.writerow([
            policy_id,
            harvester_job_id,
            counts['user'],
            counts['non_user']
        ])

if __name__ == '__main__':
    main()