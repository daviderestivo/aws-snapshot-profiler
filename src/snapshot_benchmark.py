#!/usr/bin/env python3
import boto3
import time
import csv
import os
import subprocess
import argparse
import random
from datetime import datetime

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module="boto3")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="botocore")
warnings.filterwarnings("ignore", message=".*Boto3 will no longer support Python.*")

def create_random_file(size_gb):
    """Create file with random content and random name"""
    random_num = random.randint(10000, 99999)
    filename = f"/tmp/aws-snapshot-profiler-{random_num}.dat"
    size_mb = size_gb * 1024

    print(f"Creating {size_gb}GB random file: {filename}")
    subprocess.run(['dd', 'if=/dev/urandom', f'of={filename}', 'bs=1M', f'count={size_mb}'], check=True)
    print(f"File {filename} created successfully")
    return filename

def get_instance_metadata():
    """Get current instance and volume info"""
    ec2 = boto3.client('ec2')

    # Get instance ID from metadata
    instance_id = subprocess.check_output(['curl', '-s', 'http://169.254.169.254/latest/meta-data/instance-id']).decode().strip()

    # Get root volume ID
    response = ec2.describe_instances(InstanceIds=[instance_id])
    volume_id = response['Reservations'][0]['Instances'][0]['BlockDeviceMappings'][0]['Ebs']['VolumeId']

    return instance_id, volume_id

def create_snapshot_and_measure(volume_id, snapshot_num):
    """Create snapshot and measure time"""
    ec2 = boto3.client('ec2')

    print(f"Starting snapshot {snapshot_num}...")
    start_time = time.time()

    response = ec2.create_snapshot(
        VolumeId=volume_id,
        Description=f'Benchmark snapshot {snapshot_num}'
    )
    snapshot_id = response['SnapshotId']

    # Wait for snapshot completion
    waiter = ec2.get_waiter('snapshot_completed')
    waiter.wait(SnapshotIds=[snapshot_id])

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Snapshot {snapshot_id} completed in {elapsed_time:.2f} seconds")
    return snapshot_id, elapsed_time

def record_to_csv(snapshot_num, elapsed_time, csv_filename):
    """Record results to CSV"""
    file_exists = os.path.exists(csv_filename)

    with open(csv_filename, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(['snapshot_number', 'elapsed_time'])
        writer.writerow([snapshot_num, elapsed_time])

def copy_snapshot_and_create_ami(snapshot_id, source_region):
    """Copy snapshot to another AZ and create AMI"""
    # Get available regions
    ec2 = boto3.client('ec2')
    regions = ec2.describe_regions()['Regions']
    target_region = next(r['RegionName'] for r in regions if r['RegionName'] != source_region)

    print(f"Copying snapshot to {target_region}...")

    # Copy snapshot
    target_ec2 = boto3.client('ec2', region_name=target_region)
    copy_response = target_ec2.copy_snapshot(
        SourceRegion=source_region,
        SourceSnapshotId=snapshot_id,
        Description='Copied benchmark snapshot'
    )
    copied_snapshot_id = copy_response['SnapshotId']

    # Wait for copy completion
    waiter = target_ec2.get_waiter('snapshot_completed')
    waiter.wait(SnapshotIds=[copied_snapshot_id])

    print(f"Creating AMI from copied snapshot...")

    # Create AMI
    ami_response = target_ec2.register_image(
        Name=f'benchmark-ami-{int(time.time())}',
        Architecture='x86_64',
        RootDeviceName='/dev/sda1',
        BlockDeviceMappings=[{
            'DeviceName': '/dev/sda1',
            'Ebs': {
                'SnapshotId': copied_snapshot_id,
                'VolumeType': 'gp3'
            }
        }]
    )

    print(f"AMI created: {ami_response['ImageId']}")
    return ami_response['ImageId']

def main():
    parser = argparse.ArgumentParser(description='AWS EBS Snapshot Benchmark Tool')
    parser.add_argument('-n', '--num-snapshots', type=int, default=1, help='Number of snapshots to create (default: 1)')
    parser.add_argument('-o', '--output', default='snapshot_results.csv', help='Output CSV filename (default: snapshot_results.csv)')
    parser.add_argument('-s', '--size', type=int, default=10, help='File size in GB (default: 10)')
    args = parser.parse_args()

    try:
        # Get instance info
        instance_id, volume_id = get_instance_metadata()
        current_region = boto3.Session().region_name

        # Create snapshots in loop
        for snapshot_num in range(1, args.num_snapshots + 1):
            # Step 1: Create random file for each snapshot
            filename = create_random_file(args.size)

            # Step 2 & 3: Create snapshot and measure time
            snapshot_id, elapsed_time = create_snapshot_and_measure(volume_id, snapshot_num)

            # Step 4: Record to CSV
            record_to_csv(snapshot_num, elapsed_time, args.output)

            # Step 5: Copy snapshot and create AMI (only for last snapshot)
            if snapshot_num == args.num_snapshots:
                ami_id = copy_snapshot_and_create_ami(snapshot_id, current_region)
                print(f"AMI created: {ami_id}")

        print(f"Process completed successfully!")
        print(f"Created {args.num_snapshots} snapshots")
        print(f"Results saved to {args.output}")

    except Exception as e:
        print(f"Error: {e}")
        return 1

    return 0

if __name__ == "__main__":
    exit(main())
