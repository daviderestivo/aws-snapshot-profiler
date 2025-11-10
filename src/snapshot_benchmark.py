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

    # Get instance ID from metadata (try IMDSv2 first, then IMDSv1)
    try:
        # Try IMDSv2
        token_result = subprocess.run(['curl', '-X', 'PUT', '-H', 'X-aws-ec2-metadata-token-ttl-seconds: 21600',
                                     'http://169.254.169.254/latest/api/token'],
                                    capture_output=True, text=True, timeout=5)
        if token_result.returncode == 0 and token_result.stdout.strip():
            token = token_result.stdout.strip()
            result = subprocess.run(['curl', '-H', f'X-aws-ec2-metadata-token: {token}',
                                   'http://169.254.169.254/latest/meta-data/instance-id'],
                                  capture_output=True, text=True, timeout=5)
        else:
            # Fallback to IMDSv1
            result = subprocess.run(['curl', '-s', '--connect-timeout', '5',
                                   'http://169.254.169.254/latest/meta-data/instance-id'],
                                  capture_output=True, text=True, timeout=5)

        if result.returncode != 0 or not result.stdout.strip():
            raise Exception("Failed to retrieve instance ID from metadata service")

        instance_id = result.stdout.strip()

    except Exception as e:
        raise Exception(f"Failed to get instance metadata. Ensure script runs on EC2 instance: {e}")

    # Get root volume ID
    response = ec2.describe_instances(InstanceIds=[instance_id])
    volume_id = response['Reservations'][0]['Instances'][0]['BlockDeviceMappings'][0]['Ebs']['VolumeId']

    return instance_id, volume_id

def create_snapshot_and_measure(volume_id, snapshot_num, filename):
    """Create snapshot and measure time"""
    ec2 = boto3.client('ec2')
    
    # Extract filename without path and extension for snapshot name
    snapshot_name = os.path.basename(filename).replace('.dat', '')

    print(f"Starting snapshot {snapshot_num}...")
    start_time = time.time()

    response = ec2.create_snapshot(
        VolumeId=volume_id,
        Description=f'{snapshot_name} - Benchmark snapshot {snapshot_num}'
    )
    snapshot_id = response['SnapshotId']

    # Wait for snapshot completion
    waiter = ec2.get_waiter('snapshot_completed')
    waiter.wait(SnapshotIds=[snapshot_id])

    end_time = time.time()
    elapsed_time = end_time - start_time

    # Enable fast snapshot restore
    try:
        current_region = boto3.Session().region_name
        ec2.enable_fast_snapshot_restores(
            AvailabilityZones=[f"{current_region}a"],
            SourceSnapshotIds=[snapshot_id]
        )
        print(f"Fast snapshot restore enabled for {snapshot_id}")
    except Exception as e:
        print(f"Warning: Could not enable fast snapshot restore: {e}")

    print(f"Snapshot {snapshot_id} ({snapshot_name}) completed in {elapsed_time:.2f} seconds")
    return snapshot_id, elapsed_time

def record_to_csv(snapshot_num, elapsed_time, csv_filename):
    """Record results to CSV"""
    file_exists = os.path.exists(csv_filename)

    with open(csv_filename, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(['snapshot_number', 'elapsed_time'])
        writer.writerow([snapshot_num, elapsed_time])

def create_ami_and_measure(snapshot_id, snapshot_name):
    """Create AMI from snapshot and measure time"""
    ec2 = boto3.client('ec2')
    
    print(f"Creating AMI from snapshot {snapshot_id}...")
    start_time = time.time()
    
    ami_response = ec2.register_image(
        Name=f'{snapshot_name}-ami-{int(time.time())}',
        Architecture='x86_64',
        RootDeviceName='/dev/sda1',
        BlockDeviceMappings=[{
            'DeviceName': '/dev/sda1',
            'Ebs': {
                'SnapshotId': snapshot_id,
                'VolumeType': 'gp3'
            }
        }]
    )
    
    ami_id = ami_response['ImageId']
    
    # Wait for AMI to be available
    waiter = ec2.get_waiter('image_available')
    waiter.wait(ImageIds=[ami_id])
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print(f"AMI {ami_id} created in {elapsed_time:.2f} seconds")
    return ami_id, elapsed_time

def record_ami_to_csv(ami_id, elapsed_time, csv_filename):
    """Record AMI creation results to CSV"""
    file_exists = os.path.exists(csv_filename)

    with open(csv_filename, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(['ami_id', 'elapsed_time'])
        writer.writerow([ami_id, elapsed_time])
    """Copy snapshot to another AZ and create AMI"""
    try:
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
        
    except Exception as e:
        if "UnauthorizedOperation" in str(e) or "service control policy" in str(e).lower():
            print(f"Warning: Cross-region copy blocked by policy. Skipping AMI creation.")
            return None
        else:
            raise e

def main():
    parser = argparse.ArgumentParser(description='AWS EBS Snapshot Benchmark Tool')
    parser.add_argument('-n', '--num-snapshots', type=int, default=1, help='Number of snapshots to create (default: 1)')
    parser.add_argument('-o', '--output', default='snapshot_results.csv', help='Output CSV filename (default: snapshot_results.csv)')
    parser.add_argument('-s', '--size', type=int, default=10, help='File size in GB (default: 10)')
    parser.add_argument('-a', '--ami-csv', default='ami_results.csv', help='AMI creation CSV filename (default: ami_results.csv)')
    args = parser.parse_args()

    try:
        # Get instance info
        instance_id, volume_id = get_instance_metadata()
        current_region = boto3.Session().region_name

        last_snapshot_id = None
        last_snapshot_name = None

        # Create snapshots in loop
        for snapshot_num in range(1, args.num_snapshots + 1):
            # Step 1: Create random file for each snapshot
            filename = create_random_file(args.size)

            # Step 2 & 3: Create snapshot and measure time
            snapshot_id, elapsed_time = create_snapshot_and_measure(volume_id, snapshot_num, filename)
            
            # Store last snapshot info
            last_snapshot_id = snapshot_id
            last_snapshot_name = os.path.basename(filename).replace('.dat', '')

            # Step 4: Record to CSV
            record_to_csv(snapshot_num, elapsed_time, args.output)

        # Step 5: Create AMI from last snapshot
        if last_snapshot_id:
            ami_id, ami_elapsed_time = create_ami_and_measure(last_snapshot_id, last_snapshot_name)
            record_ami_to_csv(ami_id, ami_elapsed_time, args.ami_csv)
            print(f"AMI created: {ami_id}")

        print(f"Process completed successfully!")
        print(f"Created {args.num_snapshots} snapshots")
        print(f"Snapshot results saved to {args.output}")
        print(f"AMI results saved to {args.ami_csv}")

    except Exception as e:
        print(f"Error: {e}")
        return 1

    return 0

if __name__ == "__main__":
    exit(main())
