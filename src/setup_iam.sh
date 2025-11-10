#!/bin/bash

# Create IAM role
aws iam create-role \
    --role-name EC2SnapshotRole \
    --assume-role-policy-document file://trust_policy.json

# Create and attach policy
aws iam create-policy \
    --policy-name EC2SnapshotPolicy \
    --policy-document file://iam_policy.json

aws iam attach-role-policy \
    --role-name EC2SnapshotRole \
    --policy-arn arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):policy/EC2SnapshotPolicy

# Create instance profile
aws iam create-instance-profile \
    --instance-profile-name EC2SnapshotProfile

aws iam add-role-to-instance-profile \
    --instance-profile-name EC2SnapshotProfile \
    --role-name EC2SnapshotRole

echo "Attach EC2SnapshotProfile to your EC2 instance"
