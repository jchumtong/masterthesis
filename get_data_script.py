#!/usr/bin/env python3

import boto3
import os
import tarfile

# Initialize Boto3 client for S3
s3_client = boto3.client('s3')

# Define the bucket name
bucket_name = 'receiptsproduction'

# Define the local directory where you want to store the receipts and extracted files
local_directory = 'local_receipts'

# Create the local directory if it doesn't exist
if not os.path.exists(local_directory):
    os.makedirs(local_directory)


# Function to download and extract receipts from a specific folder in S3
def download_and_extract_receipts_from_folder(folder_name):
    # List objects in the folder
    objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)

    # Check if there are any objects in the folder
    if 'Contents' in objects:
        # Iterate through the objects and download each receipt
        for obj in objects['Contents']:
            key = obj['Key']
            # Extract filename from the key
            filename = key.split('/')[-1]
            # Download the object
            local_path = os.path.join(local_directory, filename)
            s3_client.download_file(bucket_name, key, local_path)
            print(f"Downloaded {filename}")

            # Extract the tar.gz file
            with tarfile.open(local_path, "r:gz") as tar:
                tar.extractall(path=local_directory)
            print(f"Extracted {filename}")


# Iterate through the folders in the S3 bucket
for folder in range(9987):  # Assuming folders are labeled from 0000 to 9986
    folder_name = f"{folder:04d}/01/"
    download_and_extract_receipts_from_folder(folder_name)
