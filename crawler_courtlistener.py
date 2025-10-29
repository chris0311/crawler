#!/usr/bin/env python3
"""
Script to download all .bz2 files from CourtListener S3 bucket
Dynamically discovers files by parsing the S3 bucket listing
"""

import requests
import re
import os
from pathlib import Path
import sys
from urllib.parse import unquote


def get_s3_bucket_listing(bucket_url):
    """
    Fetch and parse S3 bucket XML listing to get all files
    """
    print(f"Fetching bucket listing from {bucket_url}...")

    try:
        # Try to get the XML listing directly from S3 API
        response = requests.get(bucket_url, params={'prefix': 'bulk-data/'}, timeout=30)
        response.raise_for_status()

        # Parse XML to extract all keys (file paths)
        # S3 returns XML with <Key> elements containing file paths
        keys = re.findall(r'<Key>([^<]+)</Key>', response.text)

        # Filter for .bz2 files only
        bz2_files = [key for key in keys if key.endswith('.bz2')]

        return bz2_files

    except requests.RequestException as e:
        print(f"Error fetching bucket listing: {e}")
        return None


def get_files_from_html_listing(list_url):
    """
    Parse the HTML listing page that uses JavaScript to load files
    We'll extract the bucket info and make direct API calls
    """
    print(f"Attempting to fetch from HTML listing page...")

    try:
        response = requests.get(list_url, timeout=30)
        response.raise_for_status()

        # Extract bucket name and prefix from the JavaScript variables
        bucket_match = re.search(r"BUCKET_NAME\s*=\s*['\"]([^'\"]+)['\"]", response.text)

        if bucket_match:
            bucket_name = bucket_match.group(1)
            print(f"Found bucket: {bucket_name}")

            # Build direct S3 API URL
            s3_url = f"https://{bucket_name}.s3-us-west-2.amazonaws.com/"

            # Try to list files using S3 API v2
            return get_s3_bucket_listing(s3_url)
        else:
            print("Could not extract bucket name from HTML")
            return None

    except requests.RequestException as e:
        print(f"Error fetching HTML listing: {e}")
        return None


def get_files_alternative_method(base_url):
    """
    Alternative method: Try common S3 listing endpoints
    """
    print("Trying alternative discovery methods...")

    endpoints = [
        f"{base_url}?list-type=2&prefix=bulk-data/",
        f"{base_url}?prefix=bulk-data/",
        f"{base_url}?delimiter=/&prefix=bulk-data/",
    ]

    for endpoint in endpoints:
        try:
            print(f"Trying: {endpoint}")
            response = requests.get(endpoint, timeout=30)

            if response.status_code == 200:
                # Look for Key elements in XML
                keys = re.findall(r'<Key>([^<]+)</Key>', response.text)
                bz2_files = [key for key in keys if key.endswith('.bz2')]

                if bz2_files:
                    print(f"✓ Found {len(bz2_files)} files using this endpoint")
                    return bz2_files
        except:
            continue

    return None


def download_file(filename, base_url, output_dir):
    """
    Download a single file with progress indication
    """
    url = f"{base_url}{filename}"
    filepath = os.path.join(output_dir, os.path.basename(filename))

    # Skip if file already exists
    if os.path.exists(filepath):
        print(f"Skipping {os.path.basename(filename)} (already exists)")
        return True

    print(f"Downloading {os.path.basename(filename)}...")
    try:
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))
        downloaded_size = 0

        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                if chunk:
                    f.write(chunk)
                    downloaded_size += len(chunk)

                    # Print progress
                    if total_size > 0:
                        percent = (downloaded_size / total_size) * 100
                        mb_downloaded = downloaded_size / (1024 * 1024)
                        mb_total = total_size / (1024 * 1024)
                        print(f"\rProgress: {percent:.1f}% ({mb_downloaded:.1f}/{mb_total:.1f} MB)", end='', flush=True)

        print(f"\n✓ Completed {os.path.basename(filename)}")
        return True

    except requests.RequestException as e:
        print(f"\n✗ Error downloading {os.path.basename(filename)}: {e}")
        # Remove partial file if it exists
        if os.path.exists(filepath):
            os.remove(filepath)
        return False


def main():
    # CourtListener bucket information
    list_url = "https://com-courtlistener-storage.s3-us-west-2.amazonaws.com/list.html?prefix=bulk-data/"
    base_url = "https://storage.courtlistener.com/bulk-data/"
    s3_base_url = "https://com-courtlistener-storage.s3-us-west-2.amazonaws.com/"

    # Create output directory
    output_dir = "courtlistener_downloads"
    Path(output_dir).mkdir(exist_ok=True)
    print(f"Files will be saved to: {output_dir}/\n")

    # Try multiple methods to discover files
    bz2_files = None

    # Method 1: Parse HTML listing page
    bz2_files = get_files_from_html_listing(list_url)

    # Method 2: Try direct S3 API endpoints
    if not bz2_files:
        bz2_files = get_files_alternative_method(s3_base_url)

    # Method 3: Try the storage.courtlistener.com endpoint
    if not bz2_files:
        print("\nTrying storage.courtlistener.com endpoint...")
        bz2_files = get_s3_bucket_listing(base_url)

    if not bz2_files:
        print("\n" + "=" * 60)
        print("ERROR: Could not discover files automatically.")
        print("=" * 60)
        print("\nThis may be due to:")
        print("1. Network restrictions/proxy blocking S3 access")
        print("2. Changes in the bucket configuration")
        print("3. The bucket requiring authentication")
        print("\nAlternative methods to download:")
        print("1. Use AWS CLI directly:")
        print("   aws s3 sync s3://com-courtlistener-storage/bulk-data/ ./courtlistener_downloads/ --no-sign-request")
        print("\n2. Visit the CourtListener bulk data page:")
        print("   https://www.courtlistener.com/help/api/bulk-data/")
        print("\n3. Check if the files are listed at:")
        print("   https://storage.courtlistener.com/bulk-data/")
        sys.exit(1)

    print(f"\n{'-' * 60}")
    print(f"Found {len(bz2_files)} .bz2 files")
    print(f"{'-' * 60}")

    # Display first few files
    print("\nFirst 10 files:")
    for i, filename in enumerate(bz2_files[:10], 1):
        print(f"  {i}. {filename}")
    if len(bz2_files) > 10:
        print(f"  ... and {len(bz2_files) - 10} more files")

    print(f"\n{'-' * 60}")
    response = input("Do you want to download all these files? (y/n): ")
    if response.lower() != 'y':
        print("Download cancelled.")
        sys.exit(0)

    # Download each file
    successful = 0
    failed = 0

    for i, filename in enumerate(bz2_files, 1):
        print(f"\n[{i}/{len(bz2_files)}]")
        if download_file(filename, s3_base_url, output_dir):
            successful += 1
        else:
            failed += 1

    # Summary
    print("\n" + "=" * 60)
    print(f"Download complete!")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Output directory: {output_dir}/")
    print("=" * 60)


if __name__ == "__main__":
    main()