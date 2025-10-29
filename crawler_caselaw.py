#!/usr/bin/env python3
"""
Crawler to download all .tar files from https://static.case.law/
Recursively discovers reporter directories and downloads all tar files
"""

import requests
from bs4 import BeautifulSoup
import os
from pathlib import Path
import sys
import re
from urllib.parse import urljoin
import time


class CaseLawCrawler:
    def __init__(self, base_url="https://static.case.law/", output_dir="data/caselaw_downloads"):
        self.base_url = base_url
        self.output_dir = output_dir
        self.session = requests.Session()
        self.downloaded_count = 0
        self.failed_count = 0
        self.skipped_count = 0

    def get_page_links(self, url):
        """
        Parse HTML page and extract links
        """
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')
            links = []

            for link in soup.find_all('a', href=True):
                href = link['href']
                full_url = urljoin(url, href)
                links.append(full_url)

            return links
        except requests.RequestException as e:
            print(f"Error fetching {url}: {e}")
            return []

    def get_reporter_directories(self):
        """
        Get all reporter directory links from the main page
        """
        print("Discovering reporter directories...")
        links = self.get_page_links(self.base_url)

        # Filter for reporter directories (exclude metadata JSON files)
        reporters = []
        for link in links:
            # Reporter directories end with '/' and are not the base URL
            if link.endswith('/') and link != self.base_url:
                # Extract reporter name
                reporter_name = link.rstrip('/').split('/')[-1]
                if reporter_name and not reporter_name.endswith('.json'):
                    reporters.append((reporter_name, link))

        return reporters

    def get_tar_files(self, reporter_url):
        """
        Get all .tar file links from a reporter directory
        """
        links = self.get_page_links(reporter_url)
        tar_files = [link for link in links if link.endswith('.tar')]
        return tar_files

    def download_file(self, url, output_path):
        """
        Download a file with progress indication
        """
        try:
            response = self.session.get(url, stream=True, timeout=300)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0

            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)

                        # Print progress
                        if total_size > 0:
                            percent = (downloaded_size / total_size) * 100
                            mb_downloaded = downloaded_size / (1024 * 1024)
                            mb_total = total_size / (1024 * 1024)
                            print(f"\r  Progress: {percent:.1f}% ({mb_downloaded:.1f}/{mb_total:.1f} MB)", end='',
                                  flush=True)

            print()  # New line after progress
            return True

        except requests.RequestException as e:
            print(f"\n  ✗ Error downloading: {e}")
            if os.path.exists(output_path):
                os.remove(output_path)
            return False

    def crawl_and_download(self, limit_reporters=None):
        """
        Main crawling function
        """
        print(f"Case Law Crawler - https://static.case.law/")
        print("=" * 60)
        print(f"Output directory: {self.output_dir}/")
        print()

        # Get all reporter directories
        reporters = self.get_reporter_directories()

        if not reporters:
            print("No reporter directories found!")
            return

        print(f"Found {len(reporters)} reporter directories")

        if limit_reporters:
            reporters = reporters[:limit_reporters]
            print(f"Limiting to first {limit_reporters} reporters for testing")

        print()

        # Process each reporter
        for idx, (reporter_name, reporter_url) in enumerate(reporters, 1):
            print(f"[{idx}/{len(reporters)}] Processing: {reporter_name}")

            # Create reporter directory
            reporter_dir = os.path.join(self.output_dir, reporter_name)
            Path(reporter_dir).mkdir(parents=True, exist_ok=True)

            # Get all tar files for this reporter
            tar_files = self.get_tar_files(reporter_url)

            if not tar_files:
                print(f"  No .tar files found")
                continue

            print(f"  Found {len(tar_files)} tar file(s)")

            # Download each tar file
            for tar_idx, tar_url in enumerate(tar_files, 1):
                filename = tar_url.split('/')[-1]
                output_path = os.path.join(reporter_dir, filename)

                # Skip if already exists
                if os.path.exists(output_path):
                    print(f"  [{tar_idx}/{len(tar_files)}] Skipping {filename} (already exists)")
                    self.skipped_count += 1
                    continue

                print(f"  [{tar_idx}/{len(tar_files)}] Downloading {filename}...")

                if self.download_file(tar_url, output_path):
                    print(f"  ✓ Completed {filename}")
                    self.downloaded_count += 1
                else:
                    print(f"  ✗ Failed {filename}")
                    self.failed_count += 1

                # Small delay to be respectful
                time.sleep(0.5)

            print()

        # Final summary
        print("=" * 60)
        print("Download Complete!")
        print("=" * 60)
        print(f"Downloaded: {self.downloaded_count}")
        print(f"Skipped: {self.skipped_count}")
        print(f"Failed: {self.failed_count}")
        print(f"Output directory: {self.output_dir}/")
        print("=" * 60)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Download all .tar files from https://static.case.law/'
    )
    parser.add_argument(
        '-o', '--output',
        default='caselaw_downloads',
        help='Output directory (default: caselaw_downloads)'
    )
    parser.add_argument(
        '-l', '--limit',
        type=int,
        help='Limit number of reporters to process (for testing)'
    )
    parser.add_argument(
        '--base-url',
        default='https://static.case.law/',
        help='Base URL (default: https://static.case.law/)'
    )

    args = parser.parse_args()

    # Create crawler instance
    crawler = CaseLawCrawler(
        base_url=args.base_url,
        output_dir=args.output
    )

    # Run the crawler
    try:
        crawler.crawl_and_download(limit_reporters=args.limit)
    except KeyboardInterrupt:
        print("\n\nDownload interrupted by user")
        print(f"Downloaded so far: {crawler.downloaded_count}")
        print(f"You can resume by running the script again (existing files will be skipped)")
        sys.exit(0)


if __name__ == "__main__":
    main()