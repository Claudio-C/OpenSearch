#!/usr/bin/env python3

import os
import sys
import requests
import csv
from io import StringIO
from datetime import datetime, timedelta
import argparse
import subprocess
import logging
import time
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("batch_download_index.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def download_csv(url):
    """Download the CSV file from the given URL."""
    try:
        logger.info(f"Downloading CSV from {url}")
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download CSV: {e}")
        return None

def parse_csv(csv_content):
    """Parse the CSV content and return a list of (OJS, date) tuples."""
    publications = []
    csv_reader = csv.reader(StringIO(csv_content))
    
    # Skip header row
    next(csv_reader, None)
    
    for row in csv_reader:
        if len(row) >= 2:
            ojs = row[0].strip()
            date_str = row[1].strip()
            try:
                # Parse date in the format D/M/YYYY (European format)
                date = datetime.strptime(date_str, "%d/%m/%Y")
                publications.append((ojs, date))
            except ValueError:
                logger.warning(f"Could not parse date {date_str}")
    
    return publications

def get_available_ojs_for_year(year):
    """Get all available OJS numbers for a specific year."""
    base_url = os.getenv("TED_CALENDAR_URL", "https://ted.europa.eu/es/release-calendar/-/download/file/CSV")
    csv_url = f"{base_url}/{year}"
    
    csv_content = download_csv(csv_url)
    if not csv_content:
        return []
    
    publications = parse_csv(csv_content)
    
    # Filter only publications up to today
    today = datetime.now()
    available_publications = [(ojs, date) for ojs, date in publications if date <= today]
    
    # Sort by date
    sorted_publications = sorted(available_publications, key=lambda x: x[1])
    
    logger.info(f"Found {len(sorted_publications)} available publications for year {year}")
    return sorted_publications

def download_and_index_ojs(year, ojs, date):
    """Download and index a specific OJS package."""
    # Format the OJS part as a 5-digit number with leading zeros (00XXX)
    ojs_int = int(ojs)
    ojs_formatted = f"00{ojs_int:03d}"
    download_url = f"https://ted.europa.eu/packages/daily/{year}{ojs_formatted}"
    
    # Construct the output path
    download_dir = os.getenv("TED_DOWNLOAD_DIR", "/home/ia/TenderSync/OpenSearch/downloads")
    file_extension = os.getenv("PACKAGE_FILE_EXTENSION", ".tar.gz")
    output_filename = f"{year}{ojs_formatted}{file_extension}"
    output_path = os.path.join(download_dir, output_filename)
    
    # Ensure download directory exists
    os.makedirs(download_dir, exist_ok=True)
    
    # Download the package
    logger.info(f"Downloading OJS {ojs} for {date.strftime('%d/%m/%Y')} from {download_url}")
    try:
        response = requests.get(download_url, stream=True)
        response.raise_for_status()
        
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info(f"Download successful: {output_path}")
        
        # Index the downloaded package
        index_package(output_path)
        
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download package: {e}")
        return False

def index_package(package_path):
    """Index a downloaded package using the index_ted_packages.py script."""
    url = os.getenv("OPENSEARCH_URL", "http://localhost:9200")
    index = os.getenv("OPENSEARCH_INDEX", "ted_dev")
    bulk_size = os.getenv("BULK_SIZE", "100")
    workers = os.getenv("NUM_WORKERS", "10")
    username = os.getenv("OPENSEARCH_USERNAME", "")
    password = os.getenv("OPENSEARCH_PASSWORD", "")
    
    cmd = [
        sys.executable,
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "index_ted_packages.py"),
        package_path,
        "-u", url,
        "-i", index,
        "-b", bulk_size,
        "-w", workers
    ]
    
    if username:
        cmd.extend(["--username", username])
    if password:
        cmd.extend(["--password", password])
    
    logger.info(f"Indexing package: {package_path}")
    
    try:
        subprocess.run(cmd, check=True)
        logger.info(f"Successfully indexed package: {package_path}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error indexing package {package_path}: {e}")
        return False

def process_year_range(start_year, end_year, skip_existing=False):
    """Process a range of years, downloading and indexing all available publications."""
    download_dir = os.getenv("TED_DOWNLOAD_DIR", "/home/ia/TenderSync/OpenSearch/downloads")
    stats = {
        "total": 0,
        "downloaded": 0,
        "indexed": 0,
        "failed": 0,
        "skipped": 0
    }
    
    # Create a tracking file to record processed files
    tracking_file = os.path.join(download_dir, "processed_publications.txt")
    processed = set()
    
    # Load already processed files if the tracking file exists
    if os.path.exists(tracking_file):
        with open(tracking_file, "r") as f:
            processed = set(line.strip() for line in f)
    
    # Process each year
    for year in range(start_year, end_year + 1):
        logger.info(f"Processing year {year}")
        publications = get_available_ojs_for_year(str(year))
        
        for ojs, date in publications:
            package_id = f"{year}-{ojs}"
            stats["total"] += 1
            
            # Skip if already processed
            if skip_existing and package_id in processed:
                logger.info(f"Skipping already processed publication: {package_id}")
                stats["skipped"] += 1
                continue
            
            # Download and index
            success = download_and_index_ojs(str(year), ojs, date)
            
            if success:
                stats["downloaded"] += 1
                stats["indexed"] += 1
                
                # Mark as processed
                with open(tracking_file, "a") as f:
                    f.write(f"{package_id}\n")
                processed.add(package_id)
            else:
                stats["failed"] += 1
            
            # Add a small delay between requests to be nice to the server
            time.sleep(2)
    
    # Print statistics
    logger.info("Batch processing completed")
    logger.info(f"Total publications: {stats['total']}")
    logger.info(f"Successfully downloaded and indexed: {stats['indexed']}")
    logger.info(f"Failed: {stats['failed']}")
    logger.info(f"Skipped (already processed): {stats['skipped']}")
    
    return stats

def main():
    current_year = datetime.now().year
    
    parser = argparse.ArgumentParser(description="Download and index TED packages from a range of years")
    parser.add_argument("--start-year", type=int, default=2015, 
                        help="Starting year (default: 2015)")
    parser.add_argument("--end-year", type=int, default=current_year, 
                        help=f"Ending year (default: current year {current_year})")
    parser.add_argument("--skip-existing", action="store_true", 
                        help="Skip already processed publications")
    
    args = parser.parse_args()
    
    if args.start_year > args.end_year:
        logger.error("Start year cannot be greater than end year")
        sys.exit(1)
    
    if args.end_year > current_year:
        logger.warning(f"End year is in the future, using current year {current_year} instead")
        args.end_year = current_year
    
    logger.info(f"Starting batch download and indexing from {args.start_year} to {args.end_year}")
    
    # Process the year range
    stats = process_year_range(args.start_year, args.end_year, args.skip_existing)
    
    if stats["failed"] > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()
