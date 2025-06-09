#!/usr/bin/env python3

import os
import sys
import subprocess
import logging
import argparse
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def run_download_script(year=None, output_dir=None):
    """Run the TED package download script."""
    logger.info("Downloading TED package...")
    
    # Use provided values or fall back to .env values
    year = year or os.getenv("TED_YEAR", "2025")
    output_dir = output_dir or os.getenv("TED_DOWNLOAD_DIR", "/home/ia/TenderSync/OpenSearch/downloads")
    
    # Build the command
    cmd = [
        sys.executable,
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "download_ted_packages.py"),
        "-y", year,
        "-o", output_dir
    ]
    
    logger.info(f"Running command: {' '.join(cmd)}")
    
    try:
        # Run the command and capture output
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(result.stdout)
        
        # Parse the output to get the path of the downloaded file
        for line in result.stdout.split('\n'):
            if "Package downloaded successfully to" in line:
                downloaded_file = line.split("to")[-1].strip()
                return downloaded_file
        
        logger.error("Could not find downloaded file path in output")
        return None
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running download script: {e}")
        logger.error(f"Error output: {e.stderr}")
        return None

def run_index_script(package_path, url=None, index=None, bulk_size=None, workers=None, username=None, password=None):
    """Run the TED package indexing script."""
    logger.info(f"Indexing TED package: {package_path}")
    
    # Use provided values or fall back to .env values
    url = url or os.getenv("OPENSEARCH_URL", "http://localhost:9200")
    index = index or os.getenv("OPENSEARCH_INDEX", "ted")
    bulk_size = bulk_size or os.getenv("BULK_SIZE", "100")
    workers = workers or os.getenv("NUM_WORKERS", "10")
    username = username or os.getenv("OPENSEARCH_USERNAME", "")
    password = password or os.getenv("OPENSEARCH_PASSWORD", "")
    
    # Build the command
    cmd = [
        sys.executable,
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "index_ted_packages.py"),
        package_path,
        "-u", url,
        "-i", index,
        "-b", bulk_size,
        "-w", workers
    ]
    
    # Add credentials if provided
    if username:
        cmd.extend(["--username", username])
    if password:
        cmd.extend(["--password", password])
    
    logger.info(f"Running command: {' '.join(cmd)}")
    
    try:
        # Run the command
        subprocess.run(cmd, check=True)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running index script: {e}")
        if e.stderr:
            logger.error(f"Error output: {e.stderr}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Download and index TED packages")
    parser.add_argument("-y", "--year", help="Year for the calendar")
    parser.add_argument("-o", "--output", help="Output directory for downloaded packages")
    parser.add_argument("-u", "--url", help="OpenSearch URL")
    parser.add_argument("-i", "--index", help="OpenSearch index name")
    parser.add_argument("-b", "--bulk-size", help="Number of documents to index in each bulk request")
    parser.add_argument("-w", "--workers", help="Number of parallel workers")
    parser.add_argument("--username", help="OpenSearch username")
    parser.add_argument("--password", help="OpenSearch password")
    parser.add_argument("--skip-download", action="store_true", help="Skip download and use existing package file")
    parser.add_argument("--package-path", help="Path to existing package file (for use with --skip-download)")
    
    args = parser.parse_args()
    
    package_path = None
    
    # Download or use existing package
    if args.skip_download:
        if not args.package_path:
            logger.error("--package-path must be provided when using --skip-download")
            sys.exit(1)
        package_path = args.package_path
    else:
        package_path = run_download_script(args.year, args.output)
        if not package_path:
            logger.error("Failed to download TED package")
            sys.exit(1)
    
    # Index the package
    success = run_index_script(
        package_path,
        args.url,
        args.index,
        args.bulk_size,
        args.workers,
        args.username,
        args.password
    )
    
    if not success:
        logger.error("Failed to index TED package")
        sys.exit(1)
    
    logger.info("Download and indexing completed successfully")

if __name__ == "__main__":
    main()
