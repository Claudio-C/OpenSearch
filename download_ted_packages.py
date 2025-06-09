#!/usr/bin/env python3

import requests
import csv
from io import StringIO
from datetime import datetime
import os
import sys
import argparse
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def download_csv(url):
    """Download the CSV file from the given URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Failed to download CSV: {e}")
        sys.exit(1)

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
                print(f"Warning: Could not parse date {date_str}")
    
    return publications

def get_latest_available_ojs(publications):
    """Get the latest available OJS by comparing publication dates with today."""
    today = datetime.now()
    print(f"Current date: {today.strftime('%d/%m/%Y')}")
    
    # Debug each publication's comparison with today
    available_publications = []
    for ojs, date in publications:
        is_available = date <= today
        if is_available:
            available_publications.append((ojs, date))
            
    if not available_publications:
        print("No available publications found.")
        sys.exit(1)
    
    # Sort by date to get the most recent one
    latest = max(available_publications, key=lambda x: x[1])
    print(f"All available publications: {[(ojs, date.strftime('%d/%m/%Y')) for ojs, date in sorted(available_publications, key=lambda x: x[1], reverse=True)[:5]]}")
    
    return latest

def construct_download_url(ojs, year):
    """
    Construct the download URL for the given OJS and year.
    Format: https://ted.europa.eu/packages/daily/YYYYOOJJJ
    Example: For OJS 103 in 2025, the URL is https://ted.europa.eu/packages/daily/202500103
    """
    # Convert ojs to integer to remove leading zeros if present
    ojs_int = int(ojs)
    # Format the OJS part as a 5-digit number with leading zeros (00XXX)
    ojs_formatted = f"00{ojs_int:03d}"
    return f"https://ted.europa.eu/packages/daily/{year}{ojs_formatted}"

def download_package(url, output_dir):
    """Download the package from the URL and save to the output directory."""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Get file extension from .env or use default
        file_extension = os.getenv('PACKAGE_FILE_EXTENSION', '.tar.gz')
        
        # Extract the filename from the URL
        filename = url.split('/')[-1] + file_extension
        output_path = os.path.join(output_dir, filename)
        
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"Package downloaded successfully to {output_path}")
        return output_path
    except requests.exceptions.RequestException as e:
        print(f"Failed to download package: {e}")
        # Print the URL that failed for debugging
        print(f"Failed URL: {url}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Download TED packages based on publication dates")
    parser.add_argument("-y", "--year", 
                        default=os.getenv("TED_YEAR", "2025"), 
                        help=f"Year for the calendar (default: {os.getenv('TED_YEAR', '2025')})")
    parser.add_argument("-o", "--output", 
                        default=os.getenv("TED_DOWNLOAD_DIR", "/home/ia/TenderSync/OpenSearch/downloads"),
                        help=f"Output directory for downloaded packages (default: {os.getenv('TED_DOWNLOAD_DIR', '/home/ia/TenderSync/OpenSearch/downloads')})")
    
    args = parser.parse_args()
    
    # CSV URL with the year
    base_url = os.getenv("TED_CALENDAR_URL", "https://ted.europa.eu/es/release-calendar/-/download/file/CSV")
    csv_url = f"{base_url}/{args.year}"
    
    print(f"Downloading publication calendar for {args.year}...")
    csv_content = download_csv(csv_url)
    
    print("Parsing publication dates...")
    publications = parse_csv(csv_content)
    
    if not publications:
        print("No publications found in the CSV.")
        sys.exit(1)
    
    print("Finding the latest available publication...")
    latest_ojs, latest_date = get_latest_available_ojs(publications)
    print(f"Latest available OJS: {latest_ojs}, Date: {latest_date.strftime('%d/%m/%Y')}")
    
    # Add safeguard against future dates
    today = datetime.now()
    if latest_date > today:
        print(f"Warning: Selected OJS {latest_ojs} has a future date ({latest_date.strftime('%d/%m/%Y')}).")
        print("Cannot download publications from the future. Exiting.")
        sys.exit(1)
    
    # Construct download URL
    download_url = construct_download_url(latest_ojs, args.year)
    print(f"Download URL: {download_url}")
    
    # Download the package
    output_file = download_package(download_url, args.output)
    print(f"Process completed. Package saved to: {output_file}")

if __name__ == "__main__":
    main()
