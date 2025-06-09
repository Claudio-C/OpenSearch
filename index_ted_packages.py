#!/usr/bin/env python3

import os
import sys
import zipfile
import tarfile
import argparse
import json
import xml.etree.ElementTree as ET
import concurrent.futures
from pathlib import Path
import requests
from tqdm import tqdm
import logging
import tempfile
import shutil
import time
import re
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

def extract_package(package_path, output_dir):
    """Extract TED package to the specified directory, handling both zip and tar.gz formats."""
    logger.info(f"Extracting {package_path} to {output_dir}")
    try:
        # Check file extension or try to determine file type
        if package_path.endswith('.zip'):
            # Handle zip files
            with zipfile.ZipFile(package_path, 'r') as zip_ref:
                zip_ref.extractall(output_dir)
        elif package_path.endswith('.tar.gz') or package_path.endswith('.tgz'):
            # Handle tar.gz files
            with tarfile.open(package_path, 'r:gz') as tar_ref:
                tar_ref.extractall(output_dir)
        else:
            # Try to guess the format
            try:
                # Try as tar.gz first
                with tarfile.open(package_path, 'r:gz') as tar_ref:
                    tar_ref.extractall(output_dir)
            except tarfile.ReadError:
                # Try as zip
                with zipfile.ZipFile(package_path, 'r') as zip_ref:
                    zip_ref.extractall(output_dir)
        
        return True
    except zipfile.BadZipFile:
        logger.error(f"Bad zip file: {package_path}")
        return False
    except tarfile.ReadError:
        logger.error(f"Bad tar.gz file: {package_path}")
        return False
    except Exception as e:
        logger.error(f"Error extracting file {package_path}: {e}")
        return False

def find_xml_files(directory):
    """Find all XML files in the given directory."""
    xml_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".xml"):
                xml_files.append(os.path.join(root, file))
    
    logger.info(f"Found {len(xml_files)} XML files")
    return xml_files

def xml_to_dict(xml_file):
    """Parse XML file and convert to a dictionary."""
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        # Remove namespace prefixes for easier data access
        for elem in root.iter():
            if '}' in elem.tag:
                elem.tag = elem.tag.split('}', 1)[1]
        
        # Function to convert Element to dict
        def element_to_dict(element):
            result = {}
            
            # Add attributes
            if element.attrib:
                result.update(element.attrib)
            
            # Add text content if it exists and is not just whitespace
            if element.text and element.text.strip():
                result['text'] = element.text.strip()
            
            # Add children
            for child in element:
                child_dict = element_to_dict(child)
                if child.tag in result:
                    if not isinstance(result[child.tag], list):
                        result[child.tag] = [result[child.tag]]
                    result[child.tag].append(child_dict)
                else:
                    result[child.tag] = child_dict
            
            return result
        
        # Include the filename as part of the document
        result = element_to_dict(root)
        result['_filename'] = os.path.basename(xml_file)
        result['_filepath'] = xml_file
        
        return result
    except ET.ParseError as e:
        logger.error(f"Error parsing XML file {xml_file}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error processing {xml_file}: {e}")
        return None

def process_xml_file(xml_file):
    """Process a single XML file."""
    doc = xml_to_dict(xml_file)
    if doc:
        # Create a unique ID based on the filename
        doc_id = os.path.splitext(os.path.basename(xml_file))[0]
        return {"_id": doc_id, "_source": doc}
    return None

def chunk_list(lst, chunk_size):
    """Split a list into chunks of the specified size."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def bulk_index(opensearch_url, index_name, docs, username=None, password=None):
    """Index multiple documents in bulk."""
    if not docs:
        return {"errors": False, "items": []}
    
    auth = None
    if username and password:
        auth = (username, password)
    
    bulk_data = []
    for doc in docs:
        bulk_data.append(json.dumps({"index": {"_index": index_name, "_id": doc["_id"]}}))
        bulk_data.append(json.dumps(doc["_source"]))
    
    bulk_body = "\n".join(bulk_data) + "\n"
    
    headers = {"Content-Type": "application/x-ndjson"}
    
    try:
        response = requests.post(
            f"{opensearch_url}/_bulk",
            headers=headers,
            data=bulk_body,
            auth=auth
        )
        
        if response.status_code >= 200 and response.status_code < 300:
            result = response.json()
            if result.get("errors", False):
                errors = [item for item in result.get("items", []) if item.get("index", {}).get("error")]
                logger.error(f"Bulk indexing had errors: {errors[:3]}...")
            return result
        else:
            logger.error(f"Failed to bulk index documents: {response.text}")
            return {"errors": True, "items": []}
    except Exception as e:
        logger.error(f"Error during bulk indexing: {e}")
        return {"errors": True, "items": []}

def create_index_if_not_exists(opensearch_url, index_name, username=None, password=None):
    """Create the index if it doesn't exist."""
    auth = None
    if username and password:
        auth = (username, password)
    
    # Get field limit from environment or use a much higher default
    field_limit = int(os.getenv("OPENSEARCH_FIELD_LIMIT", "30000"))
    
    try:
        # Check if index exists
        response = requests.head(
            f"{opensearch_url}/{index_name}",
            auth=auth
        )
        
        if response.status_code == 404:
            # Create the index with enhanced settings
            settings = {
                "settings": {
                    "number_of_shards": 5,
                    "number_of_replicas": 1,
                    "index.mapping.total_fields.limit": field_limit,  # Increased from 10000
                    "index.mapping.nested_fields.limit": 2000,  # Also increase nested fields limit
                    "index.mapping.nested_objects.limit": 20000  # Maximum number of nested JSON objects 
                },
                "mappings": {
                    "dynamic": True  # Allow all fields to be indexed dynamically
                }
            }
            
            create_response = requests.put(
                f"{opensearch_url}/{index_name}",
                json=settings,
                auth=auth,
                headers={"Content-Type": "application/json"}
            )
            
            if create_response.status_code >= 200 and create_response.status_code < 300:
                logger.info(f"Created index {index_name} with field limit {field_limit}")
            else:
                logger.error(f"Failed to create index {index_name}: {create_response.text}")
        elif response.status_code == 200:
            # Check if we need to update the existing index settings
            settings_response = requests.get(
                f"{opensearch_url}/{index_name}/_settings",
                auth=auth
            )
            
            if settings_response.status_code >= 200 and settings_response.status_code < 300:
                settings_data = settings_response.json()
                try:
                    # Try to get current field limit
                    current_limit = settings_data.get(index_name, {}).get("settings", {}).get("index", {}).get("mapping", {}).get("total_fields", {}).get("limit")
                    
                    if not current_limit or int(current_limit) < field_limit:
                        # Close the index before updating settings
                        close_response = requests.post(
                            f"{opensearch_url}/{index_name}/_close",
                            auth=auth
                        )
                        
                        if close_response.status_code >= 200 and close_response.status_code < 300:
                            logger.info(f"Closed index {index_name} to update settings")
                            
                            # Update the field limit
                            update_response = requests.put(
                                f"{opensearch_url}/{index_name}/_settings",
                                json={
                                    "index.mapping.total_fields.limit": field_limit,
                                    "index.mapping.nested_fields.limit": 2000,
                                    "index.mapping.nested_objects.limit": 20000
                                },
                                auth=auth,
                                headers={"Content-Type": "application/json"}
                            )
                            
                            if update_response.status_code >= 200 and update_response.status_code < 300:
                                logger.info(f"Updated index {index_name} to increase field limit to {field_limit}")
                            else:
                                logger.warning(f"Could not update field limit for existing index: {update_response.text}")
                            
                            # Reopen the index
                            open_response = requests.post(
                                f"{opensearch_url}/{index_name}/_open",
                                auth=auth
                            )
                            
                            if open_response.status_code >= 200 and open_response.status_code < 300:
                                logger.info(f"Reopened index {index_name}")
                            else:
                                logger.error(f"Failed to reopen index {index_name}: {open_response.text}")
                        else:
                            logger.error(f"Failed to close index {index_name} for updates: {close_response.text}")
                except Exception as e:
                    logger.warning(f"Error checking field limits: {e}")
            
            logger.info(f"Index {index_name} already exists")
        else:
            logger.error(f"Error checking index {index_name}: {response.status_code}")
    
    except Exception as e:
        logger.error(f"Error creating index: {e}")

def index_xml_files(xml_files, opensearch_url, index_name, bulk_size=100, num_workers=4, username=None, password=None):
    """Index XML files in parallel using multiple workers."""
    create_index_if_not_exists(opensearch_url, index_name, username, password)
    
    # Process XML files in parallel
    processed_docs = []
    success_count = 0
    error_count = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(process_xml_file, xml_file): xml_file for xml_file in xml_files}
        
        with tqdm(total=len(xml_files), desc="Processing XML files") as pbar:
            for future in concurrent.futures.as_completed(futures):
                xml_file = futures[future]
                try:
                    result = future.result()
                    if result:
                        processed_docs.append(result)
                        success_count += 1
                    else:
                        error_count += 1
                except Exception as e:
                    logger.error(f"Error processing {xml_file}: {e}")
                    error_count += 1
                
                pbar.update(1)
                
                # If we have enough documents, index them in bulk
                if len(processed_docs) >= bulk_size:
                    bulk_index(opensearch_url, index_name, processed_docs, username, password)
                    processed_docs = []
    
    # Index any remaining documents
    if processed_docs:
        bulk_index(opensearch_url, index_name, processed_docs, username, password)
    
    logger.info(f"Indexing complete. Success: {success_count}, Errors: {error_count}")

def process_package(package_path, opensearch_url, index_name, bulk_size=100, num_workers=4, username=None, password=None):
    """Process a TED package file: extract, parse XML files, and index into OpenSearch."""
    # Create a temporary directory for extraction
    with tempfile.TemporaryDirectory() as temp_dir:
        logger.info(f"Created temporary directory: {temp_dir}")
        
        # Extract the package
        if not extract_package(package_path, temp_dir):
            logger.error(f"Failed to extract package {package_path}")
            return False
        
        # Find XML files
        xml_files = find_xml_files(temp_dir)
        if not xml_files:
            logger.warning(f"No XML files found in {package_path}")
            return False
        
        # Index the XML files
        index_xml_files(xml_files, opensearch_url, index_name, bulk_size, num_workers, username, password)
        
        logger.info(f"Successfully processed package {package_path}")
        return True

def main():
    parser = argparse.ArgumentParser(description="Index TED packages into OpenSearch")
    parser.add_argument("package_path", help="Path to the downloaded TED package file")
    parser.add_argument("-u", "--url", 
                        default=os.getenv("OPENSEARCH_URL", "http://localhost:9200"), 
                        help=f"OpenSearch URL (default: {os.getenv('OPENSEARCH_URL', 'http://localhost:9200')})")
    parser.add_argument("-i", "--index", 
                        default=os.getenv("OPENSEARCH_INDEX", "ted"), 
                        help=f"OpenSearch index name (default: {os.getenv('OPENSEARCH_INDEX', 'ted')})")
    parser.add_argument("-b", "--bulk-size", 
                        type=int, 
                        default=int(os.getenv("BULK_SIZE", "100")), 
                        help=f"Number of documents to index in each bulk request (default: {os.getenv('BULK_SIZE', '100')})")
    parser.add_argument("-w", "--workers", 
                        type=int, 
                        default=int(os.getenv("NUM_WORKERS", "10")), 
                        help=f"Number of parallel workers (default: {os.getenv('NUM_WORKERS', '10')})")
    parser.add_argument("--username", 
                        default=os.getenv("OPENSEARCH_USERNAME", ""), 
                        help="OpenSearch username")
    parser.add_argument("--password", 
                        default=os.getenv("OPENSEARCH_PASSWORD", ""), 
                        help="OpenSearch password")
    
    args = parser.parse_args()
    
    # Check if the package file exists
    if not os.path.isfile(args.package_path):
        logger.error(f"Package file not found: {args.package_path}")
        sys.exit(1)
    
    # Process the package
    success = process_package(
        args.package_path,
        args.url,
        args.index,
        args.bulk_size,
        args.workers,
        args.username if args.username else None,
        args.password if args.password else None
    )
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()
