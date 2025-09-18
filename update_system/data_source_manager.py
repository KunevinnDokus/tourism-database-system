"""
Data Source Manager Module

Handles downloading and validation of TTL files from the Flemish Tourism data source.
Provides utilities for file integrity checking, temporary database management, and error handling.
"""

import os
import hashlib
import requests
import tempfile
import shutil
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, Any, List
from urllib.parse import urlparse
import logging

logger = logging.getLogger(__name__)


class DataSourceManager:
    """Manages TTL file downloads and validation from tourism data sources."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize data source manager.

        Args:
            config: Configuration dict containing URLs and settings
        """
        self.config = config
        self.download_timeout = config.get('download_timeout', 3600)
        self.max_retries = config.get('max_retries', 3)
        self.temp_dir = None
        self.current_file_path = None
        self.current_file_hash = None
        self.current_file_size = None

    def __enter__(self):
        """Context manager entry - create temporary directory."""
        self.temp_dir = tempfile.mkdtemp(prefix='tourism_ttl_')
        logger.info(f"Created temporary directory: {self.temp_dir}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup temporary directory."""
        if self.temp_dir and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
            logger.info(f"Cleaned up temporary directory: {self.temp_dir}")

    def download_latest_ttl(self, url: str = None, target_filename: str = None, save_to_downloads: bool = True) -> Tuple[str, str, int]:
        """
        Download the latest TTL file from the tourism data source.

        Args:
            url: Custom URL to download from (uses config default if None)
            target_filename: Custom filename (auto-generated if None)
            save_to_downloads: If True, save to permanent downloads folder with timestamp

        Returns:
            Tuple of (file_path, file_hash, file_size)

        Raises:
            Exception: If download fails after all retries
        """
        if not self.temp_dir:
            raise RuntimeError("DataSourceManager must be used as context manager")

        download_url = url or self.config.get('current_file_url')
        if not download_url:
            raise ValueError("No download URL specified")

        if not target_filename:
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            if save_to_downloads:
                # Use standard name for permanent downloads
                target_filename = f"toeristische-attracties_{timestamp}.ttl"
            else:
                # Use URL-based name for temporary downloads
                parsed_url = urlparse(download_url)
                base_name = os.path.basename(parsed_url.path) or "tourism_data.ttl"
                name, ext = os.path.splitext(base_name)
                target_filename = f"{name}_{timestamp}{ext}"

        # Determine target directory
        if save_to_downloads:
            # Save to permanent downloads directory
            downloads_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'downloads')
            os.makedirs(downloads_dir, exist_ok=True)
            target_path = os.path.join(downloads_dir, target_filename)
        else:
            # Save to temporary directory
            target_path = os.path.join(self.temp_dir, target_filename)

        logger.info(f"Starting download from: {download_url}")
        logger.info(f"Target file: {target_path}")

        # Attempt download with retries
        last_exception = None
        for attempt in range(self.max_retries):
            try:
                # Download with progress tracking
                response = requests.get(
                    download_url,
                    stream=True,
                    timeout=self.download_timeout,
                    headers={'User-Agent': 'Tourism-Database-Updater/1.0'}
                )
                response.raise_for_status()

                # Get file size from headers
                total_size = int(response.headers.get('content-length', 0))
                logger.info(f"Download size: {total_size:,} bytes ({total_size / 1024 / 1024:.1f} MB)")

                # Download with progress
                downloaded_size = 0
                hash_sha256 = hashlib.sha256()

                with open(target_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            hash_sha256.update(chunk)
                            downloaded_size += len(chunk)

                            # Log progress every 50MB
                            if downloaded_size % (50 * 1024 * 1024) == 0:
                                progress = (downloaded_size / total_size * 100) if total_size > 0 else 0
                                logger.info(f"Download progress: {downloaded_size:,} bytes ({progress:.1f}%)")

                # Final validation
                final_size = os.path.getsize(target_path)
                file_hash = hash_sha256.hexdigest()

                logger.info(f"Download completed: {final_size:,} bytes")
                logger.info(f"File hash (SHA256): {file_hash}")

                # Validate download
                if total_size > 0 and final_size != total_size:
                    raise RuntimeError(f"Size mismatch: expected {total_size}, got {final_size}")

                # Store results
                self.current_file_path = target_path
                self.current_file_hash = file_hash
                self.current_file_size = final_size

                return target_path, file_hash, final_size

            except Exception as e:
                last_exception = e
                logger.warning(f"Download attempt {attempt + 1} failed: {e}")

                # Clean up partial file
                if os.path.exists(target_path):
                    os.remove(target_path)

                if attempt < self.max_retries - 1:
                    logger.info(f"Retrying download in 5 seconds...")
                    import time
                    time.sleep(5)

        # All retries failed
        raise RuntimeError(f"Download failed after {self.max_retries} attempts. Last error: {last_exception}")

    def validate_ttl_file(self, file_path: str) -> Dict[str, Any]:
        """
        Validate TTL file format and basic content.

        Args:
            file_path: Path to TTL file to validate

        Returns:
            Dict with validation results and statistics

        Raises:
            Exception: If validation fails
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"TTL file not found: {file_path}")

        validation_result = {
            'file_path': file_path,
            'file_size': os.path.getsize(file_path),
            'is_valid': False,
            'triple_count': 0,
            'entity_counts': {},
            'error_message': None
        }

        try:
            # Basic file validation
            with open(file_path, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()
                if not first_line.startswith('@prefix') and not first_line.startswith('#'):
                    logger.warning("TTL file doesn't start with expected prefix or comment")

            # Try to validate with rdflib if available
            try:
                from rdflib import Graph
                g = Graph()
                g.parse(file_path, format='turtle')

                validation_result['triple_count'] = len(g)
                validation_result['is_valid'] = True

                # Count entity types
                entity_queries = {
                    'logies': 'SELECT (COUNT(?s) AS ?count) WHERE { ?s a <https://data.vlaanderen.be/ns/logies#Logies> . }',
                    'addresses': 'SELECT (COUNT(?s) AS ?count) WHERE { ?s a <http://www.w3.org/ns/locn#Address> . }',
                    'contact_points': 'SELECT (COUNT(?s) AS ?count) WHERE { ?s a <http://schema.org/ContactPoint> . }',
                    'geometries': 'SELECT (COUNT(?s) AS ?count) WHERE { ?s a <http://www.w3.org/ns/locn#Geometry> . }',
                    'identifiers': 'SELECT (COUNT(?s) AS ?count) WHERE { ?s a <http://www.w3.org/ns/adms#Identifier> . }'
                }

                for entity_type, query in entity_queries.items():
                    try:
                        results = list(g.query(query))
                        count = int(results[0][0]) if results else 0
                        validation_result['entity_counts'][entity_type] = count
                    except Exception as e:
                        logger.warning(f"Failed to count {entity_type}: {e}")
                        validation_result['entity_counts'][entity_type] = -1

                logger.info(f"TTL validation successful: {validation_result['triple_count']:,} triples")
                for entity_type, count in validation_result['entity_counts'].items():
                    if count >= 0:
                        logger.info(f"  {entity_type}: {count:,} entities")

            except ImportError:
                logger.warning("rdflib not available, performing basic validation only")
                validation_result['is_valid'] = True  # Assume valid if basic checks pass

            except Exception as e:
                validation_result['error_message'] = str(e)
                logger.error(f"TTL validation failed: {e}")
                raise

        except Exception as e:
            validation_result['error_message'] = str(e)
            validation_result['is_valid'] = False
            raise

        return validation_result

    def compare_file_metadata(self, old_hash: str = None, old_size: int = None) -> Dict[str, Any]:
        """
        Compare current file with previous version metadata.

        Args:
            old_hash: Previous file hash
            old_size: Previous file size

        Returns:
            Dict with comparison results
        """
        if not self.current_file_hash:
            raise RuntimeError("No current file loaded")

        comparison = {
            'has_changes': False,
            'hash_changed': False,
            'size_changed': False,
            'size_difference': 0,
            'current_hash': self.current_file_hash,
            'current_size': self.current_file_size,
            'previous_hash': old_hash,
            'previous_size': old_size
        }

        if old_hash and old_hash != self.current_file_hash:
            comparison['hash_changed'] = True
            comparison['has_changes'] = True

        if old_size and old_size != self.current_file_size:
            comparison['size_changed'] = True
            comparison['size_difference'] = self.current_file_size - old_size
            comparison['has_changes'] = True

        return comparison

    def copy_to_destination(self, destination_path: str) -> str:
        """
        Copy current TTL file to a permanent destination.

        Args:
            destination_path: Target path for the file

        Returns:
            str: Path to the copied file
        """
        if not self.current_file_path or not os.path.exists(self.current_file_path):
            raise RuntimeError("No current file to copy")

        # Ensure destination directory exists
        dest_dir = os.path.dirname(destination_path)
        if dest_dir:
            os.makedirs(dest_dir, exist_ok=True)

        # Copy file
        shutil.copy2(self.current_file_path, destination_path)
        logger.info(f"Copied TTL file to: {destination_path}")

        return destination_path

    def get_file_info(self) -> Dict[str, Any]:
        """
        Get information about the current file.

        Returns:
            Dict with file information
        """
        if not self.current_file_path:
            return {}

        return {
            'file_path': self.current_file_path,
            'file_hash': self.current_file_hash,
            'file_size': self.current_file_size,
            'exists': os.path.exists(self.current_file_path) if self.current_file_path else False
        }

    @staticmethod
    def calculate_file_hash(file_path: str) -> str:
        """
        Calculate SHA256 hash of a file.

        Args:
            file_path: Path to file

        Returns:
            str: SHA256 hash in hex format
        """
        hash_sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()

    @staticmethod
    def check_url_availability(url: str, timeout: int = 30) -> Dict[str, Any]:
        """
        Check if a URL is available and get basic metadata.

        Args:
            url: URL to check
            timeout: Request timeout in seconds

        Returns:
            Dict with availability information
        """
        result = {
            'url': url,
            'available': False,
            'status_code': None,
            'content_length': None,
            'content_type': None,
            'last_modified': None,
            'error_message': None
        }

        try:
            response = requests.head(
                url,
                timeout=timeout,
                headers={'User-Agent': 'Tourism-Database-Updater/1.0'}
            )

            result['available'] = response.status_code == 200
            result['status_code'] = response.status_code
            result['content_length'] = response.headers.get('content-length')
            result['content_type'] = response.headers.get('content-type')
            result['last_modified'] = response.headers.get('last-modified')

            if result['content_length']:
                result['content_length'] = int(result['content_length'])

        except Exception as e:
            result['error_message'] = str(e)

        return result

    @staticmethod
    def get_downloads_directory() -> str:
        """Get the path to the downloads directory."""
        project_root = os.path.dirname(os.path.dirname(__file__))
        downloads_dir = os.path.join(project_root, 'downloads')
        os.makedirs(downloads_dir, exist_ok=True)
        return downloads_dir

    @staticmethod
    def list_downloaded_files() -> List[Dict[str, Any]]:
        """
        List all downloaded TTL files in the downloads directory.

        Returns:
            List of file information dictionaries sorted by date (newest first)
        """
        downloads_dir = DataSourceManager.get_downloads_directory()
        files = []

        for filename in os.listdir(downloads_dir):
            if filename.endswith('.ttl') and 'toeristische-attracties' in filename:
                filepath = os.path.join(downloads_dir, filename)
                if os.path.isfile(filepath):
                    stat = os.stat(filepath)

                    # Extract timestamp from filename
                    timestamp_str = None
                    if '_' in filename:
                        parts = filename.split('_')
                        if len(parts) >= 2:
                            timestamp_part = parts[1].replace('.ttl', '')
                            try:
                                # Parse timestamp from filename (YYYYMMDD-HHMMSS)
                                timestamp_str = timestamp_part
                                file_datetime = datetime.strptime(timestamp_part, '%Y%m%d-%H%M%S')
                            except ValueError:
                                file_datetime = datetime.fromtimestamp(stat.st_mtime)
                    else:
                        file_datetime = datetime.fromtimestamp(stat.st_mtime)

                    files.append({
                        'filename': filename,
                        'filepath': filepath,
                        'size_bytes': stat.st_size,
                        'size_mb': stat.st_size / (1024 * 1024),
                        'modified_time': datetime.fromtimestamp(stat.st_mtime),
                        'download_time': file_datetime,
                        'timestamp_str': timestamp_str,
                        'file_hash': DataSourceManager.calculate_file_hash(filepath)
                    })

        # Sort by download time (newest first)
        files.sort(key=lambda x: x['download_time'], reverse=True)
        return files

    @staticmethod
    def get_latest_downloaded_file() -> Optional[Dict[str, Any]]:
        """
        Get information about the most recently downloaded file.

        Returns:
            File info dictionary or None if no files found
        """
        files = DataSourceManager.list_downloaded_files()
        return files[0] if files else None

    @staticmethod
    def cleanup_old_downloads(days_to_keep: int = 30) -> int:
        """
        Remove downloaded files older than specified days.

        Args:
            days_to_keep: Number of days to keep files (default: 30)

        Returns:
            Number of files removed
        """
        downloads_dir = DataSourceManager.get_downloads_directory()
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        removed_count = 0

        for filename in os.listdir(downloads_dir):
            if filename.endswith('.ttl') and 'toeristische-attracties' in filename:
                filepath = os.path.join(downloads_dir, filename)
                if os.path.isfile(filepath):
                    file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                    if file_time < cutoff_date:
                        os.remove(filepath)
                        removed_count += 1
                        logger.info(f"Removed old download: {filename}")

        return removed_count

    def get_downloads_summary(self) -> Dict[str, Any]:
        """
        Get summary information about downloaded files.

        Returns:
            Dictionary with download statistics
        """
        files = self.list_downloaded_files()

        if not files:
            return {
                'total_files': 0,
                'total_size_mb': 0,
                'latest_download': None,
                'oldest_download': None
            }

        total_size = sum(f['size_bytes'] for f in files)

        return {
            'total_files': len(files),
            'total_size_mb': total_size / (1024 * 1024),
            'latest_download': files[0]['download_time'].isoformat(),
            'oldest_download': files[-1]['download_time'].isoformat(),
            'files': [
                {
                    'filename': f['filename'],
                    'size_mb': f['size_mb'],
                    'download_time': f['download_time'].isoformat()
                }
                for f in files[:5]  # Show latest 5 files
            ]
        }