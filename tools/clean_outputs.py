#!/usr/bin/env python3
"""
Clean Outputs Script

Safely deletes generated artifacts from the outputs/ directory.
Only removes CSV, JSON, and TMP files - never deletes source code.

Usage:
    python tools/clean_outputs.py --dry-run          # Preview what would be deleted
    python tools/clean_outputs.py --keep-cache        # Clean but keep cache_serper.json
    python tools/clean_outputs.py                    # Clean everything
"""

import argparse
import logging
import os
from pathlib import Path
from typing import List, Set

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Output directory
OUTPUT_DIR = Path('outputs')

# File extensions to clean
CLEAN_EXTENSIONS = {'.csv', '.json', '.tmp'}

# Protected files (never delete)
PROTECTED_FILES = {
    'cache_serper.json'  # Can be kept with --keep-cache flag
}


def find_files_to_clean(output_dir: Path, keep_cache: bool = False) -> List[Path]:
    """
    Find all files that should be cleaned.
    
    Args:
        output_dir: Path to outputs directory
        keep_cache: If True, exclude cache_serper.json from deletion
        
    Returns:
        List of file paths to delete
    """
    files_to_clean = []
    
    if not output_dir.exists():
        logger.warning(f"Output directory does not exist: {output_dir}")
        return files_to_clean
    
    for file_path in output_dir.iterdir():
        # Only process files (not directories)
        if not file_path.is_file():
            continue
        
        # Check extension
        if file_path.suffix.lower() not in CLEAN_EXTENSIONS:
            continue
        
        # Check if protected
        if file_path.name in PROTECTED_FILES:
            if keep_cache and file_path.name == 'cache_serper.json':
                logger.info(f"  KEEP (cache): {file_path.name}")
                continue
        
        files_to_clean.append(file_path)
    
    return sorted(files_to_clean)


def clean_outputs(dry_run: bool = False, keep_cache: bool = False):
    """
    Clean generated artifacts from outputs directory.
    
    Args:
        dry_run: If True, only print what would be deleted
        keep_cache: If True, keep cache_serper.json
    """
    logger.info("=" * 60)
    logger.info("CLEAN OUTPUTS")
    logger.info("=" * 60)
    
    if dry_run:
        logger.info("DRY RUN MODE - No files will be deleted")
    else:
        logger.info("LIVE MODE - Files will be deleted")
    
    if keep_cache:
        logger.info("Cache protection: cache_serper.json will be kept")
    
    logger.info("")
    
    # Find files to clean
    files_to_clean = find_files_to_clean(OUTPUT_DIR, keep_cache=keep_cache)
    
    if not files_to_clean:
        logger.info("No files to clean.")
        return
    
    # Group by extension for summary
    by_extension = {}
    total_size = 0
    
    for file_path in files_to_clean:
        ext = file_path.suffix.lower()
        if ext not in by_extension:
            by_extension[ext] = []
        by_extension[ext].append(file_path)
        
        if file_path.exists():
            total_size += file_path.stat().st_size
    
    # Print summary
    logger.info(f"Found {len(files_to_clean)} file(s) to clean:")
    for ext, files in sorted(by_extension.items()):
        logger.info(f"  {ext.upper()}: {len(files)} file(s)")
    
    if total_size > 0:
        size_mb = total_size / (1024 * 1024)
        logger.info(f"Total size: {size_mb:.2f} MB")
    
    logger.info("")
    logger.info("Files to be deleted:")
    for file_path in files_to_clean:
        size = file_path.stat().st_size if file_path.exists() else 0
        size_kb = size / 1024
        logger.info(f"  - {file_path.name} ({size_kb:.1f} KB)")
    
    logger.info("")
    
    if dry_run:
        logger.info("DRY RUN: No files were deleted.")
        logger.info("Run without --dry-run to actually delete files.")
        return
    
    # Confirm deletion
    deleted_count = 0
    deleted_size = 0
    
    for file_path in files_to_clean:
        try:
            if file_path.exists():
                size = file_path.stat().st_size
                file_path.unlink()
                deleted_count += 1
                deleted_size += size
                logger.debug(f"Deleted: {file_path.name}")
        except Exception as e:
            logger.error(f"Failed to delete {file_path.name}: {e}")
    
    logger.info("=" * 60)
    logger.info("CLEANUP COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Deleted: {deleted_count} file(s)")
    if deleted_size > 0:
        size_mb = deleted_size / (1024 * 1024)
        logger.info(f"Freed: {size_mb:.2f} MB")
    logger.info("")


def main():
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description='Clean generated artifacts from outputs/ directory',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tools/clean_outputs.py --dry-run          # Preview deletions
  python tools/clean_outputs.py --keep-cache        # Clean but keep cache
  python tools/clean_outputs.py                     # Clean everything
        """
    )
    parser.add_argument('--dry-run', action='store_true',
                       help='Preview what would be deleted without deleting')
    parser.add_argument('--keep-cache', action='store_true',
                       help='Keep cache_serper.json (API cache)')
    
    args = parser.parse_args()
    
    try:
        clean_outputs(dry_run=args.dry_run, keep_cache=args.keep_cache)
    except Exception as e:
        logger.error(f"Cleanup failed: {e}", exc_info=True)
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())
