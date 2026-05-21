import logging
import time
import hashlib
from dataclasses import dataclass
from enum import Enum
from typing import Optional, List, Dict
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

try:
    from databricks.sdk import WorkspaceClient
    DATABRICKS_AVAILABLE = True
except ImportError:
    DATABRICKS_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ReplicationConfig:
    # S3 source settings
    source_bucket: str = "qa-ereg-us-east-2-ses-qa-ereg"
    source_prefix: str = "sw1-qa-ereg"                          # Files live at: s3://<source_bucket>/<source_prefix>/<uuid>/
    source_external_location_path: str = "s3://qa-ereg-us-east-2-ses-qa-ereg/sw1-qa-ereg"
    aws_region: str = "us-east-2"

    # Destination: Databricks Volume path
    dest_volume_path: str = "/Volumes/poc_cc/dev_bronze/s3_replication"

    # Silver table that drives replication
    silver_table: str = "dbx_dev_data_refine.ereg_silver.s3_file"

    # Delta tracking table
    tracking_schema: str = "poc_cc.dev_bronze"
    tracking_table: str = "s3_replication_requests"

    # Job tuning
    batch_size: int = 5
    max_retries: int = 3


config = ReplicationConfig()
class ReplicationStatus(Enum):
    PENDING      = "PENDING"
    IN_PROGRESS  = "IN_PROGRESS"
    COMPLETED    = "COMPLETED"
    FAILED       = "FAILED"
    SKIPPED      = "SKIPPED"     # File already existed at destination


class TransientError(Exception):
    pass

class PermanentError(Exception):
    pass
def list_s3_files_dbutils(bucket: str, prefix: str) -> list:
    """
    List all files under an S3 prefix using dbutils.fs.ls (credential vending
    is handled automatically via Unity Catalog external location).
    Returns list of dicts with 'path', 'name', and 'size'.
    """
    s3_path = f"s3://{bucket}/{prefix}"
    try:
        items = dbutils.fs.ls(s3_path)
    except Exception as e:
        logger.warning(f"Could not list {s3_path}: {e}")
        return []

    files = []
    for item in items:
        if not item.name.endswith('/'):
            files.append({'path': item.path, 'name': item.name, 'size': item.size})
    return files


def copy_file_to_volume(src_path: str, dest_path: str) -> int:
    """
    Copy a single file from S3 to a Volume path using dbutils.fs.cp.
    Returns bytes copied, or -1 if the file already exists (idempotent skip).
    """
    import os

    # Idempotency check — Volumes are FUSE-mounted so os.path.exists works
    if os.path.exists(dest_path):
        logger.debug(f"File already exists, skipping: {dest_path}")
        return -1

    # Ensure parent directory exists on the FUSE mount
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

    # Copy directly to Volume path (no 'file:' prefix — required for serverless)
    dbutils.fs.cp(src_path, dest_path)

    # Get size of the copied file
    bytes_written = os.path.getsize(dest_path)
    logger.debug(f"Copied {src_path} -> {dest_path} ({bytes_written} bytes)")
    return bytes_written

def init_tracking_table(config: ReplicationConfig):
    """
    Create the Delta tracking table if it doesn't already exist.
    Schema mirrors the original PostgreSQL replication_requests table.
    """
    full_table = f"{config.tracking_schema}.{config.tracking_table}"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            request_id          STRING      NOT NULL,   -- s3_file_id (UUID)
            product_ins_id      BIGINT,
            product_type        STRING,
            file_name           STRING,
            source_bucket       STRING      NOT NULL,
            source_key          STRING      NOT NULL,   -- full S3 prefix path for this UUID folder
            dest_path           STRING      NOT NULL,   -- local Volume destination folder
            status              STRING      NOT NULL,   -- PENDING / IN_PROGRESS / COMPLETED / FAILED / SKIPPED
            files_copied        INT,
            total_bytes         BIGINT,
            error_message       STRING,
            retry_count         INT,
            created_timestamp   TIMESTAMP   NOT NULL,
            started_timestamp   TIMESTAMP,
            completed_timestamp TIMESTAMP
        )
        USING DELTA
        COMMENT 'Tracks S3-to-Volume replication requests, populated from ereg_silver.s3_file'
    """)
    logger.info(f"Tracking table ready: {full_table}")




def populate_pending_requests(config: ReplicationConfig):
    """
    Insert NEW rows into the tracking table from the silver s3_file table.
    Only inserts s3_file_ids that are not already tracked (any status).
    This ensures idempotency — re-running the job never creates duplicate entries.
    """
    full_table = f"{config.tracking_schema}.{config.tracking_table}"

    spark.sql(f"""
        INSERT INTO {full_table}
            (request_id, product_ins_id, product_type, file_name,
             source_bucket, source_key, dest_path,
             status, retry_count, created_timestamp)

        SELECT
            s.s3_file_id                                                    AS request_id,
            s.product_ins_id,
            s.product_type,
            s.file_name,
            '{config.source_bucket}'                                        AS source_bucket,
            '{config.source_prefix}/' || s.s3_file_id || '/'               AS source_key,
            '{config.dest_volume_path}/' || s.s3_file_id || '/'            AS dest_path,
            'PENDING'                                                        AS status,
            0                                                                AS retry_count,
            CURRENT_TIMESTAMP                                                AS created_timestamp

        FROM {config.silver_table} s
        WHERE NOT EXISTS (
            SELECT 1 FROM {full_table} t
            WHERE t.request_id = s.s3_file_id
        )
    """)

    new_count = spark.sql(f"""
        SELECT COUNT(*) as cnt FROM {full_table} WHERE status = 'PENDING'
    """).collect()[0]['cnt']

    logger.info(f"Pending requests ready to process: {new_count}")
    return new_count


# S3 file operations are now handled via dbutils.fs in Cell 8 above.
# Keeping retry error classes for replicate_uuid_folder logic.

def classify_error(e: Exception):
    """Classify an exception as transient or permanent for retry logic."""
    error_msg = str(e).lower()
    transient_keywords = ['timeout', 'throttl', 'slow', 'unavailable', 'temporary']
    permanent_keywords = ['access denied', 'forbidden', 'not found', 'no such']

    if any(kw in error_msg for kw in transient_keywords):
        raise TransientError(f"Transient error: {e}") from e
    elif any(kw in error_msg for kw in permanent_keywords):
        raise PermanentError(f"Permanent error: {e}") from e
    else:
        raise PermanentError(f"Unexpected error: {e}") from e


def update_status(config: ReplicationConfig, request_id: str, status: ReplicationStatus,
                  files_copied: int = None, total_bytes: int = None,
                  error_message: str = None):
    """Update the status of a tracking row in the Delta table."""
    full_table = f"{config.tracking_schema}.{config.tracking_table}"

    if status == ReplicationStatus.IN_PROGRESS:
        spark.sql(f"""
            UPDATE {full_table}
            SET status = '{status.value}',
                started_timestamp = CURRENT_TIMESTAMP,
                retry_count = retry_count + 1
            WHERE request_id = '{request_id}'
        """)

    elif status == ReplicationStatus.COMPLETED:
        spark.sql(f"""
            UPDATE {full_table}
            SET status = '{status.value}',
                completed_timestamp = CURRENT_TIMESTAMP,
                files_copied = {files_copied or 0},
                total_bytes = {total_bytes or 0}
            WHERE request_id = '{request_id}'
        """)

    elif status == ReplicationStatus.FAILED:
        safe_error = (error_message or "").replace("'", "''")  # escape single quotes
        spark.sql(f"""
            UPDATE {full_table}
            SET status = '{status.value}',
                completed_timestamp = CURRENT_TIMESTAMP,
                error_message = '{safe_error}'
            WHERE request_id = '{request_id}'
        """)

    elif status == ReplicationStatus.SKIPPED:
        spark.sql(f"""
            UPDATE {full_table}
            SET status = '{status.value}',
                completed_timestamp = CURRENT_TIMESTAMP,
                files_copied = 0,
                total_bytes = 0
            WHERE request_id = '{request_id}'
        """)




ef replicate_uuid_folder(config: ReplicationConfig, row: dict) -> bool:
    """
    Replicate all files under one UUID folder from S3 to the Volume.
    
    - Lists all files under s3://<bucket>/<prefix>/<uuid>/
    - Copies each file to /Volumes/.../<uuid>/<filename>
    - Skips files that already exist (idempotent)
    - Returns True on success, False on failure
    """
    request_id    = row['request_id']
    source_bucket = row['source_bucket']
    source_prefix = row['source_key']   # e.g. "sw1-qa-ereg/<uuid>/"
    dest_folder   = row['dest_path']    # e.g. "/Volumes/poc_cc/dev_bronze/s3_replication/<uuid>/"

    success       = False
    error_message = None

    for attempt in range(config.max_retries):
        try:
            update_status(config, request_id, ReplicationStatus.IN_PROGRESS)

            # List all files under this UUID folder in S3 using dbutils.fs
            files = list_s3_files_dbutils(source_bucket, source_prefix)

            if not files:
                logger.warning(f"No files found in S3 for UUID: {request_id}")
                update_status(config, request_id, ReplicationStatus.SKIPPED)
                return True

            total_bytes  = 0
            files_copied = 0

            for f in files:
                local_path = dest_folder.rstrip('/') + '/' + f['name']

                bytes_written = copy_file_to_volume(f['path'], local_path)

                if bytes_written == -1:
                    logger.debug(f"Skipped existing file: {local_path}")
                else:
                    total_bytes  += bytes_written
                    files_copied += 1

            update_status(config, request_id, ReplicationStatus.COMPLETED,
                          files_copied=files_copied, total_bytes=total_bytes)
            success = True
            break

        except TransientError as e:
            error_message = str(e)
            if attempt == config.max_retries - 1:
                logger.error(f"Max retries exceeded for {request_id}: {e}")
                break
            wait_time = 2 ** attempt   # 1s, 2s, 4s
            logger.warning(f"Transient error on {request_id}, retrying in {wait_time}s: {e}")
            time.sleep(wait_time)

        except PermanentError as e:
            error_message = str(e)
            logger.error(f"Permanent error on {request_id}: {e}")
            break

        except Exception as e:
            error_message = str(e)
            logger.error(f"Unexpected error on {request_id}: {e}", exc_info=True)
            try:
                classify_error(e)
            except TransientError:
                if attempt < config.max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
            break

    if not success:
        update_status(config, request_id, ReplicationStatus.FAILED,
                      error_message=error_message or "Max retries exceeded")

    return success



def process_pending_requests(config: ReplicationConfig) -> int:
    """
    Fetch a batch of PENDING requests from the Delta tracking table and process them.
    Also retries FAILED rows that haven't exceeded max_retries yet.
    Returns the count of successfully replicated UUID folders.
    """
    full_table = f"{config.tracking_schema}.{config.tracking_table}"

    pending_df = spark.sql(f"""
        SELECT request_id, source_bucket, source_key, dest_path, retry_count
        FROM {full_table}
        WHERE status = 'PENDING'
           OR (status = 'FAILED' AND retry_count < {config.max_retries})
        ORDER BY created_timestamp ASC
        LIMIT {config.batch_size}
    """)

    rows = [row.asDict() for row in pending_df.collect()]

    if not rows:
        logger.info("No pending replication requests found.")
        return 0

    logger.info(f"Processing {len(rows)} requests...")

    successful = 0
    for row in rows:
        ok = replicate_uuid_folder(config, row)
        if ok:
            successful += 1

    logger.info(f"Completed: {successful}/{len(rows)} successful")
    return successful


def print_summary(config: ReplicationConfig):
    """Print a summary of the replication tracking table grouped by status."""
    full_table = f"{config.tracking_schema}.{config.tracking_table}"

    summary_df = spark.sql(f"""
        SELECT
            status,
            COUNT(*)        AS request_count,
            SUM(files_copied) AS total_files,
            SUM(total_bytes)  AS total_bytes
        FROM {full_table}
        GROUP BY status
        ORDER BY status
    """)

    print("\n===== S3 Replication Summary =====")
    summary_df.show(truncate=False)



def main():
    logger.info("=== S3 Replication Job Started ===")

    # 1. Ensure tracking Delta table exists
    init_tracking_table(config)

    # 2. Populate new PENDING rows from silver table (incremental)
    populate_pending_requests(config)

    # 3. Process pending requests (uses dbutils.fs for S3 access via UC external location)
    successful = process_pending_requests(config)

    # 4. Print summary
    print_summary(config)

    logger.info(f"=== Job Completed: {successful} UUID folder(s) replicated ===")


main()
