# Databricks notebook source
from dataclasses import dataclass
# Source S3

dbutils.widgets.text("S3_Table",       "dbx_dev_data_refine.export.product_instance",
                     "S3 Table")
dbutils.widgets.text("Document_Table",       "app_dev_scapi_data.gold.documents",
                     "Document Table")
dbutils.widgets.text("hitl_review_table", "app_dev_scapi_data.stage.hitl_review_table",
                     "hitl review table")
dbutils.widgets.text("Refine_catalog",       "dbx_dev_data_refine",
                     "Refine catalog")
dbutils.widgets.text("S3_file",       "dbx_dev_data_refine.ereg_silver.s3_file",
                     "S3 file")

# Destination Volume
dbutils.widgets.text("dest_volume_path",    "/Volumes/app_dev_scapi_data/ereg_ingest/s3_replication/eReg_Sandbox/",
                     "Destination Volume Path")

# Tracking table
dbutils.widgets.text("tracking_schema",     "app_dev_scapi_data.ereg_ingest",
                     "Tracking Schema")
dbutils.widgets.text("tracking_table",      "s3_replication_requests_1",
                     "Tracking Table")

# Scalability knobs
dbutils.widgets.text("batch_size",          "800",   "Batch Size")
dbutils.widgets.text("max_retries",         "3",     "Max Retries")
dbutils.widgets.text("uuid_workers",        "10",    "UUID Workers")
dbutils.widgets.text("file_workers",        "5",     "File Workers")


# ── Config dataclass — reads from widgets ──────────────────────────────────────
@dataclass
class ReplicationConfig:

    # Source S3
    S3_Table:        str
    Document_Table:        str
    hitl_review_table:  str
    Refine_catalog:       str
    S3_file:       str
    
    # Destination Volume
    dest_volume_path:     str

    # Tracking table
    tracking_schema:      str
    tracking_table:       str

    # Scalability
    batch_size:           int
    max_retries:          int
    uuid_workers:         int
    file_workers:         int



def load_config() -> ReplicationConfig:
    """
    Read all widget values and return a populated ReplicationConfig.
    Called once at startup — any job parameter override is picked up here.
    """
    return ReplicationConfig(
        S3_Table            = dbutils.widgets.get("S3_Table"),
        Document_Table       = dbutils.widgets.get("Document_Table"),
        hitl_review_table    = dbutils.widgets.get("hitl_review_table"),
        Refine_catalog       = dbutils.widgets.get("Refine_catalog"),
        S3_file            = dbutils.widgets.get("S3_file"),
        dest_volume_path    = dbutils.widgets.get("dest_volume_path"),
        tracking_schema     = dbutils.widgets.get("tracking_schema"),
        tracking_table      = dbutils.widgets.get("tracking_table"),
        batch_size          = int(dbutils.widgets.get("batch_size")),
        max_retries         = int(dbutils.widgets.get("max_retries")),
        uuid_workers        = int(dbutils.widgets.get("uuid_workers")),
        file_workers        = int(dbutils.widgets.get("file_workers")),
    )


config = load_config()

print("Config loaded from widgets:")
print(f"  S3 Table        : {config.S3_Table}")
print(f"  Document Table  : {config.Document_Table}")
print(f"  hitl review table : {config.hitl_review_table}")
print(f"  Refine catalog  : {config.Refine_catalog}")
print(f"  S3 file         : {config.S3_file
print(f"  Dest Volume     : {config.dest_volume_path}")
print(f"  Tracking Schema : {config.tracking_schema}")       
print(f"  Tracking Table  : {config.tracking_schema}.{config.tracking_table}")

# COMMAND ----------

import logging
import time
import os
from dataclasses import dataclass
from enum import Enum
from typing import List, Dict, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# COMMAND ----------

filtered_df = spark.sql(f"""
    SELECT
      s3.*,
      d.productInsId,
      hrt.sys_study_id,
      hrt.sys_site_id,
      split(d.filePath, '/')[0] as request_id,
      split(d.filePath, '/')[1] as file_name,
      concat('/Volumes/dbx_prod_data_refine/export/ereg_raw/', s3.source_folder, "/", d.filePath) AS source_path
    FROM {config.S3_Table} AS s3
    JOIN {config.Document_Table} AS d
      ON s3.product_ins_id = d.productInsId
    JOIN {config.hitl_review_table} AS hrt
      ON d.studyId = hrt.sys_study_id AND d.siteId = hrt.sys_site_id
    WHERE hrt.hitl_status = 'Accept'
""")

# COMMAND ----------

# DBTITLE 1,Cell 4
# /Volumes/dbx_prod_data_refine/export/ereg_raw

# COMMAND ----------

filter_column = filtered_df.select("request_id", "file_name", "source_path")

# COMMAND ----------

# ── Status & Errors ────────────────────────────────────────────────────────────

class ReplicationStatus(Enum):
    PENDING     = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED   = "COMPLETED"
    FAILED      = "FAILED"
    SKIPPED     = "SKIPPED"   # UUID folder missing or empty in S3


class TransientError(Exception):
    pass

class PermanentError(Exception):
    pass

# COMMAND ----------

# DBTITLE 1,Volume Helpers
# ── Volume Helpers ─────────────────────────────────────────────────────────────

def list_volume_files(source_path: str) -> Tuple[list, Optional[str]]:
    """
    List all files under a volume path using dbutils.fs.ls.

    Returns (files, skip_reason):
        files       : list of dicts — 'path', 'name', 'size'
        skip_reason : None if files found, otherwise reason for skip
    """
    try:
        items = dbutils.fs.ls(source_path)
    except Exception as e:
        logger.warning(f"Source folder not found at {source_path}: {e}")
        return [], "Source folder not found in volume"

    files = [
        {'path': item.path, 'name': item.name, 'size': item.size}
        for item in items
        if not item.name.endswith('/')     # skip folder placeholder entries
    ]

    if not files:
        logger.warning(f"Source folder exists but is empty at {source_path}")
        return [], "Source folder exists but is empty"

    return files, None


def copy_file_to_volume(src_path: str, dest_path: str) -> int:
    """
    Copy a single file from source volume to destination volume.
    Returns bytes copied, or -1 if the file already exists (idempotent skip).
    Thread-safe — each call works on a different file path.
    """
    # Idempotency check — skip if already at destination
    if os.path.exists(dest_path):
        logger.debug(f"Already exists, skipping: {dest_path}")
        return -1

    # Ensure the UUID subfolder exists under the Volume
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

    dbutils.fs.cp(src_path, dest_path)

    bytes_written = os.path.getsize(dest_path)
    logger.debug(f"Copied {src_path} -> {dest_path} ({bytes_written} bytes)")
    return bytes_written

# COMMAND ----------

def classify_error(e: Exception):
    """Classify an exception as transient (retryable) or permanent."""
    msg = str(e).lower()
    if any(kw in msg for kw in ['timeout', 'throttl', 'slow', 'unavailable', 'temporary']):
        raise TransientError(str(e)) from e
    raise PermanentError(str(e)) from e


# COMMAND ----------

# DBTITLE 1,Delta Table Setup
# ── Delta Table Setup ──────────────────────────────────────────────────────────

def init_tracking_table(config: ReplicationConfig):
    """Create the Delta tracking table if it doesn't already exist."""
    full_table = f"{config.tracking_schema}.{config.tracking_table}"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            request_id          STRING    NOT NULL,  -- UUID folder name
            file_name           STRING    NOT NULL,  -- File name
            source_path         STRING    NOT NULL,  -- Source volume path
            dest_path           STRING    NOT NULL,  -- Destination volume folder
            status              STRING    NOT NULL,  -- PENDING/IN_PROGRESS/COMPLETED/FAILED/SKIPPED
            files_copied        INT,
            total_bytes         BIGINT,
            error_message       STRING,              -- failure reason OR skip reason
            retry_count         INT,
            created_timestamp   TIMESTAMP NOT NULL,
            started_timestamp   TIMESTAMP,
            completed_timestamp TIMESTAMP
        )
        USING DELTA 
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',   -- auto-compact small files on write
            'delta.autoOptimize.autoCompact'   = 'true'    -- auto-compact on read
        )
        COMMENT 'Tracks volume-to-volume replication for HITL-accepted documents'
    """)
    logger.info(f"Tracking table ready: {full_table}")


# COMMAND ----------

# DBTITLE 1,Populate pending requests
def populate_pending_requests(config: ReplicationConfig, source_paths_df) -> int:
    """
    Insert NEW rows into the tracking table from filtered_df source paths.
    Only inserts paths not already tracked (any status) — fully idempotent.
    Uses a single INSERT ... SELECT instead of row-by-row inserts for efficiency.
    """
    full_table = f"{config.tracking_schema}.{config.tracking_table}"

    # Register source paths as temp view
    source_paths_df.createOrReplaceTempView("_source_paths_temp")

    # Extract UUID (last path segment) using substring_index
    spark.sql(f"""
        INSERT INTO {full_table}
            (request_id,file_name, source_path, dest_path,
             status, retry_count, created_timestamp)

        SELECT
            s.request_id                            AS request_id,
            s.file_name                             AS file_name,
            s.source_path                                                     AS source_path,
            concat('{config.dest_volume_path}', substring_index(s.source_path, '/', -2), '/') AS dest_path,
            'PENDING'                                                         AS status,
            0                                                                 AS retry_count,
            CURRENT_TIMESTAMP                                                 AS created_timestamp

        FROM _source_paths_temp s
        WHERE NOT EXISTS (
            SELECT 1 FROM {full_table} t
            WHERE t.request_id = s.request_id
        )
    """)

    pending_count = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {full_table} WHERE status = 'PENDING'
    """).collect()[0]['cnt']

    logger.info(f"Pending requests ready to process: {pending_count}")
    return pending_count


# COMMAND ----------

# ── Batch Status Update (key scalability change) ───────────────────────────────

def batch_update_status(config: ReplicationConfig, results: list):
    """
    Write all status updates for the current batch in ONE single MERGE statement
    instead of one UPDATE per UUID.

    'results' is a list of dicts, each with:
        request_id, status, files_copied, total_bytes, error_message
    """
    if not results:
        return

    full_table = f"{config.tracking_schema}.{config.tracking_table}"

    # Build a VALUES list from all results
    # e.g. ('uuid-1','COMPLETED',2,4096,NULL), ('uuid-2','FAILED',0,0,'Access denied')
    values_rows = []
    for r in results:
        safe_error = (r.get('error_message') or '').replace("'", "''")
        error_val  = f"'{safe_error}'" if safe_error else 'NULL'
        files_val  = r.get('files_copied') if r.get('files_copied') is not None else 0
        bytes_val  = r.get('total_bytes')  if r.get('total_bytes')  is not None else 0
        values_rows.append(
            f"('{r['request_id']}', '{r['status']}', {files_val}, {bytes_val}, {error_val})"
        )

    values_sql = ",\n        ".join(values_rows)

    # Single MERGE covers all rows in the batch — far more efficient than
    # individual UPDATE statements (avoids N separate Delta transactions)
    spark.sql(f"""
        MERGE INTO {full_table} AS target
        USING (
            SELECT
                request_id,
                status,
                files_copied,
                total_bytes,
                error_message
            FROM (VALUES
                {values_sql}
            ) AS t(request_id, status, files_copied, total_bytes, error_message)
        ) AS source
        ON target.request_id = source.request_id

        WHEN MATCHED THEN UPDATE SET
            target.status              = source.status,
            target.files_copied        = source.files_copied,
            target.total_bytes         = source.total_bytes,
            target.error_message       = source.error_message,
            target.completed_timestamp = CURRENT_TIMESTAMP
    """)

    logger.info(f"Batch status update complete for {len(results)} rows")

# COMMAND ----------

# DBTITLE 1,Replicate one UUID folder
# ── Core: Replicate ONE UUID folder (runs inside a thread) ────────────────────

def replicate_uuid_folder(config: ReplicationConfig, row: dict) -> dict:
    """
    Replicate all files under one UUID folder from source volume to destination volume.
    Runs inside a ThreadPoolExecutor thread — must be thread-safe.

    Returns a result dict:
        request_id, status, files_copied, total_bytes, error_message
    """
    request_id  = row['request_id']
    source_path = row['source_path']     # Source volume path
    dest_folder = row['dest_path']       # Destination volume folder

    result = {
        'request_id'   : request_id,
        'status'       : ReplicationStatus.FAILED.value,
        'files_copied' : 0,
        'total_bytes'  : 0,
        'error_message': None
    }

    for attempt in range(config.max_retries):
        try:
            # List all files under this UUID folder in source volume
            files, skip_reason = list_volume_files(source_path)

            if not files:
                # UUID folder missing from S3 OR exists but empty — distinguish clearly
                logger.warning(f"Skipping UUID {request_id}: {skip_reason}")
                result['status']        = ReplicationStatus.SKIPPED.value
                result['error_message'] = skip_reason
                return result

            # ── Parallel file copy within this UUID folder ─────────────────
            # Uses a nested ThreadPoolExecutor so multiple files under one UUID
            # are copied simultaneously instead of one-by-one
            total_bytes  = 0
            files_copied = 0
            copy_errors  = []

            with ThreadPoolExecutor(max_workers=config.file_workers) as file_executor:
                future_to_file = {
                    file_executor.submit(
                        copy_file_to_volume,
                        f['path'],
                        dest_folder.rstrip('/') + '/' + f['name']
                    ): f
                    for f in files
                }

                for future in as_completed(future_to_file):
                    f = future_to_file[future]
                    try:
                        bytes_written = future.result()
                        if bytes_written == -1:
                            logger.debug(f"Skipped existing file: {f['name']}")
                        else:
                            total_bytes  += bytes_written
                            files_copied += 1
                    except Exception as e:
                        copy_errors.append(f"{f['name']}: {str(e)}")
                        logger.error(f"Failed to copy {f['name']} for UUID {request_id}: {e}")

            if copy_errors:
                # Some files failed — mark UUID as FAILED with details
                result['error_message'] = f"{len(copy_errors)} file(s) failed: {'; '.join(copy_errors[:3])}"
                result['status']        = ReplicationStatus.FAILED.value
                return result

            result['status']        = ReplicationStatus.COMPLETED.value
            result['files_copied']  = files_copied
            result['total_bytes']   = total_bytes
            return result

        except Exception as e:
            result['error_message'] = str(e)
            try:
                classify_error(e)
            except TransientError:
                if attempt < config.max_retries - 1:
                    wait_time = 2 ** attempt    # 1s, 2s, 4s
                    logger.warning(f"Transient error on {request_id}, retry {attempt+1} in {wait_time}s: {e}")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Max retries exceeded for {request_id}: {e}")
            except PermanentError:
                logger.error(f"Permanent error on {request_id}: {e}")
            break

    return result


# COMMAND ----------

# DBTITLE 1,Mark batch in-progress helper
# ── Mark Batch In Progress ──────────────────────────────────────────────────────

def mark_batch_in_progress(config: ReplicationConfig, request_ids: list):
    """
    Mark an entire batch of request_ids as IN_PROGRESS in one UPDATE statement.
    This avoids N individual updates and reduces Delta transactions.
    """
    full_table = f"{config.tracking_schema}.{config.tracking_table}"
    ids_csv = ", ".join(f"'{rid}'" for rid in request_ids)

    spark.sql(f"""
        UPDATE {full_table}
        SET status = 'IN_PROGRESS',
            started_timestamp = CURRENT_TIMESTAMP,
            retry_count = retry_count + 1
        WHERE request_id IN ({ids_csv})
    """)

    logger.info(f"Marked {len(request_ids)} requests as IN_PROGRESS")

# COMMAND ----------

# DBTITLE 1,Process pending requests
# ── Main Processing Loop (parallel UUID processing) ───────────────────────────

def process_pending_requests(config: ReplicationConfig) -> int:
    """
    Fetch a batch of PENDING/retryable FAILED requests and process them in parallel.

    Scalability changes vs sequential version:
      1. Marks entire batch IN_PROGRESS in ONE UPDATE (not N updates)
      2. Processes all UUIDs in parallel via ThreadPoolExecutor (uuid_workers threads)
      3. Each UUID copies its files in parallel (file_workers threads)
      4. Writes all results back in ONE MERGE at the end (not N updates)
    """
    full_table = f"{config.tracking_schema}.{config.tracking_table}"

    pending_df = spark.sql(f"""
        SELECT request_id, source_path, dest_path, retry_count
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

    logger.info(f"Processing {len(rows)} UUID folders with {config.uuid_workers} parallel workers...")

    # Step 1 — Mark entire batch IN_PROGRESS in one shot
    request_ids = [r['request_id'] for r in rows]
    mark_batch_in_progress(config, request_ids)

    # Step 2 — Process all UUIDs in parallel
    # Each UUID runs replicate_uuid_folder() in its own thread
    results = []
    with ThreadPoolExecutor(max_workers=config.uuid_workers) as uuid_executor:
        future_to_row = {
            uuid_executor.submit(replicate_uuid_folder, config, row): row
            for row in rows
        }

        for future in as_completed(future_to_row):
            row = future_to_row[future]
            try:
                result = future.result()
                results.append(result)
                logger.info(
                    f"[{result['status']}] {result['request_id']} — "
                    f"{result['files_copied']} files, {result['total_bytes']} bytes"
                    + (f" | {result['error_message']}" if result['error_message'] else "")
                )
            except Exception as e:
                # Unexpected thread-level failure — shouldn't happen but handle safely
                logger.error(f"Thread crashed for UUID {row['request_id']}: {e}", exc_info=True)
                results.append({
                    'request_id'   : row['request_id'],
                    'status'       : ReplicationStatus.FAILED.value,
                    'files_copied' : 0,
                    'total_bytes'  : 0,
                    'error_message': f"Thread crashed: {str(e)}"
                })

    # Step 3 — Write ALL results back in ONE MERGE (not N individual updates)
    batch_update_status(config, results)

    successful = sum(1 for r in results if r['status'] == ReplicationStatus.COMPLETED.value)
    skipped    = sum(1 for r in results if r['status'] == ReplicationStatus.SKIPPED.value)
    failed     = sum(1 for r in results if r['status'] == ReplicationStatus.FAILED.value)

    logger.info(f"Batch done — Completed: {successful} | Skipped: {skipped} | Failed: {failed}")
    return successful


# COMMAND ----------

# DBTITLE 1,Summary
# ── Summary ────────────────────────────────────────────────────────────────────

def print_summary(config: ReplicationConfig):
    """Print a summary grouped by status, plus a SKIPPED breakdown showing why."""
    full_table = f"{config.tracking_schema}.{config.tracking_table}"

    print("\n===== S3 Replication Summary =====")
    spark.sql(f"""
        SELECT
            status,
            COUNT(*)          AS request_count,
            SUM(files_copied) AS total_files,
            ROUND(SUM(total_bytes) / 1024 / 1024, 2) AS total_mb
        FROM {full_table}
        GROUP BY status
        ORDER BY status
    """).show(truncate=False)

    print("\n===== SKIPPED Breakdown (Why?) =====")
    # Shows clearly: how many UUIDs were missing from S3 vs how many had empty folders
    spark.sql(f"""
        SELECT
            error_message AS skip_reason,
            COUNT(*)      AS count
        FROM {full_table}
        WHERE status = 'SKIPPED'
        GROUP BY error_message
        ORDER BY error_message
    """).show(truncate=False)

    print("\n===== FAILED Details =====")
    spark.sql(f"""
        SELECT
            request_id,
            source_path,
            error_message,
            retry_count
        FROM {full_table}
        WHERE status = 'FAILED'
        ORDER BY completed_timestamp DESC
        LIMIT 20
    """).show(truncate=False)


# COMMAND ----------

# DBTITLE 1,Cell 12
# ── Entry Point ────────────────────────────────────────────────────────────────

def main():
    config = load_config()
    logger.info("=== Volume Replication Job Started ===")

    # 1. Create Delta tracking table if not exists
    init_tracking_table(config)

    # 2. Insert new source paths as PENDING (deduplicated to avoid future conflicts)
    populate_pending_requests(config, filter_column.distinct())

    # 3. Process batch in parallel — UUID-level + file-level parallelism
    successful = process_pending_requests(config)

    # 4. Print summary with SKIPPED breakdown
    print_summary(config)

    logger.info(f"=== Job Completed: {successful} UUID folder(s) replicated ===")


main()