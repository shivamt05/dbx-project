# Databricks notebook source
# MAGIC %md ## Cell 1 — Install

# COMMAND ----------

# MAGIC %pip install boto3 --quiet
# MAGIC %pip install --upgrade databricks-sdk --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Cell 2 — Imports

# COMMAND ----------

import logging
import time
import hashlib
from dataclasses import dataclass
from enum import Enum
from typing import Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.exceptions import ClientError
from botocore.config import Config as BotocoreConfig
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import PathOperation

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
print("✅ Imports done")

# COMMAND ----------

# MAGIC %md ## Cell 3 — Config
# MAGIC
# MAGIC Only things that change between environments or source buckets.
# MAGIC All other logic stays the same.

# COMMAND ----------

# ── Widgets ────────────────────────────────────────────────────────────────────
# Defaults = dev values.
# When run via Databricks Job → job overwrites these via base_parameters.
# When run manually in notebook UI → defaults below are used.
# ──────────────────────────────────────────────────────────────────────────────

# Source S3
dbutils.widgets.text("source_bucket",       "qa-ereg-us-east-2-ses-qa-ereg",
                     "Source S3 Bucket")
dbutils.widgets.text("source_ext_location", "s3://qa-ereg-us-east-2-ses-qa-ereg",
                     "Source External Location")
dbutils.widgets.text("aws_region",          "us-east-2",
                     "AWS Region")

# Destination Volume
dbutils.widgets.text("dest_volume_path",    "/Volumes/dbx_dev_data_refine/export/cc_refine/test",
                     "Destination Volume Path")

# Silver table — has s3_file_id, product_ins_id, product_type, file_name
dbutils.widgets.text("silver_table",        "dbx_dev_data_refine.ereg_silver.s3_file",
                     "Silver Table")

# Folder lookup table — has product_ins_id + folder_name
# (the S3 top-level folder a file lives in — replaces live S3 discovery entirely)
dbutils.widgets.text("folder_lookup_table", "dbx_dev_data_refine.ereg_silver.product_folder_lookup",
                     "Folder Lookup Table (product_ins_id -> folder_name)")
dbutils.widgets.text("folder_lookup_join_col", "product_ins_id",
                     "Join column shared by silver_table and folder_lookup_table")
dbutils.widgets.text("folder_name_col",     "folder_name",
                     "Column in folder_lookup_table holding the S3 folder name")

# Tracking table
dbutils.widgets.text("tracking_schema",     "dbx_dev_data_refine.export",
                     "Tracking Schema")
dbutils.widgets.text("tracking_table",      "s3_replication_requests_2",
                     "Tracking Table")

# Scalability knobs
# NOTE: batch_size deliberately has NO upper cap behavior baked into the query —
# every PENDING/retryable row found is processed in the SAME run. The job is
# expected to run frequently (e.g. every few minutes) until the backlog clears,
# rather than relying on a hard per-run ceiling that forces extra re-runs.
dbutils.widgets.text("max_retries",         "3",     "Max Retries (FAILED)")
dbutils.widgets.text("skipped_max_retries", "5",     "Max Retries (SKIPPED)")
dbutils.widgets.text("uuid_workers",        "32",    "UUID Workers")
dbutils.widgets.text("file_workers",        "4",     "File Workers (per UUID)")


# ── Config dataclass — reads from widgets ──────────────────────────────────────
@dataclass
class ReplicationConfig:

    # Source S3
    source_bucket:        str
    source_ext_location:  str
    aws_region:           str

    # Destination Volume
    dest_volume_path:     str

    # Silver table + folder lookup
    silver_table:         str
    folder_lookup_table:  str
    folder_lookup_join_col: str
    folder_name_col:      str

    # Tracking table
    tracking_schema:      str
    tracking_table:       str

    # Scalability
    max_retries:          int
    skipped_max_retries:  int
    uuid_workers:         int
    file_workers:         int

    # Chunk size — fixed, not a widget (no reason to change per env)
    chunk_size:           int = 8 * 1024 * 1024   # 8MB


def load_config() -> ReplicationConfig:
    """
    Read all widget values and return a populated ReplicationConfig.
    Called once at startup — any job parameter override is picked up here.
    """
    return ReplicationConfig(
        source_bucket          = dbutils.widgets.get("source_bucket"),
        source_ext_location    = dbutils.widgets.get("source_ext_location"),
        aws_region              = dbutils.widgets.get("aws_region"),
        dest_volume_path         = dbutils.widgets.get("dest_volume_path"),
        silver_table             = dbutils.widgets.get("silver_table"),
        folder_lookup_table      = dbutils.widgets.get("folder_lookup_table"),
        folder_lookup_join_col   = dbutils.widgets.get("folder_lookup_join_col"),
        folder_name_col          = dbutils.widgets.get("folder_name_col"),
        tracking_schema          = dbutils.widgets.get("tracking_schema"),
        tracking_table           = dbutils.widgets.get("tracking_table"),
        max_retries              = int(dbutils.widgets.get("max_retries")),
        skipped_max_retries      = int(dbutils.widgets.get("skipped_max_retries")),
        uuid_workers             = int(dbutils.widgets.get("uuid_workers")),
        file_workers             = int(dbutils.widgets.get("file_workers")),
    )


config = load_config()

print("Config loaded from widgets:")
print(f"  Source        : s3://{config.source_bucket}/ (folder resolved per-row via lookup join)")
print(f"  Dest          : {config.dest_volume_path}/")
print(f"  Silver        : {config.silver_table}")
print(f"  Folder lookup : {config.folder_lookup_table} (join on {config.folder_lookup_join_col})")
print(f"  Tracking      : {config.tracking_schema}.{config.tracking_table}")
print(f"  Workers       : {config.uuid_workers} UUID × {config.file_workers} file")
print(f"  Retries       : {config.max_retries} (FAILED) | {config.skipped_max_retries} (SKIPPED)")
print(f"  Chunk         : {config.chunk_size // (1024*1024)}MB per chunk")

# COMMAND ----------

# MAGIC %md ## Cell 4 — Status Enums & Error Classes
# MAGIC
# MAGIC

# COMMAND ----------

class ReplicationStatus(Enum):
    PENDING     = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED   = "COMPLETED"
    FAILED      = "FAILED"
    SKIPPED     = "SKIPPED"    # UUID folder missing or empty in source S3


class TransientError(Exception):
    """Retryable — throttling, timeout, service unavailable."""
    pass

class PermanentError(Exception):
    """Non-retryable — access denied, no such key/bucket."""
    pass


def classify_boto3_error(e: ClientError):
    """
    V3 used string matching on error messages — unreliable.
    V4 uses specific boto3 ClientError codes — exact and reliable.
    """
    code = e.response.get('Error', {}).get('Code', '')

    transient_codes = {
        'RequestTimeout', 'ServiceUnavailable',
        'ThrottlingException', 'SlowDown',
        'RequestTimeTooSkewed', 'InternalError',
        '503', '500'
    }
    permanent_codes = {
        'AccessDenied', 'NoSuchBucket', 'NoSuchKey',
        'InvalidBucketName', 'AllAccessDisabled',
        'AuthorizationHeaderMalformed', '403', '404'
    }

    if code in transient_codes:
        raise TransientError(f"Transient [{code}]: {e}") from e
    elif code in permanent_codes:
        raise PermanentError(f"Permanent [{code}]: {e}") from e
    else:
        raise PermanentError(f"Unknown [{code}]: {e}") from e

print("✅ Enums and error classes ready")

# COMMAND ----------

# MAGIC %md ## Cell 5 — Unity Catalog Temp Credentials → boto3 Source Client
# MAGIC
# MAGIC Only ONE boto3 client needed — source bucket (READ only).
# MAGIC Destination is a UC Volume — no S3 client needed for dest.
# MAGIC UC Volume uses Databricks-managed internal credentials automatically.

# COMMAND ----------

# DBTITLE 1,Untitled
def build_s3_client(ext_location_path: str, region: str,
                    operation: PathOperation) -> boto3.client:
    """
    Build a boto3 S3 client using Unity Catalog temporary credentials.
    Credentials are scoped to the given external location path only.
    Expire in ~1 hour — rebuilt each job run.

    Botocore adaptive retry handles S3 rate limiting automatically
    (backs off on ThrottlingException / SlowDown without crashing).
    """
    ws    = WorkspaceClient()
    creds = ws.temporary_path_credentials.generate_temporary_path_credentials(
        ext_location_path, operation
    )
    aws   = creds.aws_temp_credentials

    boto_config = BotocoreConfig(
        retries={
            'mode'        : 'adaptive',  # auto back-off on throttling
            'max_attempts': 5
        }
    )

    session = boto3.Session(
        aws_access_key_id     = aws.access_key_id,
        aws_secret_access_key = aws.secret_access_key,
        aws_session_token     = aws.session_token,
        region_name           = region
    )
    return session.client('s3', config=boto_config)


# Build source client — READ only
print("Building source S3 client (PATH_READ)...")
source_client = build_s3_client(
    ext_location_path = config.source_ext_location,
    region            = config.aws_region,
    operation         = PathOperation.PATH_READ
)

# Destination: UC Volume (no S3 client needed — Databricks-managed credentials)
# The Volume path is a local filesystem path: /Volumes/catalog/schema/volume/...

# Sanity checks
try:
    source_client.head_bucket(Bucket=config.source_bucket)
    print(f"✅ Source client ready — '{config.source_bucket}' accessible")
except Exception as e:
    print(f"❌ Source bucket error: {e}")

import os
if os.path.isdir(config.dest_volume_path):
    print(f"✅ Dest Volume ready   — '{config.dest_volume_path}' accessible")
else:
    os.makedirs(config.dest_volume_path, exist_ok=True)
    print(f"✅ Dest Volume created — '{config.dest_volume_path}'")

# COMMAND ----------

# MAGIC %md ## Cell 6 — S3 File Operations (boto3)
# MAGIC
# MAGIC **List**     : boto3 `list_objects_v2` paginator — handles 1000+ files
# MAGIC **Copy**     : boto3 `get_object` → 8MB chunks → `open()` write to Volume
# MAGIC               Data streams through driver node (S3 → driver → Volume FUSE)
# MAGIC               Why not s3.copy_object(): dest bucket policy denies PutObject
# MAGIC               for UC temp credential role — Volume bypasses this via internal creds
# MAGIC **Checksum** : SHA256 computed IN THE SAME streaming pass as copy
# MAGIC               No second stream needed — halves S3 GET requests per file
# MAGIC **Verify**   : boto3 `head_object` vs `os.path.getsize` — zero bytes downloaded

# COMMAND ----------

# DBTITLE 1,Untitled
def list_s3_files_boto3(s3_client, bucket: str, prefix: str) -> Tuple[list, Optional[str]]:
    """
    List all files under s3://<bucket>/<prefix> using boto3 paginator.
    Returns a list of files and an error message if any.
    """
    paginator = s3_client.get_paginator('list_objects_v2')
    files     = []
    found_any = False

    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if page.get('KeyCount', 0) > 0:
                found_any = True
            for obj in page.get('Contents', []):
                if not obj['Key'].endswith('/'):   # skip folder placeholder keys
                    files.append({
                        'key' : obj['Key'],
                        'name': obj['Key'].split('/')[-1],
                        'size': obj['Size']
                    })
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code', '')
        logger.warning(f"S3 listing error [{code}] for {prefix}: {e}")
        return [], "UUID folder not found in S3"

    if not files and not found_any:
        return [], "UUID folder not found in S3"
    if not files and found_any:
        return [], "UUID folder exists but is empty in S3"

    return files, None


def stream_s3_to_volume_with_checksum(source_client, source_bucket: str,
                                      source_key: str, dest_file_path: str,
                                      chunk_size: int) -> str:
    """
    Stream file from S3 to UC Volume AND compute SHA256 checksum in ONE pass.

    Chunk flow per iteration:
        chunk = read 8MB from S3
        f.write(chunk)          → writes to Volume
        sha256.update(chunk)    → feeds into hash
        chunk discarded         → RAM freed
        → next chunk

    Memory usage: constant ~8MB regardless of file size.
    Thread-safe: each call writes to a unique dest_file_path.

    Returns: SHA256 hex string (64 chars)
    """
    import os
    os.makedirs(os.path.dirname(dest_file_path), exist_ok=True)

    response = source_client.get_object(Bucket=source_bucket, Key=source_key)
    sha256   = hashlib.sha256()

    with open(dest_file_path, 'wb') as f:
        for chunk in iter(lambda: response['Body'].read(chunk_size), b''):
            f.write(chunk)          # write chunk to Volume
            sha256.update(chunk)    # feed same chunk into hash

    return sha256.hexdigest()       # 64 character hex string


def verify_sizes(source_client, source_bucket: str, source_key: str,
                 dest_file_path: str) -> bool:
    """
    Verify copy by comparing S3 source size vs Volume file size.
    Source: HEAD request (zero bytes). Dest: os.path.getsize (local stat).
    """
    import os
    try:
        src_size  = source_client.head_object(
            Bucket=source_bucket, Key=source_key
        )['ContentLength']
        dest_size = os.path.getsize(dest_file_path)
        return src_size == dest_size
    except Exception as e:
        logger.warning(f"Size verification failed for {source_key}: {e}")
        return False


def copy_one_file(source_client, source_bucket: str, source_key: str,
                  dest_file_path: str, chunk_size: int) -> dict:
    """
    Copy ONE file from S3 to UC Volume with checksum and verification.
    Thread-safe — each call writes to a unique dest_file_path.

    Steps:
    1. Idempotency check — skip if already at destination
    2. stream_s3_to_volume_with_checksum() — copy + SHA256 in ONE stream pass
    3. verify_sizes() — HEAD request vs os.path.getsize (zero bytes downloaded)

    Returns dict: name, size, checksum, skipped
    """
    import os
    filename = source_key.split('/')[-1]

    # Idempotency — skip if already exists at destination Volume
    if os.path.exists(dest_file_path):
        logger.debug(f"Already exists at dest, skipping: {dest_file_path}")
        # Get source size for tracking record
        src_size = source_client.head_object(
            Bucket=source_bucket, Key=source_key
        )['ContentLength']
        return {'name': filename, 'size': src_size,
                'checksum': 'already_existed', 'skipped': True}

    # Step 1 — Stream from S3 to Volume + compute SHA256 in one pass
    try:
        checksum = stream_s3_to_volume_with_checksum(
            source_client, source_bucket, source_key,
            dest_file_path, chunk_size
        )
    except ClientError as e:
        classify_boto3_error(e)   # raises TransientError or PermanentError
    except Exception as e:
        # Clean up partial file if write failed midway
        if os.path.exists(dest_file_path):
            os.remove(dest_file_path)
        raise PermanentError(f"Stream failed for {source_key}: {e}") from e

    # Step 2 — Verify size: S3 HEAD vs Volume file size
    verified = verify_sizes(
        source_client, source_bucket, source_key, dest_file_path
    )
    if not verified:
        # Remove corrupt file so retry can re-copy cleanly
        if os.path.exists(dest_file_path):
            os.remove(dest_file_path)
        raise PermanentError(f"Size mismatch after copy — corrupt file removed: {source_key}")

    src_size = source_client.head_object(
        Bucket=source_bucket, Key=source_key
    )['ContentLength']

    logger.debug(f"✅ {filename} ({src_size} bytes) checksum: {checksum[:12]}...")
    return {'name': filename, 'size': src_size, 'checksum': checksum, 'skipped': False}

print("✅ S3 file operations ready")

# COMMAND ----------

# MAGIC %md ## Cell 7 — Delta Tracking Table
# MAGIC

# COMMAND ----------

# DBTITLE 1,Untitled
def init_tracking_table(config: ReplicationConfig):
    """
    Create Delta tracking table if not exists.
    Added vs V3: dest_bucket, dest_key, checksum, duration_seconds columns.
    """
    full_table = f"{config.tracking_schema}.{config.tracking_table}"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            request_id          STRING    NOT NULL,
            product_ins_id      BIGINT,
            product_type        STRING,
            file_name           STRING,
            source_bucket       STRING    NOT NULL,
            source_folder       STRING,              -- folder_name from folder_lookup_table join
            source_key          STRING    NOT NULL,  -- full S3 prefix for this UUID (folder/uuid/)
            dest_path           STRING    NOT NULL,  -- UC Volume destination folder
            status              STRING    NOT NULL,  -- PENDING/IN_PROGRESS/COMPLETED/FAILED/SKIPPED
            files_copied        INT,
            total_bytes         BIGINT,
            checksum            STRING,              -- SHA256 computed during copy stream
            error_message       STRING,              -- failure reason OR skip reason
            retry_count         INT,
            skipped_retry_count INT,              -- how many times SKIPPED has been retried
            duration_seconds    DOUBLE,           -- time taken to process this UUID
            created_timestamp   TIMESTAMP NOT NULL,
            started_timestamp   TIMESTAMP,
            completed_timestamp TIMESTAMP
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true'
        )
        COMMENT 'Tracks S3-to-Volume replication. Populated from ereg_silver.s3_file.'
    """)
    logger.info(f"Tracking table ready: {full_table}")
    print(f"✅ Tracking table ready: {full_table}")


def populate_pending_requests(config: ReplicationConfig) -> int:
    """
    Insert NEW rows from silver table as PENDING.

    KEY CHANGE — no live S3 discovery anymore:
    folder_name comes from a JOIN to folder_lookup_table on product_ins_id
    (or whatever folder_lookup_join_col is configured to). This is a pure
    Delta-to-Delta join — zero S3 API calls, zero driver-side Python loops.
    At 75M+ UUIDs this is the difference between ~2 hours of sequential
    list_objects_v2 calls and a Spark anti-join that completes in seconds
    to low minutes depending on how many NEW rows exist since last run.

    source_key is built directly in SQL as: folder_name || '/' || s3_file_id || '/'
    dest_path mirrors that folder structure under dest_volume_path.

    NOT EXISTS guard — fully idempotent, re-runs never create duplicates,
    and because it's checked BEFORE the join, only genuinely new silver
    rows get folder-resolved at all (not all 75M every run).

    LEFT JOIN (not INNER) — a silver row whose product_ins_id has no match
    in folder_lookup_table still gets inserted, with source_folder/source_key
    = NULL. It will be marked SKIPPED ("folder not found in lookup table")
    during processing rather than silently dropped — same visibility
    guarantee the old S3-discovery INNER JOIN gave you, but without needing
    S3 to know about it.
    """
    full_table = f"{config.tracking_schema}.{config.tracking_table}"

    spark.sql(f"""
        INSERT INTO {full_table}
            (request_id, product_ins_id, product_type, file_name,
             source_bucket, source_folder, source_key, dest_path,
             status, retry_count, skipped_retry_count, created_timestamp)

        SELECT
            s.s3_file_id                                                     AS request_id,
            s.product_ins_id,
            s.product_type,
            s.file_name,
            '{config.source_bucket}'                                        AS source_bucket,
            f.{config.folder_name_col}                                      AS source_folder,
            CASE
                WHEN f.{config.folder_name_col} IS NOT NULL
                THEN f.{config.folder_name_col} || '/' || s.s3_file_id || '/'
                ELSE NULL
            END                                                              AS source_key,
            CASE
                WHEN f.{config.folder_name_col} IS NOT NULL
                THEN '{config.dest_volume_path}/' || f.{config.folder_name_col} || '/' || s.s3_file_id
                ELSE '{config.dest_volume_path}/_unresolved/' || s.s3_file_id
            END                                                              AS dest_path,
            'PENDING'                                                       AS status,
            0                                                               AS retry_count,
            0                                                               AS skipped_retry_count,
            CURRENT_TIMESTAMP                                               AS created_timestamp

        FROM {config.silver_table} s
        LEFT JOIN {config.folder_lookup_table} f
            ON s.{config.folder_lookup_join_col} = f.{config.folder_lookup_join_col}
        WHERE NOT EXISTS (
            SELECT 1 FROM {full_table} t
            WHERE t.request_id = s.s3_file_id
        )
    """)

    pending_count = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {full_table} WHERE status = 'PENDING'
    """).collect()[0]['cnt']

    unresolved_count = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {full_table}
        WHERE status = 'PENDING' AND source_key IS NULL
    """).collect()[0]['cnt']

    logger.info(f"Pending requests: {pending_count} ({unresolved_count} unresolved folder)")
    print(f"✅ Pending requests to process: {pending_count}"
          + (f" — ⚠️ {unresolved_count} have no folder match (will be SKIPPED)" if unresolved_count else ""))
    return pending_count

print("✅ Delta table functions ready")

# COMMAND ----------

# MAGIC %md ## Cell 8 — Batch Status Updates
# MAGIC
# MAGIC
# MAGIC   mark_batch_in_progress() — ONE UPDATE for all UUIDs
# MAGIC   batch_update_status()    — ONE MERGE for all results
# MAGIC Added: checksum and duration_seconds in MERGE.

# COMMAND ----------

def mark_batch_in_progress(config: ReplicationConfig, request_ids: list):
    """Same as V3 — mark all UUIDs IN_PROGRESS in one UPDATE."""
    if not request_ids:
        return
    full_table = f"{config.tracking_schema}.{config.tracking_table}"
    ids_csv    = ", ".join(f"'{rid}'" for rid in request_ids)
    spark.sql(f"""
        UPDATE {full_table}
        SET status            = 'IN_PROGRESS',
            started_timestamp = CURRENT_TIMESTAMP,
            retry_count       = retry_count + 1
        WHERE request_id IN ({ids_csv})
    """)
    logger.info(f"Marked {len(request_ids)} requests IN_PROGRESS")


def batch_update_status(config: ReplicationConfig, results: list):
    
    
    if not results:
        return

    full_table  = f"{config.tracking_schema}.{config.tracking_table}"
    values_rows = []

    for r in results:
        safe_error    = (r.get('error_message') or '').replace("'", "''")
        safe_checksum = (r.get('checksum')       or '').replace("'", "''")
        error_val     = f"'{safe_error}'"    if safe_error    else 'NULL'
        checksum_val  = f"'{safe_checksum}'" if safe_checksum else 'NULL'
        files_val     = r.get('files_copied')     or 0
        bytes_val     = r.get('total_bytes')      or 0
        dur_val       = r.get('duration_seconds') or 0.0

        values_rows.append(
            f"('{r['request_id']}', '{r['status']}', "
            f"{files_val}, {bytes_val}, {checksum_val}, {error_val}, {dur_val})"
        )

    values_sql = ",\n        ".join(values_rows)

    spark.sql(f"""
        MERGE INTO {full_table} AS target
        USING (
            SELECT request_id, status, files_copied,
                   total_bytes, checksum, error_message, duration_seconds
            FROM (VALUES {values_sql})
            AS t(request_id, status, files_copied,
                 total_bytes, checksum, error_message, duration_seconds)
        ) AS source
        ON target.request_id = source.request_id
        WHEN MATCHED THEN UPDATE SET
            target.status               = source.status,
            target.files_copied         = source.files_copied,
            target.total_bytes          = source.total_bytes,
            target.checksum             = source.checksum,
            target.error_message        = source.error_message,
            target.duration_seconds     = source.duration_seconds,
            target.completed_timestamp  = CURRENT_TIMESTAMP,
            -- increment skipped_retry_count only when this result is SKIPPED,
            -- otherwise leave it untouched
            target.skipped_retry_count  = CASE
                WHEN source.status = 'SKIPPED'
                THEN COALESCE(target.skipped_retry_count, 0) + 1
                ELSE target.skipped_retry_count
            END
    """)
    logger.info(f"Batch MERGE complete: {len(results)} rows")

print("✅ Batch update functions ready")

# COMMAND ----------

# MAGIC %md ## Cell 9 — Core: Replicate ONE UUID Folder
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Untitled
def replicate_uuid_folder(config: ReplicationConfig,
                          source_client,
                          row: dict) -> dict:
    """
    Replicate all files under one UUID folder from S3 to UC Volume.
    Runs inside a ThreadPoolExecutor thread — fully thread-safe.

    Steps:
    1. List files via boto3 paginator 
    2. If no files → SKIPPED with specific reason 
    3. Copy files in parallel via nested ThreadPoolExecutor
       Each file: S3 get_object → Volume write (8MB chunked streaming)
    4. Verify each file: S3 HEAD size vs os.path.getsize
    5. Compute SHA256 checksum via 1MB chunked streaming
    6. Return result dict
    """
    request_id    = row['request_id']
    source_bucket = row['source_bucket']
    source_prefix = row['source_key']    # "folder/<uuid>/" — resolved via folder_lookup_table join
    # Destination: mirrors S3 folder structure (e.g. /Volumes/.../folder/<uuid>/)
    dest_folder   = row['dest_path']
    job_start     = time.time()

    result = {
        'request_id'      : request_id,
        'status'          : ReplicationStatus.FAILED.value,
        'files_copied'    : 0,
        'total_bytes'     : 0,
        'checksum'        : None,
        'error_message'   : None,
        'duration_seconds': 0.0
    }

    # source_key is NULL when this UUID's product_ins_id had no match in
    # folder_lookup_table — skip immediately, no point attempting an S3
    # list call for a folder we already know we can't resolve.
    if not source_prefix:
        result['status']           = ReplicationStatus.SKIPPED.value
        result['error_message']    = "No folder match in folder_lookup_table for this product_ins_id"
        result['duration_seconds'] = round(time.time() - job_start, 2)
        return result

    for attempt in range(config.max_retries):
        try:
            # ── Step 1: List files ─────────────────────────────────────────────
            files, skip_reason = list_s3_files_boto3(
                source_client, source_bucket, source_prefix
            )

            if not files:
                # Same SKIPPED distinction as V3
                logger.warning(f"Skipping {request_id}: {skip_reason}")
                result['status']           = ReplicationStatus.SKIPPED.value
                result['error_message']    = skip_reason
                result['duration_seconds'] = round(time.time() - job_start, 2)
                return result

            # ── Step 2: Copy files in parallel ────────────────────────────────
            total_bytes   = 0
            files_copied  = 0
            copy_errors   = []
            last_checksum = None

            with ThreadPoolExecutor(max_workers=config.file_workers) as file_exec:
                future_to_file = {
                    file_exec.submit(
                        copy_one_file,
                        source_client,
                        source_bucket,
                        f['key'],
                        f"{dest_folder}/{f['name']}",
                        config.chunk_size
                    ): f
                    for f in files
                }

                for future in as_completed(future_to_file):
                    f = future_to_file[future]
                    try:
                        file_result   = future.result()
                        last_checksum = file_result['checksum']
                        if file_result['skipped']:
                            logger.debug(f"Already existed at dest: {f['name']}")
                        else:
                            total_bytes  += file_result['size']
                            files_copied += 1
                    except TransientError as e:
                        copy_errors.append(f"{f['name']}: {str(e)}")
                        logger.warning(f"Transient error {f['name']}: {e}")
                    except PermanentError as e:
                        copy_errors.append(f"{f['name']}: {str(e)}")
                        logger.error(f"Permanent error {f['name']}: {e}")
                    except Exception as e:
                        copy_errors.append(f"{f['name']}: {str(e)}")
                        logger.error(f"Unexpected error {f['name']}: {e}")

            if copy_errors:
                result['error_message']    = f"{len(copy_errors)} file(s) failed: {'; '.join(copy_errors[:3])}"
                result['status']           = ReplicationStatus.FAILED.value
                result['duration_seconds'] = round(time.time() - job_start, 2)
                return result

            result['status']           = ReplicationStatus.COMPLETED.value
            result['files_copied']     = files_copied
            result['total_bytes']      = total_bytes
            result['checksum']         = last_checksum
            result['duration_seconds'] = round(time.time() - job_start, 2)
            return result

        except TransientError as e:
            result['error_message'] = str(e)
            if attempt < config.max_retries - 1:
                wait_time = 2 ** attempt
                logger.warning(f"Transient error {request_id}, retry {attempt+1} in {wait_time}s: {e}")
                time.sleep(wait_time)
                continue
            logger.error(f"Max retries exceeded for {request_id}: {e}")
            break

        except PermanentError as e:
            result['error_message'] = str(e)
            logger.error(f"Permanent error {request_id}: {e}")
            break

        except Exception as e:
            result['error_message'] = str(e)
            logger.error(f"Unexpected error {request_id}: {e}", exc_info=True)
            break

    result['duration_seconds'] = round(time.time() - job_start, 2)
    return result

print("✅ replicate_uuid_folder ready")

# COMMAND ----------

# MAGIC %md ## Cell 10 — Main Processing Loop
# MAGIC

# COMMAND ----------

# DBTITLE 1,Untitled
def process_pending_requests(config: ReplicationConfig,
                             source_client) -> int:
    """
    Flow:
    1. Fetch ALL PENDING / retryable FAILED / retryable SKIPPED rows
       (no batch_size cap — job is run frequently and processes whatever
       backlog exists each time it executes, rather than capping per-run
       and forcing extra re-runs to clear a large queue)
    2. Mark all IN_PROGRESS in ONE UPDATE
    3. Process all UUIDs in parallel (uuid_workers threads)
    4. Write all results in ONE MERGE

    SKIPPED rows ARE retried, but only up to skipped_max_retries times —
    previously SKIPPED had no retry cap at all, which meant permanently
    unresolvable UUIDs (e.g. truly missing from S3) would be re-attempted
    forever, every single run, wasting cycles indefinitely.
    """
    full_table = f"{config.tracking_schema}.{config.tracking_table}"

    pending_df = spark.sql(f"""
        SELECT request_id, source_bucket, source_key, dest_path,
               retry_count, skipped_retry_count
        FROM {full_table}
        WHERE status = 'PENDING'
           OR (status = 'FAILED'  AND retry_count         < {config.max_retries})
           OR (status = 'SKIPPED' AND COALESCE(skipped_retry_count, 0) < {config.skipped_max_retries})
        ORDER BY created_timestamp ASC
    """)

    rows = [row.asDict() for row in pending_df.collect()]

    if not rows:
        logger.info("No pending replication requests found.")
        print("ℹ️  No pending requests.")
        return 0

    print(f"\nProcessing {len(rows)} UUID folders "
          f"({config.uuid_workers} UUID workers × {config.file_workers} file workers)...")

    # Step 1 — ONE UPDATE for entire batch
    mark_batch_in_progress(config, [r['request_id'] for r in rows])

    # Step 2 — parallel UUID processing
    results = []
    with ThreadPoolExecutor(max_workers=config.uuid_workers) as uuid_exec:
        future_to_row = {
            uuid_exec.submit(
                replicate_uuid_folder,
                config, source_client, row
            ): row
            for row in rows
        }

        for future in as_completed(future_to_row):
            row = future_to_row[future]
            try:
                result = future.result()
                results.append(result)
                icon = {'COMPLETED':'✅','SKIPPED':'⚠️ ','FAILED':'❌'}.get(result['status'], '?')
                logger.info(
                    f"{icon} [{result['status']}] {result['request_id']} — "
                    f"{result['files_copied']} files, {result['total_bytes']} bytes, "
                    f"{result['duration_seconds']}s"
                    + (f" | {result['error_message']}" if result['error_message'] else "")
                )
            except Exception as e:
                logger.error(f"Thread crashed for {row['request_id']}: {e}", exc_info=True)
                results.append({
                    'request_id'      : row['request_id'],
                    'status'          : ReplicationStatus.FAILED.value,
                    'files_copied'    : 0,
                    'total_bytes'     : 0,
                    'checksum'        : None,
                    'error_message'   : f"Thread crashed: {str(e)}",
                    'duration_seconds': 0.0
                })

    # Step 3 — ONE MERGE for all results
    batch_update_status(config, results)

    successful = sum(1 for r in results if r['status'] == ReplicationStatus.COMPLETED.value)
    skipped    = sum(1 for r in results if r['status'] == ReplicationStatus.SKIPPED.value)
    failed     = sum(1 for r in results if r['status'] == ReplicationStatus.FAILED.value)

    print(f"\nBatch done — ✅ Completed: {successful} | ⚠️  Skipped: {skipped} | ❌ Failed: {failed}")
    return successful

print("✅ process_pending_requests ready")

# COMMAND ----------

# MAGIC %md ## Cell 11 — Summary

# COMMAND ----------

def print_summary(config: ReplicationConfig):
    
    full_table = f"{config.tracking_schema}.{config.tracking_table}"

    print("\n===== S3 Replication Summary =====")
    spark.sql(f"""
        SELECT
            status,
            COUNT(*)                                   AS request_count,
            SUM(files_copied)                          AS total_files,
            ROUND(SUM(total_bytes)/1024/1024, 2)       AS total_mb,
            ROUND(AVG(duration_seconds), 2)            AS avg_secs,
            ROUND(MAX(duration_seconds), 2)            AS max_secs
        FROM {full_table}
        GROUP BY status ORDER BY status
    """).show(truncate=False)

    print("\n===== SKIPPED Breakdown (Why?) =====")
    spark.sql(f"""
        SELECT
            error_message               AS skip_reason,
            COUNT(*)                    AS count,
            MAX(skipped_retry_count)    AS max_retries_done,
            SUM(CASE WHEN COALESCE(skipped_retry_count, 0) >= {config.skipped_max_retries}
                THEN 1 ELSE 0 END)      AS permanently_skipped
        FROM {full_table}
        WHERE status = 'SKIPPED'
        GROUP BY error_message ORDER BY error_message
    """).show(truncate=False)

    print("\n===== FAILED Details =====")
    spark.sql(f"""
        SELECT request_id, file_name, error_message, retry_count, duration_seconds
        FROM {full_table}
        WHERE status = 'FAILED'
        ORDER BY completed_timestamp DESC LIMIT 20
    """).show(truncate=False)

print("✅ print_summary ready")

# COMMAND ----------

# MAGIC %md ## Cell 12 — Run

# COMMAND ----------

# DBTITLE 1,Untitled
def main():
    logger.info("=== S3-to-Volume Replication Job Started ===")
    print("=== S3-to-Volume Replication Job Started ===\n")

    config = load_config()

    # 1. Create Delta tracking table
    init_tracking_table(config)

    # 2. Populate PENDING from silver (incremental, idempotent)
    populate_pending_requests(config)

    # 3. Process — S3 source → UC Volume destination
    #    source_client built in Cell 5 (boto3, thread-safe)
    #    Destination is UC Volume — no S3 client needed
    successful = process_pending_requests(config, source_client)

    # 4. Summary
    print_summary(config)

    logger.info(f"=== Job Completed: {successful} UUID folder(s) replicated ===")
    print(f"\n=== Job Completed: {successful} UUID folder(s) replicated ===")


main()