# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # boto3 S3 Copy Test Notebook
# MAGIC
# MAGIC Tests copying one file from S3 to Databricks Volume using boto3.
# MAGIC
# MAGIC **Run cells one by one and check output at each step.**
# MAGIC
# MAGIC Steps:
# MAGIC 1. Install boto3
# MAGIC 2. Get temporary AWS credentials from Unity Catalog
# MAGIC 3. Build boto3 S3 client
# MAGIC 4. List files under one UUID folder in S3
# MAGIC 5. Copy one file to Volume
# MAGIC 6. Verify the copy succeeded

# COMMAND ----------

# MAGIC %md ## ⚙️ Config — Change These Values Before Running

# COMMAND ----------

# ── CHANGE THESE ───────────────────────────────────────────────────────────────
EXTERNAL_LOCATION_PATH = "s3://qa-ereg-us-east-2-ses-qa-ereg/sw1-qa-ereg"
SOURCE_BUCKET          = "qa-ereg-us-east-2-ses-qa-ereg"
SOURCE_PREFIX          = "sw1-qa-ereg"
AWS_REGION             = "us-east-2"
DEST_VOLUME_PATH       = "/Volumes/dbx_dev_data_refine/export/cc"

# Replace with any real UUID from your s3_file silver table
# Run this to get one: spark.sql("SELECT s3_file_id FROM dbx_dev_data_refine.ereg_silver.s3_file LIMIT 1").show()
TEST_UUID              = "0007c8d8-7a77-45fd-8eaf-b0251e9538f4"
# ──────────────────────────────────────────────────────────────────────────────

print("Config loaded:")
print(f"  Source bucket : s3://{SOURCE_BUCKET}/{SOURCE_PREFIX}/")
print(f"  Test UUID     : {TEST_UUID}")
print(f"  Destination   : {DEST_VOLUME_PATH}/{TEST_UUID}/")

# COMMAND ----------

# MAGIC %md ## Cell 1 — Install boto3

# COMMAND ----------

# Install boto3 on the cluster
# This only needs to run once per cluster session
%pip install boto3 --quiet
%pip install --upgrade databricks-sdk

# Restart Python after pip install so boto3 is importable
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Cell 2 — Get Temporary AWS Credentials from Unity Catalog
# MAGIC
# MAGIC Unity Catalog generates short-lived AWS keys scoped to your external location.
# MAGIC No hardcoded AWS credentials anywhere in code.
# MAGIC Keys expire in approximately 1 hour.

# COMMAND ----------

# Re-run config after restartPython (restart clears all variables)
EXTERNAL_LOCATION_PATH = "s3://qa-ereg-us-east-2-ses-qa-ereg/sw1-qa-ereg"
SOURCE_BUCKET          = "qa-ereg-us-east-2-ses-qa-ereg"
SOURCE_PREFIX          = "sw1-qa-ereg"
AWS_REGION             = "us-east-2"
DEST_VOLUME_PATH       = "/Volumes/dbx_dev_data_refine/export/cc"
TEST_UUID              = "0007c8d8-7a77-45fd-8eaf-b0251e9538f4"

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import PathOperation

def get_temp_credentials(external_location_path: str):
    """
    Ask Unity Catalog for temporary AWS credentials
    for the registered external location path.
    Returns credentials object with access_key_id,
    secret_access_key and session_token.
    """
    ws    = WorkspaceClient()
    creds = ws.temporary_path_credentials.generate_temporary_path_credentials(
        external_location_path,
        PathOperation.PATH_READ

    )
    return creds


print("Requesting temporary credentials from Unity Catalog...")
print(f"External location: {EXTERNAL_LOCATION_PATH}")
print()

creds = get_temp_credentials(EXTERNAL_LOCATION_PATH)

# Print partial values only — never print full credentials
print("✅ Credentials generated successfully")
print(f"   Access Key ID : {creds.aws_temp_credentials.access_key_id[:12]}...")
print(f"   Session Token : {creds.aws_temp_credentials.session_token[:20]}...")
print()
print("These credentials are short-lived (~1 hour) and scoped to the external location only.")

# COMMAND ----------

# MAGIC %md ## Cell 3 — Build boto3 S3 Client

# COMMAND ----------

import boto3

def build_s3_client(creds, region: str):
    """
    Build a boto3 S3 client using the
    Unity Catalog temporary credentials.
    """
    session = boto3.Session(
        aws_access_key_id     = creds.aws_temp_credentials.access_key_id,
        aws_secret_access_key = creds.aws_temp_credentials.secret_access_key,
        aws_session_token     = creds.aws_temp_credentials.session_token,
        region_name           = region
    )
    return session.client('s3')


print("Building boto3 S3 client with temporary credentials...")

s3_client = build_s3_client(creds, region=AWS_REGION)

# Quick sanity check — try listing the bucket (top level)
try:
    resp = s3_client.head_bucket(Bucket=SOURCE_BUCKET)
    print(f"✅ boto3 client working — bucket '{SOURCE_BUCKET}' is accessible")
except Exception as e:
    print(f"❌ Cannot reach bucket: {e}")

# COMMAND ----------

# MAGIC %md ## Cell 4 — List Files Under the Test UUID Folder in S3

# COMMAND ----------

def list_files_under_uuid(s3_client, bucket: str, prefix: str, uuid: str) -> list:
    """
    List all files under s3://<bucket>/<prefix>/<uuid>/
    Uses paginator — safe for folders with 1000+ objects.
    Returns list of dicts: key, name, size.
    """
    full_prefix = f"{prefix}/{uuid}/"
    paginator   = s3_client.get_paginator('list_objects_v2')
    files       = []

    for page in paginator.paginate(Bucket=bucket, Prefix=full_prefix):
        for obj in page.get('Contents', []):
            if not obj['Key'].endswith('/'):    # skip folder placeholder keys
                files.append({
                    'key' : obj['Key'],
                    'name': obj['Key'].split('/')[-1],
                    'size': obj['Size']
                })

    return files


print(f"Listing S3 files under UUID: {TEST_UUID}")
print(f"Path: s3://{SOURCE_BUCKET}/{SOURCE_PREFIX}/{TEST_UUID}/")
print()

files = list_files_under_uuid(s3_client, SOURCE_BUCKET, SOURCE_PREFIX, TEST_UUID)

if not files:
    print("⚠️  No files found under this UUID.")
    print("   Either the UUID folder doesn't exist in S3")
    print("   or the UUID is wrong — update TEST_UUID in config cell and re-run.")
else:
    print(f"✅ Found {len(files)} file(s):\n")
    for i, f in enumerate(files, 1):
        size_kb = round(f['size'] / 1024, 2)
        print(f"   {i}. {f['name']}  ({size_kb} KB)")
        print(f"      Full S3 key: {f['key']}")

# COMMAND ----------

# MAGIC %md ## Cell 5 — Copy First File from S3 to Volume

# COMMAND ----------

import os

def copy_one_file_boto3(s3_client, source_bucket: str, source_key: str,
                        dest_volume_path: str, uuid: str, filename: str) -> int:
    """
    Copy ONE file from S3 to a Databricks Volume using boto3.

    How it works:
    → boto3 get_object streams file from S3 in 1MB chunks
    → Each chunk written immediately to Volume path
    → Memory stays constant regardless of file size
    → Returns bytes written, or -1 if file already existed (skipped)

    Note: Data streams through driver node because destination
    is a Volume. True server-side boto3 s3.copy() only works S3-to-S3.
    """
    dest_folder = f"{dest_volume_path}/{uuid}/"
    dest_path   = f"{dest_folder}{filename}"

    # Idempotency — skip if already copied
    if os.path.exists(dest_path):
        print(f"⚠️  File already exists at destination — skipping")
        print(f"   {dest_path}")
        return -1

    # Create UUID subfolder in Volume if it doesn't exist yet
    os.makedirs(dest_folder, exist_ok=True)

    print(f"Source : s3://{source_bucket}/{source_key}")
    print(f"Dest   : {dest_path}")
    print(f"Copying in 1MB chunks...")
    print()

    # Stream from S3 → write to Volume in 1MB chunks
    response      = s3_client.get_object(Bucket=source_bucket, Key=source_key)
    bytes_written = 0
    chunk_size    = 1024 * 1024   # 1MB

    with open(dest_path, 'wb') as f:
        for chunk in iter(lambda: response['Body'].read(chunk_size), b''):
            f.write(chunk)
            bytes_written += len(chunk)

    return bytes_written


if not files:
    print("⚠️  No files to copy — run Cell 4 first and fix the UUID.")
else:
    # Copy only the FIRST file for this test
    test_file     = files[0]
    print(f"Testing copy of: {test_file['name']}")
    print(f"Size            : {round(test_file['size']/1024, 2)} KB")
    print()

    bytes_written = copy_one_file_boto3(
        s3_client        = s3_client,
        source_bucket    = SOURCE_BUCKET,
        source_key       = test_file['key'],
        dest_volume_path = DEST_VOLUME_PATH,
        uuid             = TEST_UUID,
        filename         = test_file['name']
    )

    if bytes_written > 0:
        print(f"✅ Copy complete — {bytes_written} bytes written")

# COMMAND ----------

# MAGIC %md ## Cell 6 — Verify Copy Succeeded

# COMMAND ----------

def verify_copy(s3_client, source_bucket: str, source_key: str, dest_path: str) -> bool:
    """
    Verify copy integrity by comparing source and destination file sizes.
    Uses HEAD request for source — no data downloaded, metadata only.
    """
    # Source size via S3 HEAD (zero data downloaded)
    head     = s3_client.head_object(Bucket=source_bucket, Key=source_key)
    src_size = head['ContentLength']

    # Destination size from Volume filesystem
    dest_size = os.path.getsize(dest_path)

    print(f"Source size (S3)   : {src_size} bytes")
    print(f"Dest size (Volume) : {dest_size} bytes")

    if src_size == dest_size:
        print(f"✅ Size match — copy verified successfully")
        return True
    else:
        diff = abs(src_size - dest_size)
        print(f"❌ Size MISMATCH — {diff} bytes difference — file may be corrupt")
        return False


if not files:
    print("⚠️  No files — run Cell 4 first.")
elif bytes_written == -1:
    print("File was skipped (already existed) — checking existing file size...")
    dest_path = f"{DEST_VOLUME_PATH}/{TEST_UUID}/{files[0]['name']}"
    verify_copy(s3_client, SOURCE_BUCKET, files[0]['key'], dest_path)
elif bytes_written > 0:
    dest_path = f"{DEST_VOLUME_PATH}/{TEST_UUID}/{files[0]['name']}"
    print(f"Verifying: {dest_path}")
    print()
    verify_copy(s3_client, SOURCE_BUCKET, files[0]['key'], dest_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ What to Check After Running
# MAGIC
# MAGIC 1. Cell 2 → Should print: "Credentials generated successfully"
# MAGIC 2. Cell 3 → Should print: "boto3 client working — bucket is accessible"
# MAGIC 3. Cell 4 → Should list files under your UUID folder
# MAGIC 4. Cell 5 → Should print bytes written
# MAGIC 5. Cell 6 → Should print "Size match — copy verified"
# MAGIC
# MAGIC Also verify in Catalog Explorer:
# MAGIC Catalog → poc_cc → dev_bronze → Volumes → s3_replication → Files
# MAGIC You should see: <TEST_UUID>/<filename>

# COMMAND ----------

DEST_BUCKET = "dbx-dev-data-export"
DEST_PREFIX = "ereg/refine/eReg_QA/"

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import PathOperation

def get_temp_credentials(path):
    ws = WorkspaceClient()

    response = ws.temporary_path_credentials.generate_temporary_path_credentials(
        path,
        PathOperation.PATH_READ_WRITE   # ✅ IMPORTANT: write access needed
    )

    aws = response.aws_temp_credentials

    return {
        "access_key": aws.access_key_id,
        "secret_key": aws.secret_access_key,
        "session_token": aws.session_token
    }

# COMMAND ----------

import boto3

EXTERNAL_LOCATION_PATH = "s3://dbx-dev-data-export/ereg/refine"

creds = get_temp_credentials(EXTERNAL_LOCATION_PATH)

session = boto3.Session(
    aws_access_key_id=creds["access_key"],
    aws_secret_access_key=creds["secret_key"],
    aws_session_token=creds["session_token"],
    region_name="us-east-2"
)

s3_client = session.client("s3")

# COMMAND ----------

source_bucket = "qa-ereg-us-east-2-ses-qa-ereg"
source_key = "sw1-qa-ereg/sample.pdf"

dest_bucket = "dbx-dev-data-export"
dest_key = "ereg/refine/eReg_QA/sample.pdf"

s3_client.copy_object(
    CopySource={
        "Bucket": source_bucket,
        "Key": source_key
    },
    Bucket=dest_bucket,
    Key=dest_key
)

print("✅ File copied successfully")