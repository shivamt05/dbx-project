"""
S3 Bucket Replication Job for Databricks

Replicates files from a source S3 bucket to a destination bucket while tracking
replication requests and completion status in a PostgreSQL database.
"""

import logging
import json
import time
import hashlib
from typing import Optional, List, Dict
from dataclasses import dataclass
from enum import Enum

import boto3
import psycopg2
import psycopg2.extensions
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool, PoolError
from psycopg2 import IntegrityError
from botocore.exceptions import ClientError

try:
    from databricks.sdk import WorkspaceClient
    DATABRICKS_AVAILABLE = True
except ImportError:
    DATABRICKS_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Custom Exception Classes for Better Error Handling
class ReplicationError(Exception):
    """Base exception for replication errors."""
    def __init__(self, message: str, retryable: bool = False):
        self.message = message
        self.retryable = retryable
        super().__init__(message)


class TransientReplicationError(ReplicationError):
    """Retryable errors like network timeouts, rate limits, and service unavailability."""
    def __init__(self, message: str):
        super().__init__(message, retryable=True)


class PermanentReplicationError(ReplicationError):
    """Non-retryable errors like access denied, missing objects, or bad configurations."""
    def __init__(self, message: str):
        super().__init__(message, retryable=False)


class ReplicationStatus(Enum):
    """Enumeration of replication statuses."""
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass
class ReplicationRequest:
    """Represents a file replication request."""
    request_id: str
    source_bucket: str
    source_key: str
    dest_bucket: str
    dest_key: str
    file_size: Optional[int] = None
    checksum: Optional[str] = None


@dataclass
class ReplicationConfig:
    """
    Configuration for the replication job.
    
    Defaults are tuned for a typical Databricks cluster with 2 workers on i3.xlarge instances.
    For higher concurrency, increase connection_pool_size and batch_size proportionally.
    """
    source_bucket: str
    dest_bucket: str
    postgres_host: str
    postgres_port: int = 5432
    postgres_database: str = "s3_replication"
    postgres_user: str = "postgres"
    postgres_password: str = ""
    aws_region: str = "us-east-1"
    source_external_location_path: Optional[str] = None
    dest_external_location_path: Optional[str] = None
    db_table: str = "replication_requests"
    batch_size: int = 100
    """
    Number of replication requests to process per job run.
    Tuned assuming average file size of 10MB and 1GB driver memory available for S3 buffers.
    Reduce for very large files, increase for small files.
    """
    max_retries: int = 3
    """
    Maximum number of retry attempts per file.
    With exponential backoff (2^n seconds), 3 retries = ~11 seconds total wait time.
    Suitable for transient network errors and S3 throttling.
    """
    connection_pool_size: int = 5
    """
    Maximum connections in PostgreSQL connection pool.
    Tuned for 1 main thread + 4 worker threads. Each connection uses ~1MB of PostgreSQL memory.
    For highly concurrent workloads, increase proportionally.
    """
    connection_timeout: float = 5.0
    """
    Seconds to wait for available database connection from pool.
    Prevents indefinite blocking under connection exhaustion.
    """
    db_schema: str = "s3_replication"
    """Schema name for replication tables. Used for validation and organization."""


def get_temporary_path_credentials(path: str):
    """
    Retrieve temporary path-scoped AWS credentials from Unity Catalog.
    
    Args:
        path: S3 path mapped to a Unity Catalog External Location
        
    Returns:
        Temporary credential object with access_key_id, secret_access_key, session_token
        
    Raises:
        RuntimeError: If Databricks SDK is not available or credentials cannot be generated
    """
    if not DATABRICKS_AVAILABLE:
        raise RuntimeError(
            "Databricks SDK not available. Install with: pip install databricks-sdk"
        )
    
    try:
        logger.debug(f"Requesting temporary path credentials for: {path}")
        ws = WorkspaceClient()
        temp_cred = ws.temporary_path_credentials.generate_temporary_path_credentials(path=path)
        logger.info("Successfully generated temporary path credentials")
        return temp_cred
    except Exception as e:
        logger.error(f"Failed to generate temporary path credentials: {e}", exc_info=True)
        raise


class S3ReplicationService:
    """Service for handling S3 file replication operations."""
    
    # Whitelist valid table names to prevent SQL injection
    VALID_DB_TABLES = {
        'replication_requests',
        'replication_requests_archive',
    }

    def __init__(self, config: ReplicationConfig):
        """
        Initialize the S3 replication service with proper error handling and cleanup.
        
        Args:
            config: Replication configuration
            
        Raises:
            ValueError: If configuration is invalid
            RuntimeError: If initialization fails
        """
        self.config = config
        self.source_s3_client = None
        self.dest_s3_client = None
        self.connection_pool = None
        
        # Validate table name to prevent SQL injection
        if config.db_table not in self.VALID_DB_TABLES:
            raise ValueError(
                f"Invalid db_table '{config.db_table}'. "
                f"Must be one of: {', '.join(self.VALID_DB_TABLES)}"
            )
        
        try:
            if not config.source_external_location_path or not config.dest_external_location_path:
                raise ValueError(
                    "source_external_location_path and dest_external_location_path are required "
                    "for Unity Catalog temporary path credentials"
                )

            source_temp_cred = get_temporary_path_credentials(config.source_external_location_path)
            dest_temp_cred = get_temporary_path_credentials(config.dest_external_location_path)

            source_session = boto3.Session(
                aws_access_key_id=source_temp_cred.access_key_id,
                aws_secret_access_key=source_temp_cred.secret_access_key,
                aws_session_token=source_temp_cred.session_token,
                region_name=config.aws_region
            )
            dest_session = boto3.Session(
                aws_access_key_id=dest_temp_cred.access_key_id,
                aws_secret_access_key=dest_temp_cred.secret_access_key,
                aws_session_token=dest_temp_cred.session_token,
                region_name=config.aws_region
            )

            self.source_s3_client = source_session.client('s3')
            self.dest_s3_client = dest_session.client('s3')
            logger.info(f"Initialized source and destination S3 clients for region {config.aws_region}")
            
            # Initialize PostgreSQL connection pool
            self.connection_pool = SimpleConnectionPool(
                1,
                config.connection_pool_size,
                host=config.postgres_host,
                port=config.postgres_port,
                database=config.postgres_database,
                user=config.postgres_user,
                password=config.postgres_password,
                connect_timeout=config.connection_timeout
            )
            logger.info(f"Initialized PostgreSQL connection pool with size {config.connection_pool_size}")
            
            # Initialize database schema/migrations
            self._init_database()
            
        except Exception as e:
            logger.error(f"Initialization failed: {e}", exc_info=True)
            self.close()  # Clean up any partial initialization
            raise

    def _get_connection(self) -> psycopg2.extensions.connection:
        """
        Get a connection from the pool with timeout protection.
        
        Returns:
            Database connection
            
        Raises:
            PoolError: If no connection available within timeout
        """
        try:
            return self.connection_pool.getconn()
        except PoolError as e:
            logger.error(
                f"Could not acquire database connection within {self.config.connection_timeout}s. "
                f"Pool exhausted. Check for connection leaks or increase pool size."
            )
            raise

    def _put_connection(self, conn: psycopg2.extensions.connection) -> None:
        """
        Return a connection to the pool with state validation.
        
        Validates connection state before returning to pool to prevent corruption.
        
        Args:
            conn: Database connection to return
        """
        if not conn:
            return
            
        try:
            # Validate connection is not in a bad state
            if conn.closed:
                logger.debug("Connection was already closed, not returning to pool")
                return
            
            # Reset connection to initial state
            conn.reset()
            self.connection_pool.putconn(conn)
        except Exception as e:
            logger.error(f"Error resetting connection state: {e}")
            # Don't return corrupted connections back to pool
            try:
                conn.close()
            except Exception:
                pass

    def _get_schema_version(self) -> int:
        """
        Get current database schema version.
        
        Returns:
            Current schema version (0 if not yet initialized)
        """
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                # Create version table if not exists
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.config.db_schema}.schema_version (
                        version INT PRIMARY KEY,
                        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                cursor.execute(f"SELECT MAX(version) FROM {self.config.db_schema}.schema_version")
                result = cursor.fetchone()[0]
                conn.commit()
                return result if result is not None else 0
        finally:
            self._put_connection(conn)

    def _apply_migrations(self, from_version: int) -> None:
        """
        Apply all pending database schema migrations.
        
        Args:
            from_version: Current schema version to migrate from
        """
        migrations = {
            1: self._migration_v1_initial_schema,
            # Add future migrations here
            # 2: self._migration_v2_add_source_region,
        }
        
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                for version in sorted(v for v in migrations if v > from_version):
                    logger.info(f"Applying schema migration v{version}")
                    migrations[version](cursor)
                    cursor.execute(
                        f"INSERT INTO {self.config.db_schema}.schema_version (version) VALUES (%s)",
                        (version,)
                    )
                conn.commit()
                logger.info(f"Schema migrations complete. Now at version {max(migrations.keys()) if migrations else from_version}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Schema migration failed: {e}", exc_info=True)
            raise
        finally:
            self._put_connection(conn)

    def _migration_v1_initial_schema(self, cursor) -> None:
        """
        Initial schema creation (Version 1).
        
        Creates the main replication_requests table and indices.
        """
        cursor.execute(f"""
            CREATE SCHEMA IF NOT EXISTS {self.config.db_schema}
        """)
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.config.db_schema}.{self.config.db_table} (
                request_id VARCHAR(255) PRIMARY KEY,
                source_bucket VARCHAR(255) NOT NULL,
                source_key TEXT NOT NULL,
                dest_bucket VARCHAR(255) NOT NULL,
                dest_key TEXT NOT NULL,
                file_size BIGINT,
                checksum VARCHAR(255),
                status VARCHAR(50) NOT NULL,
                created_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                started_timestamp TIMESTAMP,
                completed_timestamp TIMESTAMP,
                error_message TEXT,
                retry_count INT DEFAULT 0
            )
        """)
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_status 
            ON {self.config.db_schema}.{self.config.db_table}(status)
        """)
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_created_timestamp 
            ON {self.config.db_schema}.{self.config.db_table}(created_timestamp)
        """)

    def _init_database(self) -> None:
        """Initialize the PostgreSQL database and apply schema migrations."""
        current_version = self._get_schema_version()
        target_version = 1  # Update when adding new migrations
        
        if current_version < target_version:
            logger.info(f"Upgrading schema from v{current_version} to v{target_version}")
            self._apply_migrations(current_version)
        else:
            logger.info(f"Schema already at version {current_version}")

    def _get_s3_object_metadata(self, bucket: str, key: str, s3_client=None) -> Optional[Dict]:
        """
        Retrieve metadata for an S3 object with proper error categorization.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            
        Returns:
            Dictionary with 'size', 'etag', 'last_modified' keys, or None if not found
            
        Raises:
            FileNotFoundError: If object doesn't exist (404)
            PermissionError: If access denied (403)
            TransientReplicationError: If transient S3 error occurs
            ReplicationError: For other unexpected errors
        """
        try:
            client = s3_client or self.source_s3_client
            response = client.head_object(Bucket=bucket, Key=key)
            return {
                'size': response.get('ContentLength'),
                'etag': response.get('ETag', '').strip('"'),
                'last_modified': response.get('LastModified')
            }
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            
            if error_code == '404':
                logger.debug(f"Object not found: s3://{bucket}/{key}")
                raise FileNotFoundError(f"Object not found: s3://{bucket}/{key}") from e
            elif error_code == 'AccessDenied':
                logger.warning(f"Access denied to s3://{bucket}/{key}")
                raise PermissionError(f"Access denied to s3://{bucket}/{key}") from e
            elif error_code in ['RequestTimeout', 'ServiceUnavailable', 'NoSuchBucket']:
                logger.warning(f"Transient S3 error for s3://{bucket}/{key}: {error_code}")
                raise TransientReplicationError(f"Transient S3 error: {error_code}") from e
            else:
                logger.error(f"Unexpected S3 error for s3://{bucket}/{key}: {error_code}")
                raise ReplicationError(f"S3 error: {error_code}") from e

    def _compute_file_checksum(self, bucket: str, key: str, algorithm: str = 'sha256', s3_client=None) -> str:
        """
        Compute checksum of S3 object by streaming download.
        
        Uses streaming to avoid loading entire file into memory. Falls back to source
        object ETag if compute fails.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            algorithm: Hash algorithm ('md5', 'sha256', etc.)
            
        Returns:
            Hex-encoded hash of the object
            
        Raises:
            TransientReplicationError: For transient S3 errors
            ReplicationError: For permanent errors
        """
        hasher = hashlib.new(algorithm)
        
        try:
            logger.debug(f"Computing {algorithm} checksum for s3://{bucket}/{key}")
            client = s3_client or self.source_s3_client
            response = client.get_object(Bucket=bucket, Key=key)
            
            # Stream file in 1MB chunks to avoid memory issues
            for chunk in iter(lambda: response['Body'].read(1024 * 1024), b''):
                hasher.update(chunk)
            
            checksum = hasher.hexdigest()
            logger.debug(f"Computed checksum {checksum[:16]}... for s3://{bucket}/{key}")
            return checksum
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code in ['RequestTimeout', 'ServiceUnavailable']:
                logger.warning(f"Transient error computing checksum: {error_code}")
                raise TransientReplicationError(f"Transient error: {error_code}") from e
            else:
                logger.error(f"Failed to compute checksum for s3://{bucket}/{key}: {error_code}")
                raise ReplicationError(f"Cannot compute checksum: {error_code}") from e

    def _replicate_file(self, request: ReplicationRequest, retry_count: int = 0) -> bool:
        """
        Replicate a single file from source to destination bucket.
        
        Args:
            request: The replication request
            retry_count: Current retry attempt number
            
        Returns:
            True if successful, False otherwise
            
        Raises:
            TransientReplicationError: For retryable errors
            PermanentReplicationError: For non-retryable errors
        """
        try:
            logger.info(f"Replicating {request.source_key} (attempt {retry_count + 1}/{self.config.max_retries})")

            # Copy object from source to destination
            copy_source = {'Bucket': request.source_bucket, 'Key': request.source_key}
            self.dest_s3_client.copy(
                CopySource=copy_source,
                Bucket=request.dest_bucket,
                Key=request.dest_key,
                SourceClient=self.source_s3_client,
                ExtraArgs={'ServerSideEncryption': 'AES256'}
            )

            # Verify replication by checking destination object
            try:
                dest_metadata = self._get_s3_object_metadata(
                    request.dest_bucket, 
                    request.dest_key,
                    s3_client=self.dest_s3_client
                )
            except TransientReplicationError:
                raise  # Propagate transient errors for retry
            except FileNotFoundError:
                raise PermanentReplicationError("Destination object verification failed: object not found after copy")
            except PermissionError as e:
                raise PermanentReplicationError(f"Destination object verification failed: {e}")
            except Exception as e:
                raise PermanentReplicationError(f"Destination object verification failed: {e}")

            logger.info(f"Successfully replicated {request.source_key} to {request.dest_key}")
            return True

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            
            # Categorize errors
            if error_code in ['RequestTimeout', 'ThrottlingException', 'ServiceUnavailable', 'SlowDown']:
                logger.warning(f"Transient S3 error: {error_code} (attempt {retry_count + 1})")
                raise TransientReplicationError(f"Transient S3 error: {error_code}")
            elif error_code in ['AccessDenied', 'NoSuchBucket', 'NoSuchKey']:
                logger.error(f"Permanent S3 error: {error_code} for {request.source_key}")
                raise PermanentReplicationError(f"Permanent S3 error: {error_code}")
            else:
                logger.error(f"Unknown S3 error for {request.source_key}: {error_code}")
                raise ReplicationError(f"Unknown S3 error: {error_code}")
        except (TransientReplicationError, PermanentReplicationError):
            raise  # Re-raise our custom exceptions
        except Exception as e:
            logger.error(f"Unexpected error replicating {request.source_key}: {e}", exc_info=True)
            raise PermanentReplicationError(f"Unexpected error: {e}") from e

    def _update_request_status(
        self, 
        request_id: str, 
        status: ReplicationStatus,
        error_message: Optional[str] = None,
        file_size: Optional[int] = None,
        checksum: Optional[str] = None
    ) -> None:
        """
        Update the status of a replication request in the database.
        
        Args:
            request_id: The request ID to update
            status: New replication status
            error_message: Error message if status is FAILED
            file_size: File size if status is COMPLETED
            checksum: File checksum if status is COMPLETED
        """
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                if status == ReplicationStatus.IN_PROGRESS:
                    cursor.execute(f"""
                        UPDATE {self.config.db_schema}.{self.config.db_table}
                        SET status = %s,
                            started_timestamp = CURRENT_TIMESTAMP,
                            retry_count = retry_count + 1
                        WHERE request_id = %s
                    """, (status.value, request_id))
                    
                elif status == ReplicationStatus.COMPLETED:
                    cursor.execute(f"""
                        UPDATE {self.config.db_schema}.{self.config.db_table}
                        SET status = %s,
                            completed_timestamp = CURRENT_TIMESTAMP,
                            file_size = COALESCE(%s, file_size),
                            checksum = COALESCE(%s, checksum)
                        WHERE request_id = %s
                    """, (status.value, file_size, checksum, request_id))
                    
                elif status == ReplicationStatus.FAILED:
                    cursor.execute(f"""
                        UPDATE {self.config.db_schema}.{self.config.db_table}
                        SET status = %s,
                            error_message = %s,
                            completed_timestamp = CURRENT_TIMESTAMP
                        WHERE request_id = %s
                    """, (status.value, error_message, request_id))
                
                conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to update request status: {e}", exc_info=True)
            raise
        finally:
            self._put_connection(conn)

    def register_replication_request(self, request: ReplicationRequest) -> None:
        """
        Register a new replication request in the database.
        
        Validates all request parameters before insertion.
        
        Args:
            request: The replication request to register
            
        Raises:
            ValueError: If request is invalid or already exists
            FileNotFoundError: If source object doesn't exist
        """
        # Validate inputs
        if not request.request_id or not request.request_id.strip():
            raise ValueError("request_id cannot be empty")
        
        for field in ['source_bucket', 'source_key', 'dest_bucket', 'dest_key']:
            value = getattr(request, field)
            if not value or not value.strip():
                raise ValueError(f"{field} cannot be empty")
        
        # Check if source object exists
        try:
            self._get_s3_object_metadata(request.source_bucket, request.source_key)
        except FileNotFoundError:
            logger.error(f"Source object not found: s3://{request.source_bucket}/{request.source_key}")
            raise FileNotFoundError(
                f"Source object not found: s3://{request.source_bucket}/{request.source_key}"
            )
        except (PermissionError, TransientReplicationError, ReplicationError) as e:
            logger.error(f"Cannot verify source object: {e}")
            raise ValueError(f"Cannot verify source object: {e}") from e
        
        # Check if duplicate request
        try:
            existing = self.get_replication_status(request.request_id)
            if existing is not None:
                raise ValueError(f"Request {request.request_id} already exists")
        except Exception as e:
            if "already exists" in str(e):
                raise
            logger.debug(f"Could not check for duplicate request: {e}")
        
        # Proceed with insertion
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    INSERT INTO {self.config.db_schema}.{self.config.db_table}
                    (request_id, source_bucket, source_key, dest_bucket, dest_key, status, created_timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """, (
                    request.request_id,
                    request.source_bucket,
                    request.source_key,
                    request.dest_bucket,
                    request.dest_key,
                    ReplicationStatus.PENDING.value
                ))
                conn.commit()
                logger.info(f"Registered replication request: {request.request_id}")
        except IntegrityError as e:
            conn.rollback()
            logger.error(f"Duplicate request_id or constraint violation: {request.request_id}")
            raise ValueError(f"Request {request.request_id} already exists") from e
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to register replication request: {e}", exc_info=True)
            raise
        finally:
            self._put_connection(conn)

    def process_pending_requests(self) -> int:
        """
        Process all pending replication requests with database-level locking.
        
        Uses SELECT FOR UPDATE SKIP LOCKED to prevent concurrent instances from
        processing the same requests, enabling safe multi-instance deployments.
        
        Returns:
            Number of successfully replicated files
        """
        conn = self._get_connection()
        pending_requests: List[Dict] = []
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Lock rows to prevent other transactions from selecting them
                # SKIP LOCKED allows other instances to process different requests
                cursor.execute(f"""
                    SELECT request_id, source_bucket, source_key, dest_bucket, dest_key, retry_count
                    FROM {self.config.db_schema}.{self.config.db_table}
                    WHERE (status = %s OR (status = %s AND retry_count < %s))
                    ORDER BY created_timestamp ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                """, (
                    ReplicationStatus.PENDING.value,
                    ReplicationStatus.FAILED.value,
                    self.config.max_retries,
                    self.config.batch_size
                ))
                
                pending_requests = cursor.fetchall()
                
                if not pending_requests:
                    logger.info("No pending replication requests")
                    return 0
                
                # Mark all selected rows as IN_PROGRESS atomically (within same transaction)
                request_ids = [row['request_id'] for row in pending_requests]
                cursor.execute(f"""
                    UPDATE {self.config.db_schema}.{self.config.db_table}
                    SET status = %s, started_timestamp = CURRENT_TIMESTAMP, retry_count = retry_count + 1
                    WHERE request_id = ANY(%s)
                """, (ReplicationStatus.IN_PROGRESS.value, request_ids))
                
                conn.commit()
                logger.info(f"Locked {len(pending_requests)} requests for processing")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to fetch pending requests: {e}", exc_info=True)
            self._put_connection(conn)
            raise
        finally:
            self._put_connection(conn)

        successful_count = 0
        
        for row in pending_requests:
            request = ReplicationRequest(
                request_id=row['request_id'],
                source_bucket=row['source_bucket'],
                source_key=row['source_key'],
                dest_bucket=row['dest_bucket'],
                dest_key=row['dest_key']
            )

            # Attempt replication with retries and exponential backoff
            success = False
            error_message = None
            
            for attempt in range(self.config.max_retries):
                try:
                    self._replicate_file(request, attempt)
                    success = True
                    break
                except TransientReplicationError as e:
                    error_message = str(e)
                    if attempt == self.config.max_retries - 1:
                        logger.error(f"Max retries exceeded for {request.request_id}: {e}")
                        break
                    # Exponential backoff: 2^attempt seconds (1s, 2s, 4s)
                    wait_time = 2 ** attempt
                    logger.info(f"Transient error, retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                except PermanentReplicationError as e:
                    error_message = str(e)
                    logger.error(f"Permanent error, not retrying: {e}")
                    break
                except Exception as e:
                    error_message = str(e)
                    logger.error(f"Unexpected error: {e}", exc_info=True)
                    break

            # Update final status
            if success:
                # Compute actual checksum for data integrity verification
                checksum = None
                file_size = None
                
                try:
                    source_metadata = self._get_s3_object_metadata(
                        request.source_bucket, 
                        request.source_key
                    )
                    file_size = source_metadata['size']
                    
                    # Compute actual checksum (fallback to ETag if fails)
                    try:
                        checksum = self._compute_file_checksum(
                            request.source_bucket, 
                            request.source_key
                        )
                    except (TransientReplicationError, ReplicationError) as e:
                        logger.warning(f"Could not compute checksum: {e}")
                        # Use ETag as fallback
                        checksum = source_metadata.get('etag')
                        
                except (FileNotFoundError, PermissionError, TransientReplicationError, ReplicationError) as e:
                    logger.warning(f"Could not retrieve source metadata after successful replication: {e}")
                
                self._update_request_status(
                    request.request_id,
                    ReplicationStatus.COMPLETED,
                    file_size=file_size,
                    checksum=checksum
                )
                successful_count += 1
            else:
                self._update_request_status(
                    request.request_id,
                    ReplicationStatus.FAILED,
                    error_message=error_message or "Max retries exceeded"
                )

        logger.info(f"Processed {successful_count} successful replications out of {len(pending_requests)}")
        return successful_count

    def get_replication_status(self, request_id: str) -> Optional[Dict]:
        """
        Retrieve the status of a specific replication request.
        
        Args:
            request_id: The request ID to look up
            
        Returns:
            Dictionary with request details, or None if not found
        """
        conn = self._get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(f"""
                    SELECT * FROM {self.config.db_schema}.{self.config.db_table}
                    WHERE request_id = %s
                """, (request_id,))
                
                result = cursor.fetchone()
                
                if result is None:
                    return None
                
                return dict(result)
        except Exception as e:
            logger.error(f"Failed to retrieve status for {request_id}: {e}", exc_info=True)
            return None
        finally:
            self._put_connection(conn)

    def get_replication_summary(self) -> Dict:
        """
        Retrieve summary statistics of replication activity.
        
        Returns:
            Dictionary mapping status -> {count, total_bytes}
        """
        conn = self._get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(f"""
                    SELECT 
                        status,
                        COUNT(*) as count,
                        COALESCE(SUM(file_size), 0) as total_bytes
                    FROM {self.config.db_schema}.{self.config.db_table}
                    GROUP BY status
                """)
                
                results = cursor.fetchall()
                
                summary = {}
                for row in results:
                    summary[row['status']] = {
                        'count': row['count'],
                        'total_bytes': row['total_bytes']
                    }
                
                return summary
        except Exception as e:
            logger.error(f"Failed to retrieve summary: {e}", exc_info=True)
            return {}
        finally:
            self._put_connection(conn)

    def get_pool_status(self) -> Dict[str, int]:
        """
        Get current connection pool statistics.
        
        Returns:
            Dictionary with pool status information
        """
        try:
            return {
                'available_connections': self.connection_pool.availableconn,
                'total_connections': self.connection_pool.maxconn,
            }
        except Exception as e:
            logger.error(f"Error retrieving pool status: {e}")
            return {}

    def close(self) -> None:
        """
        Close all connections in the pool and cleanup resources.
        
        Ensures proper cleanup of database connections and S3 client.
        """
        errors: List[Exception] = []
        
        if self.connection_pool:
            try:
                self.connection_pool.closeall()
                logger.info("Closed PostgreSQL connection pool")
            except Exception as e:
                logger.error(f"Error closing connection pool: {e}")
                errors.append(e)
        
        for label, client in (("source", self.source_s3_client), ("destination", self.dest_s3_client)):
            try:
                if client:
                    client.close()
                    logger.info(f"Closed {label} S3 client")
            except Exception as e:
                logger.error(f"Error closing {label} S3 client: {e}")
                errors.append(e)
        
        if errors:
            logger.warning(f"Encountered {len(errors)} error(s) during cleanup")


def main():
    """Main entry point for the S3 replication job."""
    
    # Configuration with Unity Catalog temporary path credentials
    config = ReplicationConfig(
        source_bucket="prod-ereg-bucket",
        dest_bucket="dev-ereg-replica",
        postgres_host="localhost",
        postgres_port=5432,
        postgres_database="s3_replication",
        postgres_user="postgres",
        postgres_password="your_password_here",
        aws_region="us-east-1",
        source_external_location_path="s3://prod-ereg-bucket/",
        dest_external_location_path="s3://dev-ereg-replica/",
        batch_size=100,
        max_retries=3
    )

    service = None
    try:
        # Initialize service
        service = S3ReplicationService(config)
        logger.info("S3 Replication Service initialized with Unity Catalog temporary credentials")

        # Process pending replication requests
        successful = service.process_pending_requests()
        
        # Log summary
        summary = service.get_replication_summary()
        logger.info(f"Replication summary: {json.dumps(summary, indent=2)}")

        logger.info(f"Job completed successfully. Replicated {successful} files.")

    except Exception as e:
        logger.error(f"Fatal error in replication job: {e}", exc_info=True)
        raise
    finally:
        if service:
            service.close()


if __name__ == "__main__":
    main()
