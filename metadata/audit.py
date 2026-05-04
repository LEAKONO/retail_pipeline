from datetime import datetime
from core.database import SnowflakeConnection
from core.logger import logger
from core.exceptions import AuditError


CREATE_AUDIT_TABLE = """
CREATE TABLE IF NOT EXISTS pipeline_audit (
    batch_id         VARCHAR(100)  NOT NULL PRIMARY KEY,
    pipeline_name    VARCHAR(100)  NOT NULL,
    status           VARCHAR(20)   NOT NULL,
    records_read     INTEGER       DEFAULT 0,
    records_loaded   INTEGER       DEFAULT 0,
    started_at       TIMESTAMP_NTZ NOT NULL,
    completed_at     TIMESTAMP_NTZ,
    duration_seconds FLOAT,
    error_message    VARCHAR(2000),
    updated_at       TIMESTAMP_NTZ NOT NULL
)
"""

UPSERT_AUDIT = """
MERGE INTO pipeline_audit AS target
USING (
    SELECT
        %(batch_id)s         AS batch_id,
        %(pipeline_name)s    AS pipeline_name,
        %(status)s           AS status,
        %(records_read)s     AS records_read,
        %(records_loaded)s   AS records_loaded,
        %(started_at)s       AS started_at,
        %(completed_at)s     AS completed_at,
        %(duration_seconds)s AS duration_seconds,
        %(error_message)s    AS error_message,
        CURRENT_TIMESTAMP()  AS updated_at
) AS source
ON target.batch_id = source.batch_id
WHEN MATCHED THEN UPDATE SET
    target.status           = source.status,
    target.records_read     = source.records_read,
    target.records_loaded   = source.records_loaded,
    target.completed_at     = source.completed_at,
    target.duration_seconds = source.duration_seconds,
    target.error_message    = source.error_message,
    target.updated_at       = source.updated_at
WHEN NOT MATCHED THEN INSERT (
    batch_id, pipeline_name, status,
    records_read, records_loaded,
    started_at, completed_at,
    duration_seconds, error_message, updated_at
) VALUES (
    source.batch_id, source.pipeline_name, source.status,
    source.records_read, source.records_loaded,
    source.started_at, source.completed_at,
    source.duration_seconds, source.error_message,
    source.updated_at
)
"""


def ensure_audit_table(conn: SnowflakeConnection) -> None:
    """Create audit table if it does not exist."""
    try:
        conn.execute_query(CREATE_AUDIT_TABLE)
        logger.debug("Audit table ready")
    except Exception as e:
        raise AuditError(f"Failed to create audit table: {str(e)}") from e


def start_audit(
    conn:          SnowflakeConnection,
    batch_id:      str,
    pipeline_name: str,
    started_at:    datetime,
) -> None:
    try:
        conn.execute_query(UPSERT_AUDIT, {
            "batch_id":         batch_id,
            "pipeline_name":    pipeline_name,
            "status":           "in_progress",
            "records_read":     0,
            "records_loaded":   0,
            "started_at":       started_at,
            "completed_at":     None,
            "duration_seconds": None,
            "error_message":    None,
        })
        logger.info("Audit started: {batch_id}", batch_id=batch_id)
    except Exception as e:
        logger.warning("Failed to start audit: {e}", e=str(e))


def complete_audit(
    conn:           SnowflakeConnection,
    batch_id:       str,
    pipeline_name:  str,
    started_at:     datetime,
    records_read:   int,
    records_loaded: int,
) -> None:
    completed_at     = datetime.utcnow()
    duration_seconds = (completed_at - started_at).total_seconds()

    try:
        conn.execute_query(UPSERT_AUDIT, {
            "batch_id":         batch_id,
            "pipeline_name":    pipeline_name,
            "status":           "success",
            "records_read":     records_read,
            "records_loaded":   records_loaded,
            "started_at":       started_at,
            "completed_at":     completed_at,
            "duration_seconds": duration_seconds,
            "error_message":    None,
        })
        logger.info(
            "Pipeline complete: {loaded} records in {secs:.1f}s",
            loaded=records_loaded,
            secs=duration_seconds
        )
    except Exception as e:
        logger.warning("Failed to complete audit: {e}", e=str(e))


def fail_audit(
    conn:          SnowflakeConnection,
    batch_id:      str,
    pipeline_name: str,
    started_at:    datetime,
    error_message: str,
) -> None:
    completed_at     = datetime.utcnow()
    duration_seconds = (completed_at - started_at).total_seconds()

    try:
        conn.execute_query(UPSERT_AUDIT, {
            "batch_id":         batch_id,
            "pipeline_name":    pipeline_name,
            "status":           "failed",
            "records_read":     0,
            "records_loaded":   0,
            "started_at":       started_at,
            "completed_at":     completed_at,
            "duration_seconds": duration_seconds,
            "error_message":    error_message[:2000],
        })
        logger.error("Pipeline failed: {error}", error=error_message[:200])
    except Exception as e:
        logger.warning("Failed to write failure audit: {e}", e=str(e))
