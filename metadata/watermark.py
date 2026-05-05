from datetime import datetime
from core.database import SnowflakeConnection
from core.logger import logger
from core.exceptions import WatermarkError


def _to_str(value):
    if value is None:
        return None
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value


CREATE_WATERMARK_TABLE = """
CREATE TABLE IF NOT EXISTS pipeline_watermark (
    pipeline_name     VARCHAR(100)  NOT NULL,
    source_table      VARCHAR(100)  NOT NULL,
    last_processed_at TIMESTAMP_NTZ NOT NULL,
    last_invoice_no   VARCHAR(50),
    updated_at        TIMESTAMP_NTZ NOT NULL,
    PRIMARY KEY (pipeline_name, source_table)
)
"""

READ_WATERMARK = """
SELECT last_processed_at, last_invoice_no
FROM   pipeline_watermark
WHERE  pipeline_name = %(pipeline_name)s
AND    source_table  = %(source_table)s
"""

WRITE_WATERMARK = """
MERGE INTO pipeline_watermark AS target
USING (
    SELECT
        %(pipeline_name)s     AS pipeline_name,
        %(source_table)s      AS source_table,
        %(last_processed_at)s AS last_processed_at,
        %(last_invoice_no)s   AS last_invoice_no,
        CURRENT_TIMESTAMP()   AS updated_at
) AS source
ON  target.pipeline_name = source.pipeline_name
AND target.source_table  = source.source_table
WHEN MATCHED THEN UPDATE SET
    target.last_processed_at = source.last_processed_at,
    target.last_invoice_no   = source.last_invoice_no,
    target.updated_at        = source.updated_at
WHEN NOT MATCHED THEN INSERT (
    pipeline_name, source_table,
    last_processed_at, last_invoice_no, updated_at
) VALUES (
    source.pipeline_name, source.source_table,
    source.last_processed_at, source.last_invoice_no,
    source.updated_at
)
"""


def ensure_watermark_table(conn):
    try:
        conn.execute_query(CREATE_WATERMARK_TABLE)
        logger.debug("Watermark table ready")
    except Exception as e:
        raise WatermarkError(f"Failed to create watermark table: {str(e)}") from e


def read_watermark(conn, pipeline_name, source_table):
    try:
        rows = conn.execute_query(READ_WATERMARK, {
            "pipeline_name": pipeline_name,
            "source_table":  source_table,
        })
        if not rows:
            logger.info("No watermark found — this is a first run")
            return None, None
        logger.info("Watermark read: last_processed_at={ts}", ts=rows[0]["last_processed_at"])
        return rows[0]["last_processed_at"], rows[0]["last_invoice_no"]
    except WatermarkError:
        raise
    except Exception as e:
        raise WatermarkError(f"Failed to read watermark: {str(e)}") from e


def write_watermark(conn, pipeline_name, source_table, last_processed_at, last_invoice_no):
    try:
        conn.execute_query(WRITE_WATERMARK, {
            "pipeline_name":     pipeline_name,
            "source_table":      source_table,
            "last_processed_at": _to_str(last_processed_at),
            "last_invoice_no":   last_invoice_no,
        })
        logger.info("Watermark updated: {ts}", ts=last_processed_at)
    except Exception as e:
        raise WatermarkError(f"Failed to write watermark: {str(e)}") from e
