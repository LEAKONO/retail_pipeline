import pandas as pd
import numpy as np
import io
from core.database import SnowflakeConnection
from core.logger import logger
from core.exceptions import LoadError


CREATE_RAW_ORDERS = """
CREATE TABLE IF NOT EXISTS raw_orders (
    invoice_no    VARCHAR(20),
    stock_code    VARCHAR(20),
    description   VARCHAR(500),
    quantity      NUMBER(10, 2),
    invoice_date  TIMESTAMP_NTZ,
    unit_price    NUMBER(10, 2),
    customer_id   VARCHAR(20),
    country       VARCHAR(100),
    batch_id      VARCHAR(100),
    ingested_at   TIMESTAMP_NTZ,
    PRIMARY KEY (invoice_no, stock_code, invoice_date)
)
"""

CREATE_QUARANTINE_TABLE = """
CREATE TABLE IF NOT EXISTS raw_orders_quarantine (
    invoice_no     VARCHAR(20),
    stock_code     VARCHAR(20),
    description    VARCHAR(500),
    quantity       NUMBER(10, 2),
    invoice_date   TIMESTAMP_NTZ,
    unit_price     NUMBER(10, 2),
    customer_id    VARCHAR(20),
    country        VARCHAR(100),
    batch_id       VARCHAR(100),
    ingested_at    TIMESTAMP_NTZ,
    error_reason   VARCHAR(2000),
    quarantined_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
"""

CREATE_STAGE = """
CREATE STAGE IF NOT EXISTS raw_orders_stage
    FILE_FORMAT = (
        TYPE = CSV
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        NULL_IF = ('NULL', 'null', '')
        EMPTY_FIELD_AS_NULL = TRUE
        TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
    )
"""

COPY_INTO_ORDERS = """
COPY INTO raw_orders (
    invoice_no, stock_code, description,
    quantity, invoice_date, unit_price,
    customer_id, country, batch_id, ingested_at
)
FROM @raw_orders_stage/{filename}
FILE_FORMAT = (
    TYPE = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"\'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
)
ON_ERROR = CONTINUE
PURGE = TRUE
"""

DELETE_BATCH = """
DELETE FROM raw_orders WHERE batch_id = %(batch_id)s
"""

INSERT_QUARANTINE = """
INSERT INTO raw_orders_quarantine (
    invoice_no, stock_code, description,
    quantity, invoice_date, unit_price,
    customer_id, country, batch_id,
    ingested_at, error_reason
)
SELECT
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
FROM @raw_orders_stage/{filename}
FILE_FORMAT = (
    TYPE = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
)
PURGE = TRUE
"""


def _prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Format timestamps as strings
    for col in df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    df = df.fillna("")

    return df


def ensure_tables(conn: SnowflakeConnection) -> None:
    """Create raw tables and stage if they do not exist."""
    conn.execute_query(CREATE_RAW_ORDERS)
    conn.execute_query(CREATE_QUARANTINE_TABLE)
    conn.execute_query(CREATE_STAGE)
    logger.debug("Raw tables and stage ready")


def load_orders(
    conn:      SnowflakeConnection,
    df:        pd.DataFrame,
    batch_id:  str,
) -> int:
    if df.empty:
        logger.info("No clean records to load")
        return 0

    total_rows = len(df)
    logger.info("Starting bulk load: {n} records", n=total_rows)

    try:
        df_prepared = _prepare_dataframe(df)
        csv_buffer = io.StringIO()
        df_prepared.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)  
        filename = f"{batch_id}.csv"
        logger.info("Uploading to Snowflake stage: {filename}", filename=filename)
        import tempfile
        import os
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            delete=False,
            encoding="utf-8"
        ) as tmp:
            tmp.write(csv_buffer.getvalue())
            tmp_path = tmp.name

        try:
            conn.execute_query(
                f"PUT file://{tmp_path} @raw_orders_stage/{filename} "
                f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
            logger.info("File uploaded to stage successfully")
        finally:
            os.unlink(tmp_path)  
        conn.execute_query(DELETE_BATCH, {"batch_id": batch_id})
        logger.info("Running COPY INTO raw_orders...")
        conn.execute_query(COPY_INTO_ORDERS.format(filename=filename))

        result = conn.execute_query(
            "SELECT COUNT(*) as cnt FROM raw_orders WHERE batch_id = %(batch_id)s",
            {"batch_id": batch_id}
        )
        loaded = result[0]["cnt"] if result else 0

        logger.info("Bulk load complete: {loaded} records loaded", loaded=loaded)
        return loaded

    except Exception as e:
        raise LoadError(f"Bulk load failed: {str(e)}") from e


def load_quarantine(
    conn:      SnowflakeConnection,
    errors_df: pd.DataFrame,
    batch_id:  str,
) -> int:
    if errors_df.empty:
        return 0

    try:
        df_prepared = _prepare_dataframe(errors_df)

        csv_buffer = io.StringIO()
        df_prepared.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)

        filename = f"{batch_id}_quarantine.csv"

        import tempfile
        import os
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            delete=False,
            encoding="utf-8"
        ) as tmp:
            tmp.write(csv_buffer.getvalue())
            tmp_path = tmp.name

        try:
            conn.execute_query(
                f"PUT file://{tmp_path} @raw_orders_stage/{filename} "
                f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
        finally:
            os.unlink(tmp_path)

        conn.execute_query(INSERT_QUARANTINE.format(filename=filename))

        logger.warning(
            "Quarantined {n} rejected records",
            n=len(errors_df)
        )
        return len(errors_df)

    except Exception as e:
        logger.warning("Failed to quarantine records: {e}", e=str(e))
        return 0


def load(
    conn:      SnowflakeConnection,
    clean_df:  pd.DataFrame,
    errors_df: pd.DataFrame,
    batch_id:  str,
    batch_size: int = 1000,
) -> tuple[int, int]:
    ensure_tables(conn)
    records_loaded      = load_orders(conn, clean_df, batch_id)
    records_quarantined = load_quarantine(conn, errors_df, batch_id)
    return records_loaded, records_quarantined
