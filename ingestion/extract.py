import pandas as pd
from datetime import datetime
from pathlib import Path

from core.config import settings
from core.logger import logger
from core.exceptions import ExtractionError
"""
This for schema validation  I expect the CSV to have exactly these column names."
If the source data changes (e.g. someone renames "Customer ID" to "CustomerID"),
the pipeline will fail loudly instead of silently producing wrong results. 
This is called schema validation and it's a data engineering best practice.
"""
EXPECTED_COLUMNS = {
    "Invoice",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "Price",
    "Customer ID",
    "Country",
}


def read_source_file() -> pd.DataFrame:
    source_path = Path(settings.source_file_path)

    if not source_path.exists():
        raise ExtractionError(
            f"Source file not found: {source_path.absolute()}\n"
            f"Download from Kaggle: Online Retail II dataset"
        )

    logger.info("Reading source file: {path}", path=str(source_path))

    try:
        df = pd.read_csv(
            source_path,
            encoding="utf-8",
            parse_dates=["InvoiceDate"],
            engine="python",
        )
    except UnicodeDecodeError:
        logger.warning("UTF-8 failed, retrying with latin-1 encoding")
        df = pd.read_csv(
            source_path,
            encoding="latin-1",
            parse_dates=["InvoiceDate"],
            engine="python",
        )
    except Exception as e:
        raise ExtractionError(f"Failed to read CSV: {str(e)}") from e

    logger.info("Source file loaded: {rows} rows, {cols} columns",
                rows=len(df), cols=len(df.columns))
    _validate_schema(df)

    return df


def _validate_schema(df: pd.DataFrame) -> None:
    actual_columns  = set(df.columns)
    missing_columns = EXPECTED_COLUMNS - actual_columns

    if missing_columns:
        raise ExtractionError(
            f"Source schema mismatch. Missing columns: {missing_columns}\n"
            f"Found columns: {actual_columns}"
        )
    logger.debug("Schema validation passed")


def filter_new_records(
    df:               pd.DataFrame,
    last_processed_at: datetime | None,
    last_invoice_no:   str | None,
) -> pd.DataFrame:
    if last_processed_at is None:
        logger.info("First run detected — loading all {rows} records", rows=len(df))
        return df

    logger.info(
        "Applying watermark filter: InvoiceDate >= {ts}",
        ts=last_processed_at
    )
    after_watermark = df["InvoiceDate"] > last_processed_at
    at_boundary = (
        (df["InvoiceDate"] == last_processed_at) &
        (df["Invoice"] > last_invoice_no)
    )
    new_records = df[after_watermark | at_boundary].copy()
    logger.info(
        "Watermark filter applied: {new} new records from {total} total",
        new=len(new_records),
        total=len(df)
    )
    if len(new_records) == 0:
        logger.info("No new records found since last run — pipeline will exit cleanly")
    return new_records

def extract(
    last_processed_at: datetime | None,
    last_invoice_no:   str | None,
) -> pd.DataFrame:
    df     = read_source_file()
    new_df = filter_new_records(df, last_processed_at, last_invoice_no)
    return new_df
