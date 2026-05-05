"""
ingestion/transform.py
----------------------
Responsibilities:
    - Rename columns to snake_case
    - Cast columns to correct data types
    - Remove duplicates
    - Validate data quality rules
    - Reject bad records into a separate errors DataFrame
"""

import pandas as pd
from core.logger import logger
from core.exceptions import ValidationError


# Rename source columns to snake_case for Snowflake
COLUMN_MAPPING = {
    "Invoice":     "invoice_no",
    "StockCode":   "stock_code",
    "Description": "description",
    "Quantity":    "quantity",
    "InvoiceDate": "invoice_date",
    "Price":       "unit_price",
    "Customer ID": "customer_id",
    "Country":     "country",
}

# Minimum data quality rules
QUALITY_RULES = {
    "invoice_no":   {"nullable": False},
    "stock_code":   {"nullable": False},
    "invoice_date": {"nullable": False},
    "quantity":     {"nullable": False, "min_value": -9999},
    "unit_price":   {"nullable": False, "min_value": 0},
}


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    #Rename source columns to snake_case standard.
    df = df.rename(columns=COLUMN_MAPPING)
    logger.debug("Columns renamed to snake_case")
    return df


def cast_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cast each column to its correct Python/Snowflake type.
    """
    try:
        for col in ["invoice_no", "stock_code", "description", "country"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        # Numeric columns
        df["quantity"]   = pd.to_numeric(df["quantity"],   errors="coerce")
        df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

        # Customer ID — nullable string (many orders have no customer ID)
        if "customer_id" in df.columns:
            df["customer_id"] = df["customer_id"].astype(str).str.strip()
            # Replace "nan" strings that come from null float conversion
            df["customer_id"] = df["customer_id"].replace("nan", None)

        # Confirm it is datetime type
        if not pd.api.types.is_datetime64_any_dtype(df["invoice_date"]):
            df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")

        logger.debug("Data types cast successfully")

    except Exception as e:
        raise ValidationError(f"Type casting failed: {str(e)}") from e

    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    
    #Remove duplicate records based on natural key.
    before = len(df)
    df = df.drop_duplicates(
        subset=["invoice_no", "stock_code", "invoice_date"],
        keep="last"   # Keep most recent version of duplicate
    )
    after = len(df)

    if before != after:
        logger.warning(
            "Removed {n} duplicate records",
            n=before - after
        )

    return df


def validate_quality(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    #Apply data quality rules and split into good and bad records.
    error_mask = pd.Series([False] * len(df), index=df.index)
    error_reasons = pd.Series([""] * len(df), index=df.index)

    # Rule 1: Required columns must not be null
    for col, rules in QUALITY_RULES.items():
        if not rules["nullable"] and col in df.columns:
            null_mask = df[col].isna()
            error_reasons[null_mask] += f"{col} is null; "
            error_mask = error_mask | null_mask

    # Rule 2: Numeric minimums
    if "unit_price" in df.columns:
        negative_price = df["unit_price"] < 0
        error_reasons[negative_price] += "unit_price is negative; "
        error_mask = error_mask | negative_price

    # Split into clean and error DataFrames
    clean_df  = df[~error_mask].copy()
    errors_df = df[error_mask].copy()

    if len(errors_df) > 0:
        errors_df["error_reason"] = error_reasons[error_mask]
        logger.warning(
            "Data quality: {clean} clean, {errors} rejected",
            clean=len(clean_df),
            errors=len(errors_df)
        )
    else:
        logger.info(
            "Data quality passed: all {n} records clean",
            n=len(clean_df)
        )

    return clean_df, errors_df


def add_audit_columns(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    # Add pipeline metadata columns to every record.
    import uuid
    from datetime import datetime

    df["batch_id"]    = batch_id
    df["ingested_at"] = datetime.utcnow()

    logger.debug("Audit columns added: batch_id={batch_id}", batch_id=batch_id)
    return df


def transform(df: pd.DataFrame, batch_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    #Main transformation function called by main.py.
    if df.empty:
        logger.info("Empty DataFrame received — nothing to transform")
        return df, pd.DataFrame()

    logger.info("Starting transformation: {n} records", n=len(df))

    df              = rename_columns(df)
    df              = cast_data_types(df)
    df              = remove_duplicates(df)
    clean_df, errors_df = validate_quality(df)
    clean_df        = add_audit_columns(clean_df, batch_id)

    logger.info(
        "Transformation complete: {clean} clean, {errors} rejected",
        clean=len(clean_df),
        errors=len(errors_df)
    )

    return clean_df, errors_df
