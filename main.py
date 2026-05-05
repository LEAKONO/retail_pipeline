"""
main.py
-------
Pipeline entry point. Ties all modules together into one runnable flow.

Run with:
    python main.py
"""

import uuid
from datetime import datetime

from core.config import settings
from core.logger import logger, setup_logger
from core.database import get_snowflake_connection
from core.exceptions import PipelineException

from metadata.watermark import ensure_watermark_table, read_watermark, write_watermark
from metadata.audit import ensure_audit_table, start_audit, complete_audit, fail_audit

from ingestion.extract import extract
from ingestion.transform import transform
from ingestion.load import load


def run_pipeline() -> None:
    setup_logger(settings.log_level)

    batch_id   = f"orders_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    started_at = datetime.utcnow()

    logger.info("=" * 60)
    logger.info("Pipeline starting: {batch_id}", batch_id=batch_id)
    logger.info("=" * 60)

    with get_snowflake_connection() as conn:

        ensure_watermark_table(conn)
        ensure_audit_table(conn)

        start_audit(
            conn=conn,
            batch_id=batch_id,
            pipeline_name=settings.pipeline_name,
            started_at=started_at,
        )

        try:
            last_processed_at, last_invoice_no = read_watermark(
                conn=conn,
                pipeline_name=settings.pipeline_name,
                source_table="raw_orders",
            )

            raw_df = extract(
                last_processed_at=last_processed_at,
                last_invoice_no=last_invoice_no,
            )

            if raw_df.empty:
                logger.info("No new records found. Pipeline exiting cleanly.")
                complete_audit(
                    conn=conn,
                    batch_id=batch_id,
                    pipeline_name=settings.pipeline_name,
                    started_at=started_at,
                    records_read=0,
                    records_loaded=0,
                )
                return

            clean_df, errors_df = transform(
                df=raw_df,
                batch_id=batch_id,
            )

            # batch_id now passed to load — required by COPY INTO approach
            records_loaded, records_quarantined = load(
                conn=conn,
                clean_df=clean_df,
                errors_df=errors_df,
                batch_id=batch_id,
                batch_size=settings.batch_size,
            )

            if not clean_df.empty:
                new_watermark_ts = clean_df["invoice_date"].max()
                new_invoice_no   = clean_df.loc[
                    clean_df["invoice_date"] == new_watermark_ts,
                    "invoice_no"
                ].max()

                write_watermark(
                    conn=conn,
                    pipeline_name=settings.pipeline_name,
                    source_table="raw_orders",
                    last_processed_at=new_watermark_ts,
                    last_invoice_no=str(new_invoice_no),
                )

            complete_audit(
                conn=conn,
                batch_id=batch_id,
                pipeline_name=settings.pipeline_name,
                started_at=started_at,
                records_read=len(raw_df),
                records_loaded=records_loaded,
            )

            logger.info("=" * 60)
            logger.info("Pipeline complete")
            logger.info("  Records read:        {n}", n=len(raw_df))
            logger.info("  Records loaded:      {n}", n=records_loaded)
            logger.info("  Records quarantined: {n}", n=records_quarantined)
            logger.info("=" * 60)

        except PipelineException as e:
            logger.error("Pipeline failed: {error}", error=str(e))
            fail_audit(
                conn=conn,
                batch_id=batch_id,
                pipeline_name=settings.pipeline_name,
                started_at=started_at,
                error_message=str(e),
            )
            raise

        except Exception as e:
            logger.error("Unexpected error: {error}", error=str(e))
            fail_audit(
                conn=conn,
                batch_id=batch_id,
                pipeline_name=settings.pipeline_name,
                started_at=started_at,
                error_message=f"Unexpected: {str(e)}",
            )
            raise


if __name__ == "__main__":
    run_pipeline()
