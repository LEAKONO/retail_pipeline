import snowflake.connector
from contextlib import contextmanager
from typing import Generator

from core.config import settings
from core.logger import logger
from core.exceptions import LoadError
class SnowflakeConnection:
    def __init__(self, conn: snowflake.connector.SnowflakeConnection):
        self._conn   = conn
        self._cursor = conn.cursor()

    def execute_query(self, sql: str, params: dict = None) -> list:
        logger.debug("Executing: {sql}", sql=sql[:200])
        try:
            self._cursor.execute(sql, params or {})
            if self._cursor.description:
                columns = [col[0].lower() for col in self._cursor.description]
                return [dict(zip(columns, row)) for row in self._cursor.fetchall()]
            return []
        except snowflake.connector.errors.ProgrammingError as e:
            raise LoadError(f"Query failed: {str(e)}") from e

    def execute_many(self, sql: str, data: list) -> None:
        logger.debug("Bulk inserting {n} rows", n=len(data))
        try:
            self._cursor.executemany(sql, data)
        except snowflake.connector.errors.ProgrammingError as e:
            raise LoadError(f"Bulk insert failed: {str(e)}") from e

    def close(self) -> None:
        self._cursor.close()
        self._conn.close()
        logger.debug("Snowflake connection closed")


@contextmanager
def get_snowflake_connection() -> Generator[SnowflakeConnection, None, None]:
    conn = None
    try:
        logger.info("Connecting to Snowflake: {account}", account=settings.snowflake_account)
        raw_conn = snowflake.connector.connect(
            account            = settings.snowflake_account,
            user               = settings.snowflake_user,
            password           = settings.snowflake_password,
            database           = settings.snowflake_database,
            warehouse          = settings.snowflake_warehouse,
            role               = settings.snowflake_role,
            schema             = settings.snowflake_raw_schema,
            login_timeout      = 30,
            network_timeout    = 120,
            client_session_keep_alive = True,
        )
        conn = SnowflakeConnection(raw_conn)
        logger.info("Snowflake connection established")
        yield conn
    except snowflake.connector.errors.DatabaseError as e:
        raise LoadError(f"Failed to connect: {str(e)}") from e
    finally:
        if conn:
            conn.close()
