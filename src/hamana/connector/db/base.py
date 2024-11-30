import logging
from types import TracebackType
from typing import Type, overload

from pandas import DataFrame

from .query import Query, QueryColumn
from .interface import DatabaseConnectorABC

# set logger
logger = logging.getLogger(__name__)

class BaseConnector(DatabaseConnectorABC):
    """
        Class to represent a connector to a database.
    """

    def __enter__(self):
        logger.debug("start")
        self.connection = self._connect()
        logger.info("connection opened")
        logger.debug("end")
        return self

    def __exit__(self, exc_type: Type[BaseException] | None, exc_value: BaseException | None, exc_traceback: TracebackType | None) -> None:
        logger.debug("start")

        # log exception
        if exc_type is not None:
            logger.error(f"exception occurred: {exc_type}")
            logger.error(f"exception value: {exc_value}")
            logger.error(f"exception traceback: {exc_traceback}")

        self.connection.close()

        logger.info("connection closed")
        logger.debug("end")
        return

    def ping(self) -> None:
        logger.debug("start")

        try:
            with self:
                logger.info(f"pinging database ...")
        except Exception as e:
            logger.exception(e)
            raise e

        logger.debug("end")
        return None

    @overload
    def execute(self, query: str) -> Query: ...

    @overload
    def execute(self, query: Query) -> None: ...

    def execute(self, query: Query | str) -> None | Query:
        logger.debug("start")

        flag_query_str = isinstance(query, str)
        if flag_query_str:
            logger.info("query string provided")
            query = Query(query)

        # execute query
        try:
            with self as conn:
                logger.info(f"extracting data ...")

                logger.debug("open cursor")
                cursor = conn.connection.cursor()
                logger.debug("cursor opened")

                # execute query
                params = query.get_params()
                if params is not None:
                    cursor.execute(query.query, params)
                else:
                    cursor.execute(query.query)
                logger.info(f"query: {query.query}")
                logger.info(f"parameters: {query.get_params()}")

                # set columns
                columns = [QueryColumn(order = i, name = desc[0]) for i, desc in enumerate(cursor.description)]

                # fetch results
                result = cursor.fetchall()
                logger.info(f"data extracted ({len(result)} rows)")

                cursor.close()
                logger.debug("cursor closed")
        except Exception as e:
            logger.exception(e)
            raise e

        logger.debug("convert to Dataframe")
        df_result = DataFrame(result, columns = [column.name for column in columns])

        # adjust columns
        if query.columns:
            self._adjust_query_result_df(df_result, query.columns)
        else:
            logger.info("query column updated")
            query.columns = columns

        # set query result
        query.result = df_result

        logger.debug("end")
        return query if flag_query_str else None

    def to_sqlite(self, query: Query, table_name: str, batch_size: int = 1000) -> None:
        logger.debug("start")

        insert_query: str | None = None

        # import internal database
        from ...core.db import HamanaDatabase
        logger.debug("imported internal database")

        # get instance
        hamana_db = HamanaDatabase.get_instance()
        hamana_connection = hamana_db.get_connection()
        logger.debug("internal database instance obtained")

        # execute extraction
        logger.info(f"extracting data, batch size: {batch_size}")
        hamana_cursor = hamana_connection.cursor()
        for row_batch in self.batch_execute(query, batch_size):

            if insert_query is None:
                logger.info("generating insert query")
                insert_query = query.get_insert_query(table_name)

                logger.info(f"creating table {table_name}")
                hamana_cursor.execute(query.get_create_query(table_name))
                hamana_connection.commit()
                logger.debug("table created")

            hamana_cursor.executemany(insert_query, row_batch)
            hamana_connection.commit()

        logger.info("data inserted into table")
        hamana_cursor.close()

        logger.debug("end")
        return

    def _adjust_query_result_df(self, df_result: DataFrame, columns: list[QueryColumn]) -> None:
        """
            This function is used to adjust a DataFrame (usually 
            the result of a query) based on the columns provided.

            The function renames and re-orders the columns of the DataFrame.

            Parameters:
                df_result: DataFrame to adjust.
                columns: list of columns to use for the adjustment
        """
        logger.debug("start")

        logger.info("adjust columns")
        order =  []
        for col in sorted(columns, key = lambda col : col.order):
            order.append(col.name)

        # re-order
        logger.info(f"order > {order}")
        df_result = df_result[order]

        logger.debug("end")
        return