import logging
from types import TracebackType
from typing import Type, overload

from pandas import DataFrame, to_datetime

from .query import Query, QueryColumn, ColumnDataType
from .interface import DatabaseConnectorABC
from .exceptions import QueryColumnsNotAvailable, ColumnDataTypeConversionError

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
                columns = [desc[0] for desc in cursor.description]

                # fetch results
                result = cursor.fetchall()
                logger.info(f"data extracted ({len(result)} rows)")

                cursor.close()
                logger.debug("cursor closed")
        except Exception as e:
            logger.exception(e)
            raise e

        logger.debug("convert to Dataframe")
        df_result = DataFrame(result, columns = columns)

        # adjust columns
        if query.columns:
            df_result = self._adjust_query_result_df(df_result, query.columns)
        else:
            logger.debug("update query columns ...")
            df_dtype = df_result.dtypes
            query_columns = []

            for i, column in enumerate(columns):
                query_columns.append(
                    QueryColumn(
                        order = i,
                        name = column,
                        dtype = ColumnDataType.from_pandas(df_dtype[column].__str__())
                    )
                )

            logger.info("query column updated")
            query.columns = query_columns

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

    def _adjust_query_result_df(self, df_result: DataFrame, columns: list[QueryColumn]) -> DataFrame:
        """
            This function is used to adjust a DataFrame (usually 
            the result of a query) based on the columns provided.

            The function re-orders the columns of the DataFrame 
            and check the data types.

            Parameters:
                df_result: DataFrame to adjust.
                columns: list of columns to use for the adjustment
        """
        logger.debug("start")

        # get columns
        columns_query = []
        columns_df = df_result.columns.to_list()

        logger.info("get query columns ordered")
        for col in sorted(columns, key = lambda col : col.order):
            columns_query.append(col.name)

        # check columns_query is a substte of columns_df
        if not set(columns_query).issubset(columns_df):
            logger.error("columns do not match between query and resuls")
            raise QueryColumnsNotAvailable(f"columns do not match {set(columns_query).difference(columns_df)}")

        # re-order
        if columns_query != columns_df:
            logger.info("re-ordering columns")
            logger.info(f"order > {columns_query}")
            df_result = df_result[columns_query]
        else:
            logger.info("columns already in the correct order")

        # check data types
        logger.info("check data types")
        dtypes_df = df_result.dtypes
        for column in columns:
            dtype_query = column.dtype
            dtype_df = ColumnDataType.from_pandas(dtypes_df[column.name].name)
            logger.debug(f"column: {column.name}")
            logger.debug(f"datatype (query): {dtype_query}")
            logger.debug(f"datatype (df): {dtype_df}")

            if dtype_query != dtype_df:
                try:
                    logger.info(f"different datatype for {column.name}")
                    logger.info(f"datatype (query): {dtype_query}")
                    logger.info(f"datatype (df): {dtype_df}")

                    match dtype_query:
                        case ColumnDataType.INTEGER:
                            df_result[column.name] = df_result[column.name].astype("int64")
                        case ColumnDataType.NUMBER:
                            df_result[column.name] = df_result[column.name].astype("float64")
                        case ColumnDataType.TEXT:
                            df_result[column.name] = df_result[column.name].astype("object")
                        case ColumnDataType.BOOLEAN:
                            df_result[column.name] = df_result[column.name].astype("bool")
                        case ColumnDataType.DATETIME:
                            df_result[column.name] = to_datetime(df_result[column.name])
                        case ColumnDataType.TIMESTAMP:
                            df_result[column.name] = to_datetime(df_result[column.name], unit = "s")

                except Exception as e:
                    logger.error("ERROR: on datatype change")
                    logger.error(e)
                    raise ColumnDataTypeConversionError(f"ERROR: on datatype change for {column.name}")

        logger.debug("end")
        return df_result