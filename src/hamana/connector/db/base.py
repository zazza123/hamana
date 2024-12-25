import logging
from types import TracebackType
from typing import Type, overload, Generator

from pandas import DataFrame

from .schema import SQLiteDataImportMode
from .interface import DatabaseConnectorABC
from .query import Query, QueryColumn, ColumnDataType
from .exceptions import QueryColumnsNotAvailable, ColumnDataTypeConversionError, TableAlreadyExists

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
                logger.info("pinging database ...")
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
                logger.info("extracting data ...")

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

    def batch_execute(self, query: Query, batch_size: int) -> Generator[list[tuple], None, None]:
        logger.debug("start")

        # execute query
        try:
            with self as conn:
                logger.info("extracting data ...")

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

                # fetch in batches
                while True:
                    results = cursor.fetchmany(batch_size)

                    if not results:
                        break

                    # set columns
                    if query.columns is None:
                        """
                            Observe that this operatin is executed only once 
                            and only if the query object was not defined with columns.
                        """
                        logger.info("set query columns")
                        columns = [desc[0] for desc in cursor.description]

                        # create temporary DataFrame
                        df_temp = DataFrame(results, columns = columns)

                        # get columns
                        logger.debug("update query columns ...")
                        query_columns = []
                        for i, column in enumerate(columns):
                            query_columns.append(
                                QueryColumn(
                                    order = i,
                                    name = column,
                                    dtype = ColumnDataType.from_pandas(df_temp[column].dtype.name)
                                )
                            )

                        query.columns = query_columns
                        logger.info("query column updated")

                    yield results

                cursor.close()
                logger.debug("cursor closed")
        except Exception as e:
            logger.exception(e)
            raise e

        logger.debug("end")
        return

    def to_sqlite(
        self,
        query: Query,
        table_name: str,
        raw_insert: bool = False,
        batch_size: int = 1000,
        mode: SQLiteDataImportMode = SQLiteDataImportMode.REPLACE
    ) -> None:
        logger.debug("start")

        table_name_upper = table_name.upper()
        insert_query: str = ""
        column_names: list[str] = []

        # import internal database
        from ...core.db import HamanaDatabase
        logger.debug("imported internal database")

        # get instance
        hamana_db = HamanaDatabase.get_instance()
        hamana_connection = hamana_db.get_connection()
        logger.debug("internal database instance obtained")

        # check table existance
        query_check_table = Query(
            query = """SELECT COUNT(1) AS flag_exists FROM sqlite_master WHERE type = 'table' AND name = :table_name""",
            params = {"table_name": table_name_upper},
            columns = [
                QueryColumn(order = 0, name = "flag_exists", dtype = ColumnDataType.BOOLEAN)
            ]
        )
        hamana_db.execute(query_check_table)
        logger.debug("table check query executed")

        flag_table_exists = False
        if query_check_table.result is not None:
            flag_table_exists = query_check_table.result["flag_exists"].values[0]
        logger.info(f"table exists: {flag_table_exists}")

        # block insert if mode is fail and table exists
        if flag_table_exists and mode == SQLiteDataImportMode.FAIL:
            logger.error(f"table {table_name_upper} already exists")
            raise TableAlreadyExists(table_name_upper)

        # execute extraction
        logger.info(f"extracting data, batch size: {batch_size}")
        flag_first_batch = True
        hamana_cursor = hamana_connection.cursor()
        for raw_batch in self.batch_execute(query, batch_size):

            if flag_first_batch:
                logger.info("generating insert query")
                insert_query = query.get_insert_query(table_name_upper)
                column_names = query.get_column_names()

                # create table
                if not flag_table_exists or mode == SQLiteDataImportMode.REPLACE:

                    # drop if exists (for replace)
                    if flag_table_exists:
                        logger.info(f"drop table {table_name_upper}")
                        hamana_cursor.execute(f"DROP TABLE {table_name_upper}")
                        hamana_connection.commit()
                        logger.debug("table dropped")

                    logger.info(f"creating table {table_name_upper}")
                    hamana_cursor.execute(query.get_create_query(table_name_upper))
                    hamana_connection.commit()
                    logger.debug("table created")

                # set flag
                flag_first_batch = False

            # adjust data types
            if raw_insert:
                # no data type conversion
                hamana_cursor.executemany(insert_query, raw_batch)
                hamana_connection.commit()
            else:
                # create temporary query
                query_temp = Query(query = query.query, columns = query.columns)

                # assign result (adjust data types)
                df_temp = DataFrame(raw_batch, columns = column_names)
                df_temp = self._adjust_query_result_df(df_temp, query_temp.columns) # type: ignore
                query_temp.result = df_temp

                # insert into table
                query_temp.to_sqlite(table_name_upper, SQLiteDataImportMode.APPEND)

        logger.info(f"data inserted into table {table_name_upper}")
        hamana_cursor.close()

        logger.debug("end")
        return

    def _adjust_query_result_df(self, df_result: DataFrame, columns: list[QueryColumn]) -> DataFrame:
        """
            This function is used to adjust a DataFrame (usually 
            the result of a query) based on the columns provided.

            The function re-orders the columns of the DataFrame 
            and check the data types; if they do not match, then 
            the function will try to convert the requested one.

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

        # check columns_query is a subset of columns_df
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
                    df_result[column.name] = column.parser.parse(df_result[column.name], dtype_query)
                except Exception as e:
                    logger.error("ERROR: on datatype change")
                    logger.error(e)
                    raise ColumnDataTypeConversionError(f"ERROR: on datatype change for {column.name}")

        logger.debug("end")
        return df_result