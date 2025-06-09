import logging
from types import TracebackType
from typing import Type, overload, Generator

from pandas import DataFrame

from ...core.identifier import ColumnIdentifier
from ...core.exceptions import ColumnIdentifierError
from ...core.column import Column, BooleanColumn, StringColumn
from .query import Query
from .schema import SQLiteDataImportMode
from .interface import DatabaseConnectorABC, Cursor
from .exceptions import TableAlreadyExists, ColumnDataTypeConversionError

# set logger
logger = logging.getLogger(__name__)

class BaseConnector(DatabaseConnectorABC):
    """
        Class representing a generic database connector that can be 
        used to define new database connectors satisfying the 
        Python Database API Specification v2.0.

        New connectors that inherit from this class must implement 
        the `_connect` method returning the connection proper connection 
        object satisfying PEP 249.
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

    def parse_cursor_description(self, cursor: Cursor) -> dict[str, Column | None]:
        logger.debug("start")

        columns = {}
        for i, column in enumerate(cursor.description):

            column_inferred: Column | None = None

            column_name = column[0]
            column_type = column[1]
            logger.debug(f"column: {column_name}, type: {column_type}")
            logger.debug(f"column full info: {column}")

            if column_type is not None:
                try:
                    column_inferred = self.get_column_from_dtype(column_type, column_name, i)
                except ColumnDataTypeConversionError:
                    logger.warning(f"column {column_name} has an unmapped data type: {column_type}")

            columns[column_name] = column_inferred

        logger.debug("end")
        return columns

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
            with self:
                logger.info("extracting data ...")

                logger.debug("open cursor")
                cursor = self.connection.cursor()
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
                columns = self.parse_cursor_description(cursor)

                # fetch results
                result = cursor.fetchall()
                logger.info(f"data extracted ({len(result)} rows)")

                cursor.close()
                logger.debug("cursor closed")
        except Exception as e:
            logger.exception(e)
            raise e

        logger.debug("convert to Dataframe")
        df_result = DataFrame(result, columns = [column_name for column_name in columns], dtype = "object")

        # adjust columns
        if query.columns:
            df_result = query.adjust_df(df_result)
        else:
            logger.debug("update query columns ...")
            for i, column_name in enumerate(columns):

                inferred_column = None
                if columns[column_name] is None:
                    logger.debug(f"column {column_name} still not inferred, default infering")
                    try:
                        inferred_column = ColumnIdentifier.infer(df_result[column_name], column_name, i)
                    except ColumnIdentifierError:
                        logger.warning(f"column {column_name} could not be inferred, defualting to string")
                        inferred_column = StringColumn(name = column_name, order = i)

                    columns[column_name] = inferred_column

                # adjust column data type
                df_result[column_name] = columns[column_name].parser.pandas(df_result[column_name]) # type: ignore (always Column)

            logger.info("query column updated")
            query.columns = [columns[column_name] for column_name in columns]

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
                            Observe that this operation is executed only once 
                            and only if the query object was not defined with columns.
                        """
                        logger.info("set query columns")

                        # get columns
                        columns = self.parse_cursor_description(cursor)

                        # create temporary DataFrame
                        df_temp = DataFrame(results, columns = [column_name for column_name in columns])

                        # get columns
                        logger.debug("update query columns ...")
                        for i, column_name in enumerate(columns):

                            inferred_column = None
                            if columns[column_name] is None:
                                logger.debug(f"column {column_name} still not inferred, default infering.")
                                try:
                                    inferred_column = ColumnIdentifier.infer(df_temp[column_name], column_name, i)
                                except ColumnIdentifierError:
                                    logger.warning(f"column {column_name} could not be inferred, defualting to string")
                                    inferred_column = StringColumn(name = column_name, order = i)

                                columns[column_name] = inferred_column

                        query.columns = [columns[column_name] for column_name in columns]
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
        batch_size: int = 10_000,
        mode: SQLiteDataImportMode = SQLiteDataImportMode.REPLACE
    ) -> None:
        logger.debug("start")

        table_name_upper = table_name.upper()
        insert_query: str = ""
        column_names: list[str] = []

        # import internal database
        from .hamana import HamanaConnector
        logger.debug("imported internal database")

        # get instance
        hamana_db = HamanaConnector.get_instance()
        hamana_connection = hamana_db.get_connection()
        logger.debug("internal database instance obtained")

        # check table existance
        query_check_table = Query(
            query = """SELECT COUNT(1) AS flag_exists FROM sqlite_master WHERE type = 'table' AND name = :table_name""",
            params = {"table_name": table_name_upper},
            columns = [
                BooleanColumn(order = 0, name = "flag_exists", true_value = 1, false_value = 0)
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
                df_temp = DataFrame(raw_batch, columns = column_names, dtype = "object")
                df_temp = query_temp.adjust_df(df_temp)
                query_temp.result = df_temp

                # insert into table
                query_temp.to_sqlite(table_name_upper, SQLiteDataImportMode.APPEND)

        logger.info(f"data inserted into table {table_name_upper}")
        hamana_cursor.close()

        logger.debug("end")
        return