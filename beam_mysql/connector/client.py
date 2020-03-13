"""A client of mysql."""

import logging
from typing import Dict
from typing import Generator
from typing import List

import mysql.connector
from mysql.connector.errors import Error as MySQLConnectorError

from beam_mysql.connector.errors import MySQLClientError

_SELECT_STATEMENT = "SELECT"
_INSERT_STATEMENT = "INSERT"


class MySQLClient:
    """A mysql client object."""

    def __init__(self, config: Dict):
        self._config = config
        self._validate_config(self._config)

    def record_generator(self, query: str, dictionary=True) -> Generator[Dict, None, None]:
        """
        Generate dict record from raw data on mysql.

        Args:
            query: query with select statement
            dictionary: the type of result is dict if true else tuple

        Returns:
            dict record

        Raises:
            ~beam_mysql.connector.errors.MySQLClientError
        """
        self._validate_query(query, [_SELECT_STATEMENT])

        with _MySQLConnection(self._config) as conn:
            # buffered is false because it can be assumed that the data size is too large
            cur = conn.cursor(buffered=False, dictionary=dictionary)

            try:
                cur.execute(query)
                logging.info(f"Successfully execute query: {query}")

                for record in cur:
                    yield record
            except MySQLConnectorError as e:
                raise MySQLClientError(f"Failed to execute query: {query}, Raise exception: {e}")

            cur.close()

    def counts_estimator(self, query: str) -> int:
        """
        Make a estimate of the total number of records.

        Args:
            query: query with select statement

        Returns:
            the total number of records

        Raises:
            ~beam_mysql.connector.errors.MySQLClientError
        """
        self._validate_query(query, [_SELECT_STATEMENT])
        count_query = f"SELECT COUNT(*) AS count FROM ({query}) as subq"

        with _MySQLConnection(self._config) as conn:
            # buffered is false because it can be assumed that the data size is too large
            cur = conn.cursor(buffered=False, dictionary=True)

            try:
                cur.execute(count_query)
                logging.info(f"Successfully execute query: {count_query}")

                record = cur.fetchone()
            except MySQLConnectorError as e:
                raise MySQLClientError(f"Failed to execute query: {count_query}, Raise exception: {e}")

            cur.close()

            return record["count"]

    def rough_counts_estimator(self, query: str) -> int:
        """
        Make a rough estimate of the total number of records.
        To avoid waiting time by select counts query when the data size is too large.

        Args:
            query: query with select statement

        Returns:
            the total number of records

        Raises:
            ~beam_mysql.connector.errors.MySQLClientError
        """
        self._validate_query(query, [_SELECT_STATEMENT])
        count_query = f"EXPLAIN SELECT * FROM ({query}) as subq"

        with _MySQLConnection(self._config) as conn:
            # buffered is false because it can be assumed that the data size is too large
            cur = conn.cursor(buffered=False, dictionary=True)

            try:
                cur.execute(count_query)
                logging.info(f"Successfully execute query: {count_query}")

                records = cur.fetchall()

                total_number = 0

                for record in records:
                    # Query of the argument should be "DERIVED" because it is sub query of explain select.
                    # Count query should be "PRIMARY" or "SIMPLE" because it is not sub query.
                    if record["select_type"] in ("PRIMARY", "SIMPLE"):
                        total_number = record["rows"]
            except MySQLConnectorError as e:
                raise MySQLClientError(f"Failed to execute query: {count_query}, Raise exception: {e}")

            cur.close()

            if total_number <= 0:
                raise mysql.connector.errors.Error(f"Failed to estimate total number of records. Query: {count_query}")
            else:
                return total_number

    def record_loader(self, query: str):
        """
        Load dict record into mysql.

        Args:
            query: query with insert or update statement

        Raises:
            ~beam_mysql.connector.errors.MySQLClientError
        """
        self._validate_query(query, [_INSERT_STATEMENT])

        with _MySQLConnection(self._config) as conn:
            cur = conn.cursor()

            try:
                cur.execute(query)
                conn.commit()
                logging.info(f"Successfully execute query: {query}")
            except MySQLConnectorError as e:
                conn.rollback()
                raise MySQLClientError(f"Failed to execute query: {query}, Raise exception: {e}")

            cur.close()

    @staticmethod
    def _validate_config(config: Dict):
        required_keys = {"host", "port", "database", "user", "password"}
        if not config.keys() == required_keys:
            raise MySQLClientError(f"Config is not satisfied. required: {required_keys}, actual: {config.keys()}")

    @staticmethod
    def _validate_query(query: str, statements: List[str]):
        query = query.lstrip()

        for statement in statements:
            if statement and not query.lower().startswith(statement.lower()):
                raise MySQLClientError(f"Query expected to start with {statement} statement. Query: {query}")


class _MySQLConnection:
    """A wrapper object to connect mysql."""

    def __init__(self, _config: Dict):
        self._config = _config

    def __enter__(self):
        try:
            self.conn = mysql.connector.connect(**self._config)
            return self.conn
        except MySQLConnectorError as e:
            raise MySQLClientError(f"Failed to connect mysql, Raise exception: {e}")

    def __exit__(self, exception_type, exception_value, traceback):
        self.conn.close()
