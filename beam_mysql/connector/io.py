"""I/O connectors of mysql."""

from typing import Dict

import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.pvalue import PCollection
from apache_beam.transforms.core import PTransform

from beam_mysql.connector import splitters
from beam_mysql.connector.client import MySQLClient
from beam_mysql.connector.source import MySQLSource
from beam_mysql.connector.utils import get_runtime_value


class ReadFromMySQL(PTransform):
    """Create PCollection from MySQL."""

    def __init__(
        self,
        query: str,
        host: str,
        database: str,
        user: str,
        password: str,
        port: int = 3306,
        splitter=splitters.DefaultSplitter(),
    ):
        super().__init__()
        self._query = query
        self._host = host
        self._database = database
        self._user = user
        self._password = password
        self._port = port
        self._splitter = splitter

    def expand(self, pcoll: PCollection) -> PCollection:
        return pcoll | iobase.Read(
            MySQLSource(self._query, self._host, self._database, self._user, self._password, self._port, self._splitter)
        )


class WriteToMySQL(PTransform):
    """Write dict rows to MySQL."""

    def __init__(
        self, host: str, database: str, table: str, user: str, password: str, port: int = 3306, batch_size: int = 1000
    ):
        super().__init__()
        self._host = host
        self._database = database
        self._table = table
        self._user = user
        self._password = password
        self._port = port
        self._batch_size = batch_size

    def expand(self, pcoll: PCollection) -> PCollection:
        return pcoll | beam.ParDo(
            _WriteToMySQLFn(
                self._host, self._database, self._table, self._user, self._password, self._port, self._batch_size
            )
        )


class _WriteToMySQLFn(beam.DoFn):
    """DoFn for WriteToMySQL."""

    def __init__(self, host: str, database: str, table: str, user: str, password: str, port: int, batch_size: int):
        super().__init__()
        self._host = host
        self._database = database
        self._table = table
        self._user = user
        self._password = password
        self._port = port
        self._batch_size = batch_size

        self._config = {
            "host": self._host,
            "database": self._database,
            "user": self._user,
            "password": self._password,
            "port": self._port,
        }

    def start_bundle(self):
        self._build_value()
        self._queries = []

    def process(self, element: Dict, *args, **kwargs):
        columns = []
        values = []
        for column, value in element.items():
            columns.append(column)
            values.append(value)

        column_str = ", ".join(columns)
        value_str = ", ".join([f"{value}" if isinstance(value, (int, float)) else f"'{value}'" for value in values])
        query = f"INSERT INTO {self._config['database']}.{self._table}({column_str}) VALUES({value_str});"

        self._queries.append(query)

        if len(self._queries) > self._batch_size:
            self._client.record_loader("\n".join(self._queries))
            self._queries.clear()

    def finish_bundle(self):
        if len(self._queries):
            self._client.record_loader("\n".join(self._queries))
            self._queries.clear()

    def _build_value(self):
        for k, v in self._config.items():
            self._config[k] = get_runtime_value(v)
        self._table = get_runtime_value(self._table)
        self._batch_size = get_runtime_value(self._batch_size)

        self._client = MySQLClient(self._config)
