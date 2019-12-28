"""Sources and sinks of mysql."""

import dataclasses
from typing import Dict

import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.pvalue import PCollection
from apache_beam.transforms.core import PTransform

from beam_mysql.connector.client import MySQLClient
from beam_mysql.connector.source import MySQLSource
from beam_mysql.connector.utils import get_runtime_value


@dataclasses.dataclass
class ReadFromMySQL(PTransform):
    """Create PCollection from MySQL."""

    query: str
    host: str
    database: str
    user: str
    password: str
    port: int = 3306

    def __post_init__(self):
        self.config = {
            "host": self.host,
            "database": self.database,
            "user": self.user,
            "password": self.password,
            "port": self.port,
        }

    def expand(self, pcoll: PCollection) -> PCollection:
        return pcoll | iobase.Read(MySQLSource(self.query, self.config))


@dataclasses.dataclass
class WriteToMySQL(PTransform):
    """Write dict rows to MySQL."""

    host: str
    database: str
    table: str
    user: str
    password: str
    port: int = 3306
    batch_size: int = 0

    def __post_init__(self):
        self.config = {
            "host": self.host,
            "database": self.database,
            "user": self.user,
            "password": self.password,
            "port": self.port,
        }

    def expand(self, pcoll: PCollection) -> PCollection:
        return pcoll | beam.ParDo(_WriteToMySQLFn(self.config, self.table, self.batch_size))


@dataclasses.dataclass
class _WriteToMySQLFn(beam.DoFn):
    """DoFn for WriteToMySQL."""

    config: Dict
    table: str
    batch_size: int

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
        query = f"INSERT INTO {self.config['database']}.{self.table}({column_str}) VALUES({value_str});"

        self._queries.append(query)

        if len(self._queries) > self.batch_size:
            self.client.record_loader("\n".join(self._queries))
            self._queries.clear()

    def finish_bundle(self):
        if len(self._queries):
            self.client.record_loader("\n".join(self._queries))
            self._queries.clear()

    def _build_value(self):
        for k, v in self.config.items():
            self.config[k] = get_runtime_value(v)
        self.table = get_runtime_value(self.table)
        self.batch_size = get_runtime_value(self.batch_size)

        self.client = MySQLClient(self.config)
