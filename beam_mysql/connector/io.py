"""Sources and sinks of mysql."""

import dataclasses

from apache_beam.io import iobase
from apache_beam.pvalue import PCollection
from apache_beam.transforms.core import PTransform

from beam_mysql.connector.source import MySQLSource


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
