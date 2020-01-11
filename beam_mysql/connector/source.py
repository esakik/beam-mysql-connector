"""A source that reads a finite amount of records on mysql."""

from apache_beam.io import iobase

from beam_mysql.connector import splitters
from beam_mysql.connector.client import MySQLClient
from beam_mysql.connector.utils import cleanse_query
from beam_mysql.connector.utils import get_runtime_value


class MySQLSource(iobase.BoundedSource):
    """A source object of mysql."""

    def __init__(
        self,
        query: str,
        host: str,
        database: str,
        user: str,
        password: str,
        port: int,
        splitter: splitters.BaseSplitter,
    ):
        super().__init__()
        self._query = query
        self._host = host
        self._database = database
        self._user = user
        self._password = password
        self._port = port

        self._is_builded = False

        self._config = {
            "host": self._host,
            "database": self._database,
            "user": self._user,
            "password": self._password,
            "port": self._port,
        }

        self._splitter = splitter

    def estimate_size(self):
        """Implement :class:`~apache_beam.io.iobase.BoundedSource.estimate_size`"""
        return self._splitter.estimate_size()

    def get_range_tracker(self, start_position, stop_position):
        """Implement :class:`~apache_beam.io.iobase.BoundedSource.get_range_tracker`"""
        if not self._is_builded:
            self._build_value()

        return self._splitter.get_range_tracker(start_position, stop_position)

    def read(self, range_tracker):
        """Implement :class:`~apache_beam.io.iobase.BoundedSource.read`"""
        for record in self._splitter.read(range_tracker):
            yield record

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        """Implement :class:`~apache_beam.io.iobase.BoundedSource.split`"""
        if not self._is_builded:
            self._build_value()

        for split in self._splitter.split(desired_bundle_size, start_position, stop_position):
            yield split

    def _build_value(self):
        for k, v in self._config.items():
            self._config[k] = get_runtime_value(v)

        self.query = cleanse_query(get_runtime_value(self._query))
        self.client = MySQLClient(self._config)
        self._splitter.build_source(self)

        self._is_builded = True
