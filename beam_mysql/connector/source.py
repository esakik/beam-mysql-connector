"""A source that reads a finite amount of records on mysql."""

from apache_beam.io import iobase
from apache_beam.io.range_trackers import OffsetRangeTracker

from beam_mysql.connector.client import MySQLClient
from beam_mysql.connector.utils import cleanse_query
from beam_mysql.connector.utils import get_runtime_value

_COUNTS_RANGE_BUFFER = 10
_COUNTS_SPLIT_SIZE = 10000


class MySQLSource(iobase.BoundedSource):
    """A source object of mysql."""

    def __init__(self, query: str, host: str, database: str, user: str, password: str, port: int):
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

    def estimate_size(self):
        """Implement :class:`~apache_beam.io.iobase.BoundedSource.estimate_size`"""
        return self._counts

    def get_range_tracker(self, start_position, stop_position):
        """Implement :class:`~apache_beam.io.iobase.BoundedSource.get_range_tracker`"""
        if not self._is_builded:
            self._build_value()

        if start_position is None:
            start_position = 0
        if stop_position is None:
            # OPTIMIZE: fix algorithm to calculate stop position
            stop_position = self._counts * _COUNTS_RANGE_BUFFER

        return OffsetRangeTracker(start_position, stop_position)

    def read(self, range_tracker):
        """Implement :class:`~apache_beam.io.iobase.BoundedSource.read`"""
        record_generator = self._client.record_generator(self._query)

        for i in range(range_tracker.start_position(), range_tracker.stop_position()):
            next_object = next(record_generator, None)

            if not next_object or not range_tracker.try_claim(i):
                return

            yield next_object

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        """Implement :class:`~apache_beam.io.iobase.BoundedSource.split`"""
        if not self._is_builded:
            self._build_value()

        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = self._counts

        bundle_start = start_position
        bundle_stop = self._chunk_size
        while bundle_start < stop_position:
            yield iobase.SourceBundle(
                weight=desired_bundle_size, source=self, start_position=bundle_start, stop_position=bundle_stop
            )

            bundle_start = bundle_stop
            bundle_stop += self._chunk_size

    def _build_value(self):
        for k, v in self._config.items():
            self._config[k] = get_runtime_value(v)
        self._query = cleanse_query(get_runtime_value(self._query))

        self._client = MySQLClient(self._config)

        rough_counts = self._client.rough_counts_estimator(self._query)
        self._counts = rough_counts

        # OPTIMIZE: fix algorithm to calculate chunk size
        self._chunk_size = self._counts // _COUNTS_SPLIT_SIZE

        self._is_builded = True
