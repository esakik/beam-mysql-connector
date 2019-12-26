"""A source that reads a finite amount of records on mysql."""

import dataclasses
from typing import Dict
from typing import Any

from apache_beam.io import iobase
from apache_beam.io.range_trackers import OffsetRangeTracker

from beam_mysql.connector.client import MySQLClient
from apache_beam.options.value_provider import RuntimeValueProvider

_ESTIMATE_SIZE_BUFFER = 10


@dataclasses.dataclass
class MySQLSource(iobase.BoundedSource):
    """A source object of mysql."""

    query: str
    config: Dict

    def estimate_size(self):
        """Implement :class:`~apache_beam.io.iobase.BoundedSource.estimate_size`"""
        return self.counts

    def get_range_tracker(self, start_position, stop_position):
        """Implement :class:`~apache_beam.io.iobase.BoundedSource.get_range_tracker`"""
        self._build_value()

        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = self.counts

        return OffsetRangeTracker(start_position, stop_position)

    def read(self, range_tracker):
        """Implement :class:`~apache_beam.io.iobase.BoundedSource.read`"""
        record_generator = self.client.record_generator(self.query)

        for i in range(range_tracker.start_position(), range_tracker.stop_position()):
            next_object = next(record_generator, None)

            if not next_object or not range_tracker.try_claim(i):
                return

            yield next_object

        while True:
            next_object = next(record_generator, None)
            if next_object:
                yield next_object
            else:
                break

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        """Implement :class:`~apache_beam.io.iobase.BoundedSource.split`"""
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = self.counts

        bundle_start = start_position
        bundle_stop = self.split_size
        while bundle_start < stop_position:
            yield iobase.SourceBundle(
                weight=desired_bundle_size, source=self, start_position=bundle_start, stop_position=bundle_stop
            )

            bundle_start = bundle_stop
            bundle_stop += self.split_size

    def _build_value(self):
        for k, v in self.config.items():
            self.config[k] = self._get_runtime_value(v)

        self.query = self._get_runtime_value(self.query).strip(";")

        self.client = MySQLClient(self.config)
        rough_counts = self.client.rough_counts_estimator(self.query)

        # counts not accuracy so increase estimated data size
        self.counts = rough_counts * _ESTIMATE_SIZE_BUFFER
        self.split_size = self.counts // 10000

    @staticmethod
    def _get_runtime_value(value: Any) -> Any:
        if isinstance(value, RuntimeValueProvider):
            return value.get()
        else:
            return value
