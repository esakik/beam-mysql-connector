"""A source that reads a finite amount of records on mysql."""

import dataclasses
from typing import Dict

from apache_beam.io import iobase
from apache_beam.io.range_trackers import OffsetRangeTracker

from beam_mysql.connector.client import MySQLClient

_ESTIMATE_SIZE_BUFFER = 10


@dataclasses.dataclass
class MySQLSource(iobase.BoundedSource):
    query: str
    config: Dict

    def __post_init__(self):
        self.client = MySQLClient(self.config)

        almost_counts = self.client.estimate_almost_counts(self.query)
        # counts not accuracy so increase estimated data size
        self.counts = almost_counts * _ESTIMATE_SIZE_BUFFER
        self.split_size = self.counts // 10000

    def estimate_size(self):
        return self.counts

    def get_range_tracker(self, start_position, stop_position):
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = self.counts

        return OffsetRangeTracker(start_position, stop_position)

    def read(self, range_tracker):
        rows_generator = self.client.row_generator(self.query)

        for i in range(range_tracker.start_position(), range_tracker.stop_position()):
            if not range_tracker.try_claim(i):
                return

            yield next(rows_generator)

        while True:
            next_object = next(rows_generator)
            if next_object:
                yield next_object
            else:
                break

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = self.counts

        bundle_start = start_position
        bundle_stop = self.split_size
        while bundle_start < stop_position:
            yield iobase.SourceBundle(
                weight=desired_bundle_size,
                source=self,
                start_position=bundle_start,
                stop_position=bundle_stop
            )

            bundle_start = bundle_stop
            bundle_stop += self.split_size
