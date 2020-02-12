"""A wrapper class of `~apache_beam.io.iobase.BoundedSource` functions."""

import re
from abc import ABCMeta
from abc import abstractmethod
from typing import Callable
from typing import Iterator

from apache_beam.io import iobase
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.io.range_trackers import UnsplittableRangeTracker


class BaseSplitter(metaclass=ABCMeta):
    """Abstract class of splitter."""

    def build_source(self, source):
        """Build source on runtime."""
        self.source = source

    @abstractmethod
    def estimate_size(self):
        """Wrap :class:`~apache_beam.io.iobase.BoundedSource.estimate_size`"""
        raise NotImplementedError()

    @abstractmethod
    def get_range_tracker(self, start_position, stop_position):
        """Wrap :class:`~apache_beam.io.iobase.BoundedSource.get_range_tracker`"""
        raise NotImplementedError()

    @abstractmethod
    def read(self, range_tracker):
        """Wrap :class:`~apache_beam.io.iobase.BoundedSource.read`"""
        raise NotImplementedError()

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        """Wrap :class:`~apache_beam.io.iobase.BoundedSource.split`"""
        raise NotImplementedError()


class DefaultSplitter(BaseSplitter):
    """No split bounded source."""

    def estimate_size(self):
        return 0

    def get_range_tracker(self, start_position, stop_position):
        return LexicographicKeyRangeTracker(start_position, stop_position)

    def read(self, range_tracker):
        if range_tracker.start_position() in (None, 1000):
            for record in self.source.client.record_generator(self.source.query):
                yield record
        else:
            return None

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        for i in range(1, 1001):
            yield iobase.SourceBundle(
                weight=desired_bundle_size, source=self.source, start_position=i, stop_position=None
            )


class NoSplitter(BaseSplitter):
    """No split bounded source and prohibit scale."""

    def estimate_size(self):
        return 0

    def get_range_tracker(self, start_position, stop_position):
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = OffsetRangeTracker.OFFSET_INFINITY

        return UnsplittableRangeTracker(OffsetRangeTracker(start_position, stop_position))

    def read(self, range_tracker):
        for record in self.source.client.record_generator(self.source.query):
            yield record

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = OffsetRangeTracker.OFFSET_INFINITY

        yield iobase.SourceBundle(
            weight=desired_bundle_size, source=self, start_position=start_position, stop_position=stop_position
        )


class IdsSplitter(BaseSplitter):
    """Split bounded source by any ids."""

    def __init__(self, generate_ids_fn: Callable[[], Iterator], batch_size: int = 1000):
        self._generate_ids_fn = generate_ids_fn
        self._batch_size = batch_size

    def estimate_size(self):
        return 0

    def get_range_tracker(self, start_position, stop_position):
        return LexicographicKeyRangeTracker(start_position, stop_position)

    def read(self, range_tracker):
        if range_tracker.start_position() is None:
            range_tracker._start_position = ",".join([f"'{id}'" for id in self._generate_ids_fn()])
        elif not range_tracker.start_position():
            return None

        query = self.source.query.format(ids=range_tracker.start_position())

        for record in self.source.client.record_generator(query):
            yield record

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        condensed_query = self.source.query.lower().replace(" ", "")

        if not re.search(r"in\({ids}\)", condensed_query):
            raise ValueError(f"Require 'in' phrase and 'ids' key on query if use 'IdsSplitter': {self.source.query}")
        elif re.search(r"notin\({ids}\)", condensed_query):
            ids = [generated_id for generated_id in self._generate_ids_fn()]

            for i in range(1, self._batch_size):
                yield self._create_bundle_source(desired_bundle_size, self.source, [])

            yield self._create_bundle_source(desired_bundle_size, self.source, ids)
        else:
            ids = []

            for generated_id in self._generate_ids_fn():
                if len(ids) == self._batch_size:
                    yield self._create_bundle_source(desired_bundle_size, self.source, ids)
                    ids.clear()
                else:
                    ids.append(generated_id)

            yield self._create_bundle_source(desired_bundle_size, self.source, ids)

    @staticmethod
    def _create_bundle_source(desired_bundle_size, source, ids):
        if isinstance(ids, list):
            ids_str = ",".join([f"'{id}'" for id in ids])
        elif isinstance(ids, str):
            ids_str = ids
        else:
            raise ValueError(f"Unexpected ids: {ids}")

        return iobase.SourceBundle(
            weight=desired_bundle_size, source=source, start_position=ids_str, stop_position=None
        )
