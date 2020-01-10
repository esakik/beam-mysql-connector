from abc import abstractmethod, ABCMeta

from apache_beam.io import iobase
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.io.range_trackers import UnsplittableRangeTracker

from beam_mysql.connector.utils import cleanse_query
from beam_mysql.connector.utils import get_runtime_value


class BaseProcedure(metaclass=ABCMeta):
    def build_source(self, source):
        self._source = source

    @abstractmethod
    def estimate_size(self):
        raise NotImplementedError()

    @abstractmethod
    def get_range_tracker(self, start_position, stop_position):
        raise NotImplementedError()

    @abstractmethod
    def read(self, range_tracker):
        raise NotImplementedError()

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        raise NotImplementedError()


class DefaultProcedure(BaseProcedure):
    def estimate_size(self):
        return 0

    def get_range_tracker(self, start_position, stop_position):
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = OffsetRangeTracker.OFFSET_INFINITY

        return UnsplittableRangeTracker(OffsetRangeTracker(start_position, stop_position))

    def read(self, range_tracker):
        for record in self._source._client.record_generator(self._source._query):
            yield record

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = OffsetRangeTracker.OFFSET_INFINITY

        return iobase.SourceBundle(
            weight=desired_bundle_size, source=self, start_position=start_position, stop_position=stop_position
        )


class IdsProcedure(BaseProcedure):
    def __init__(self, subquery, batch_size=1000):
        self._subquery = subquery
        self._batch_size = batch_size

    def estimate_size(self):
        return 0

    def get_range_tracker(self, start_position, stop_position):
        return LexicographicKeyRangeTracker(start_position, stop_position)

    def read(self, range_tracker):
        # TODO ids match
        query = self._source._query.format(ids=range_tracker.start_position())

        for record in self._source._client.record_generator(query):
            yield record

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        def _generate_source(_source, _tmp_list):
            _ids = ",".join([f"'{t}'" for t in _tmp_list])
            _tmp_list.clear()

            return iobase.SourceBundle(
                weight=desired_bundle_size, source=_source, start_position=_ids, stop_position=None
            )

        query = cleanse_query(get_runtime_value(self._subquery))
        ids_generator = self._source._client.record_generator(query, False)

        tmp_list = []
        for tupled_result in ids_generator:
            if len(tmp_list) == self._batch_size:
                yield _generate_source(self._source, tmp_list)
            else:
                tmp_list.append(tupled_result[0])

        yield _generate_source(self._source, tmp_list)
