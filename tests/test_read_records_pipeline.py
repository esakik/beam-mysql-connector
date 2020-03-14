"""A test of read records pipeline."""

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from beam_mysql.connector import splitters
from beam_mysql.connector.io import ReadFromMySQL
from tests.test_base import TestBase


class TestReadRecordsPipeline(TestBase):
    def test_pipeline_default_splitter(self):
        expected = [{"id": 1, "name": "test data1"}, {"id": 2, "name": "test data2"}]

        with TestPipeline() as p:
            # Access to mysql on docker
            read_from_mysql = ReadFromMySQL(
                query="SELECT * FROM test_db.tests;",
                host="0.0.0.0",
                database="test_db",
                user="root",
                password="root",
                port=3307,
                splitter=splitters.DefaultSplitter(),
            )

            actual = p | read_from_mysql

            assert_that(actual, equal_to(expected))

    def test_pipeline_limit_offset_splitter(self):
        expected = [{"id": 1, "name": "test data1"}, {"id": 2, "name": "test data2"}]

        with TestPipeline() as p:
            # Access to mysql on docker
            read_from_mysql = ReadFromMySQL(
                query="SELECT * FROM test_db.tests;",
                host="0.0.0.0",
                database="test_db",
                user="root",
                password="root",
                port=3307,
                splitter=splitters.LimitOffsetSplitter(),
            )

            actual = p | read_from_mysql

            assert_that(actual, equal_to(expected))

    def test_pipeline_ids_splitter(self):
        expected = [{"id": 1, "name": "test data1"}]

        with TestPipeline() as p:
            # Access to mysql on docker
            read_from_mysql = ReadFromMySQL(
                query="SELECT * FROM test_db.tests WHERE id IN ({ids});",
                host="0.0.0.0",
                database="test_db",
                user="root",
                password="root",
                port=3307,
                splitter=splitters.IdsSplitter(generate_ids_fn=lambda: [1]),
            )

            actual = p | read_from_mysql

            assert_that(actual, equal_to(expected))
