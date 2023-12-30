"""A test of read records pipeline."""

from datetime import date

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from beam_mysql.connector import splitters
from beam_mysql.connector.io import ReadFromMySQL
from tests.test_base import TestBase


class TestReadRecordsPipeline(TestBase):
    def test_pipeline_no_splitter(self):
        expected = [
            {"id": 1, "name": "test data1", "date": date(2020, 1, 1), "memo": "memo1"},
            {"id": 2, "name": "test data2", "date": date(2020, 2, 2), "memo": None},
            {"id": 3, "name": "test data3", "date": date(2020, 3, 3), "memo": "memo3"},
            {"id": 4, "name": "test data4", "date": date(2020, 4, 4), "memo": None},
            {"id": 5, "name": "test data5", "date": date(2020, 5, 5), "memo": None},
        ]

        with TestPipeline() as p:
            # Access to mysql on docker
            read_from_mysql = ReadFromMySQL(
                query="SELECT * FROM test_db.tests;",
                host="0.0.0.0",
                database="test_db",
                user="root",
                password="root",
                port=3307,
                splitter=splitters.NoSplitter(),
            )

            actual = p | read_from_mysql

            assert_that(actual, equal_to(expected))

    def test_pipeline_limit_offset_splitter(self):
        expected = [
            {"id": 1, "name": "test data1", "date": date(2020, 1, 1), "memo": "memo1"},
            {"id": 2, "name": "test data2", "date": date(2020, 2, 2), "memo": None},
            {"id": 3, "name": "test data3", "date": date(2020, 3, 3), "memo": "memo3"},
            {"id": 4, "name": "test data4", "date": date(2020, 4, 4), "memo": None},
            {"id": 5, "name": "test data5", "date": date(2020, 5, 5), "memo": None},
        ]

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
        expected = [
            {"id": 1, "name": "test data1", "date": date(2020, 1, 1), "memo": "memo1"},
            {"id": 2, "name": "test data2", "date": date(2020, 2, 2), "memo": None},
        ]

        with TestPipeline() as p:
            # Access to mysql on docker
            read_from_mysql = ReadFromMySQL(
                query="SELECT * FROM test_db.tests WHERE id IN ({ids});",
                host="0.0.0.0",
                database="test_db",
                user="root",
                password="root",
                port=3307,
                splitter=splitters.IdsSplitter(generate_ids_fn=lambda: [1, 2]),
            )

            actual = p | read_from_mysql

            assert_that(actual, equal_to(expected))

    def test_pipeline_date_splitter(self):
        expected = [
            {"id": 1, "name": "test data1", "date": date(2020, 1, 1), "memo": "memo1"},
            {"id": 2, "name": "test data2", "date": date(2020, 2, 2), "memo": None},
            {"id": 3, "name": "test data3", "date": date(2020, 3, 3), "memo": "memo3"},
        ]

        with TestPipeline() as p:
            # Access to mysql on docker
            read_from_mysql = ReadFromMySQL(
                query="SELECT * FROM test_db.tests WHERE date BETWEEN '2020-01-01' AND '2020-03-03';",
                host="0.0.0.0",
                database="test_db",
                user="root",
                password="root",
                port=3307,
                splitter=splitters.DateSplitter(),
            )

            actual = p | read_from_mysql

            assert_that(actual, equal_to(expected))

    def test_pipeline_partitions_splitter(self):
        expected = [
            {"id": 2, "name": "test data2", "date": date(2020, 2, 2), "memo": None},
            {"id": 3, "name": "test data3", "date": date(2020, 3, 3), "memo": "memo3"},
        ]

        with TestPipeline() as p:
            # Access to mysql on docker
            read_from_mysql = ReadFromMySQL(
                query="SELECT * FROM test_db.tests PARTITION (p202002,p202003);",
                host="0.0.0.0",
                database="test_db",
                user="root",
                password="root",
                port=3307,
                splitter=splitters.PartitionSplitter(),
            )

            actual = p | read_from_mysql

            assert_that(actual, equal_to(expected))
