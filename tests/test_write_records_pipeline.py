"""A test of write records pipeline."""

import apache_beam as beam
import datetime
from apache_beam.testing.test_pipeline import TestPipeline

from beam_mysql.connector.io import WriteToMySQL
from tests.helper.mysql import build_connection
from tests.test_base import TestBase

HOST = "0.0.0.0"
DATABASE = "test_db"
TABLE = "tests"
USER = "root"
PASSWORD = "root"
PORT = 3307
BATCH_SIZE = 0


class TestWriteRecordsPipeline(TestBase):
    def setUp(self):
        self.config = {
            "host": HOST,
            "database": DATABASE,
            "user": USER,
            "password": PASSWORD,
            "port": PORT,
        }
        self.conn = build_connection(self.config)

    def tearDown(self):
        cur = self.conn.cursor()
        cur.execute(f"DELETE FROM {DATABASE}.{TABLE} WHERE id = 6;")
        cur.close()
        self.conn.commit()
        self.conn.close()

    def test_pipeline(self):
        expected = [
            {"id": 1, "name": "test data1", "date": datetime.date(2020, 1, 1), "memo": "memo1"},
            {"id": 2, "name": "test data2", "date": datetime.date(2020, 2, 2), "memo": None},
            {"id": 3, "name": "test data3", "date": datetime.date(2020, 3, 3), "memo": "memo3"},
            {"id": 4, "name": "test data4", "date": datetime.date(2020, 4, 4), "memo": None},
            {"id": 5, "name": "test data5", "date": datetime.date(2020, 5, 5), "memo": None},
            {"id": 6, "name": "test data6", "date": datetime.date(2020, 6, 6), "memo": None},
        ]

        with TestPipeline() as p:
            # Access to mysql on docker
            write_to_mysql = WriteToMySQL(
                host=HOST,
                database=DATABASE,
                table=TABLE,
                user=USER,
                password=PASSWORD,
                port=PORT,
                batch_size=BATCH_SIZE,
            )

            (p | beam.Create([{"id": 6, "name": "test data6", "date": "2020-06-06", "memo": None}]) | write_to_mysql)

        cur = self.conn.cursor(dictionary=True)
        cur.execute(f"SELECT * FROM {DATABASE}.{TABLE}")
        actual = cur.fetchall()
        cur.close()

        self.assertEqual(actual, expected)
