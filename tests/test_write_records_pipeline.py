"""A test of write records pipeline."""

import apache_beam as beam
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
        cur.execute(f"DELETE FROM {DATABASE}.{TABLE} WHERE id IN (3, 4)")
        cur.close()
        self.conn.commit()
        self.conn.close()

    def test_pipeline(self):
        expected = [
            {"id": 1, "name": "test data1"},
            {"id": 2, "name": "test data2"},
            {"id": 3, "name": "test data3"},
            {"id": 4, "name": "test data4"},
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

            p | beam.Create([{"id": 3, "name": "test data3"}, {"id": 4, "name": "test data4"}]) | write_to_mysql

        cur = self.conn.cursor(dictionary=True)
        cur.execute(f"SELECT * FROM {DATABASE}.{TABLE}")
        actual = cur.fetchall()
        cur.close()

        self.assertEqual(actual, expected)
