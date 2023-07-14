"""A example to read records on mysql."""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_mysql.connector.io import WriteToMySQL


class WriteRecordsOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument("--host", dest="host", default="0.0.0.0")
        parser.add_value_provider_argument("--port", dest="port", default=3307)
        parser.add_value_provider_argument(
            "--database", dest="database", default="test_db"
        )
        parser.add_value_provider_argument("--table", dest="table", default="tests")
        parser.add_value_provider_argument("--user", dest="user", default="root")
        parser.add_value_provider_argument(
            "--password", dest="password", default="root"
        )
        parser.add_value_provider_argument("--batch_size", dest="batch_size", default=0)


def run():
    options = WriteRecordsOptions()

    p = beam.Pipeline(options=options)

    write_to_mysql = WriteToMySQL(
        host=options.host,
        database=options.database,
        table=options.table,
        user=options.user,
        password=options.password,
        port=options.port,
        batch_size=options.batch_size,
    )

    (
        p
        | "ReadFromInMemory"
        >> beam.Create([{"name": "test data3"}, {"name": "test data4"}])
        | "NoTransform" >> beam.Map(lambda e: e)
        | "WriteToMySQL" >> write_to_mysql
    )

    p.run().wait_until_finish()


if __name__ == "__main__":
    run()
