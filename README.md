# Beam - MySQL Connector
[![PyPI version](https://badge.fury.io/py/beam-mysql-connector2.svg)](https://badge.fury.io/py/beam-mysql-connector2)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/beam-mysql-connector2)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Beam - MySQL Connector (with Python 3.12 support) is an io connector of [Apache Beam](https://beam.apache.org/) to access MySQL databases.
This is a direct fork of [beam-mysql-connector](https://github.com/esakik/beam-mysql-connector).

## Installation
```bash
pip install beam-mysql-connector2
```

## Getting Started
- Read From MySQL
```Python
from beam_mysql.connector import splitters
from beam_mysql.connector.io import ReadFromMySQL


read_from_mysql = ReadFromMySQL(
        query="SELECT * FROM test_db.tests;",
        host="localhost",
        database="test_db",
        user="test",
        password="test",
        port=3306,
        splitter=splitters.NoSplitter()  # you can select how to split query for performance
)
```

- Write To MySQL
```Python
from beam_mysql.connector.io import WriteToMySQL


write_to_mysql = WriteToMySQL(
        host="localhost",
        database="test_db",
        table="tests",
        user="test",
        password="test",
        port=3306,
        batch_size=1000,
)
```

## License
MIT License. Please refer to the [LICENSE.txt](https://github.com/gnomezgrave/beam-mysql-connector2/blob/master/LICENSE.txt), for further details.