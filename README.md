# Beam - MySQL Connector
[![PyPI version](https://badge.fury.io/py/beam-mysql-connector.svg)](https://badge.fury.io/py/beam-mysql-connector)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/beam-mysql-connector)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/9d5d5727996e49e19bed91ac57bb1346)](https://www.codacy.com/manual/esaki01/beam-mysql-connector?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=esaki01/beam-mysql-connector&amp;utm_campaign=Badge_Grade)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Beam - MySQL Connector is an io connector of [Apache Beam](https://beam.apache.org/) to access MySQL databases.

## Installation
```bash
pip install beam-mysql-connector
```

## Getting Started
- Read From MySQL
```Python
from beam_mysql.connector.io import ReadFromMySQL


read_from_mysql = ReadFromMySQL(
        query="SELECT * FROM test_db.tests;",
        host="localhost",
        database="test_db",
        user="test",
        password="test",
        port=3306,
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
MIT License. Please refer to the [LICENSE.txt](https://github.com/esaki01/beam-mysql-connector/blob/master/LICENSE.txt), for further details.