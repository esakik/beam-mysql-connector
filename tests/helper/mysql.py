from typing import Dict

import mysql.connector


def build_connection(config: Dict):
    conn = mysql.connector.connect(**config)
    conn.autocommit = False
    return conn
