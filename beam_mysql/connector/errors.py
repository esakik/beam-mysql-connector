"""Beam - MySQL error classes."""


class BeamMySQLError(Exception):
    """Base class for all Beam MySQL errors."""


class MySQLClientError(BeamMySQLError):
    """An error in the mysql client object (e.g. failed to execute query)."""
