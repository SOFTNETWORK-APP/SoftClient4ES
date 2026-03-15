"""
SQLAlchemy dialect that registers the ``elastiq://`` URI scheme.

Delegates entirely to flightsql-dbapi's FlightSQLDialect — the Elasticsearch
Arrow Flight SQL server returns all required SqlInfo metadata and schema
information natively.
"""

from flightsql.sqlalchemy import FlightSQLDialect


class ElastiqDialect(FlightSQLDialect):
    name = "elastiq"
    supports_statement_cache = False
