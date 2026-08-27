"""Ingestion service: public REST API -> PostgreSQL (the OLTP layer).

Everything downstream of Postgres is driven by CDC, so this package has exactly
one job and does it carefully: land validated, deduplicated rows in the
relational store, and be honest in its metrics about how current they are.
"""

__version__ = "1.0.0"
