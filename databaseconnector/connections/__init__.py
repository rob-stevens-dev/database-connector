"""
Database connections package.

This package contains database-specific connection implementations
for different database systems.
"""

from databaseconnector.connections.base import DatabaseConnection
from databaseconnector.connections.postgres import PostgreSQLConnection
from databaseconnector.connections.mysql import MySQLConnection
from databaseconnector.connections.oracle import OracleConnection
from databaseconnector.connections.mssql import MSSQLConnection
from databaseconnector.connections.sqlite import SQLiteConnection

__all__ = [
    'DatabaseConnection',
    'PostgreSQLConnection',
    'MySQLConnection',
    'OracleConnection',
    'MSSQLConnection',
    'SQLiteConnection'
]