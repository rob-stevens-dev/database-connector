"""
PostgreSQL database connection module.

This module provides the PostgreSQLConnection class for connecting to
PostgreSQL databases with PostgreSQL-specific functionality.
"""

import logging
from typing import Dict, Any, Optional, List, Tuple

from databaseconnector.connections.base import DatabaseConnection
from databaseconnector.config import DatabaseConfig
from databaseconnector.interfaces import ConnectionStrategy, ConnectionError, QueryError


class PostgreSQLConnection(DatabaseConnection):
    """PostgreSQL-specific database connection.
    
    This class extends the base DatabaseConnection with PostgreSQL-specific
    functionality such as schema operations, table information, etc.
    
    Attributes:
        config (DatabaseConfig): The database configuration
        logger (logging.Logger): Logger instance
        connection_strategy (ConnectionStrategy): Strategy for handling the connection
    """
    
    def __init__(self, config: DatabaseConfig, logger=None, connection_strategy: ConnectionStrategy = None):
        """
        Initialize a PostgreSQL database connection.
        
        Args:
            config: Database configuration
            logger: Optional logger instance
            connection_strategy: Strategy for handling the connection
        """
        super().__init__(config, logger, connection_strategy)
        
        # Ensure the driver parameter is set
        if 'driver' not in config.connection_params:
            config.connection_params['driver'] = 'postgresql'
    
    def get_tables(self) -> List[Dict[str, Any]]:
        """
        Get list of tables in the database.
        
        Returns:
            List of tables in the current database
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        query = """
            SELECT 
                tablename AS table_name,
                schemaname AS schema_name
            FROM pg_catalog.pg_tables 
            WHERE schemaname != 'pg_catalog' 
            AND schemaname != 'information_schema'
            ORDER BY schemaname, tablename
        """
        return self.execute_query(query)
        
    def create_schema(self, name: str) -> None:
        """
        Create a new schema.
        
        Args:
            name: Schema name
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        # Use parameterized query to prevent SQL injection
        # Note: Schema names cannot be parameterized in PostgreSQL, so we
        # need to validate the schema name to prevent SQL injection
        if not name.isalnum() and not all(c in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' for c in name):
            raise ValueError("Invalid schema name. Schema names must contain only alphanumeric characters and underscores.")
            
        self.execute_query(f"CREATE SCHEMA IF NOT EXISTS {name}")
        self.logger.info(f"Created schema: {name}")
        
    def get_schema_names(self) -> List[str]:
        """
        Get list of schema names.
        
        Returns:
            List of schema names
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        result = self.execute_query("SELECT schema_name FROM information_schema.schemata")
        return [row['schema_name'] for row in result]
        
    def get_table_info(self, table_name: str, schema_name: str = 'public') -> Dict[str, Any]:
        """
        Get detailed information about a table.
        
        Args:
            table_name: Name of the table
            schema_name: Name of the schema (default: 'public')
            
        Returns:
            Dictionary with table information
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        # Get column information
        columns_query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns 
            WHERE table_name = :table_name
            AND table_schema = :schema_name
            ORDER BY ordinal_position
        """
        
        columns = self.execute_query(
            columns_query,
            {"table_name": table_name, "schema_name": schema_name}
        )
        
        # Get primary key information
        pk_query = """
            SELECT 
                tc.constraint_name,
                kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_name = :table_name
            AND tc.table_schema = :schema_name
        """
        
        primary_keys = self.execute_query(
            pk_query,
            {"table_name": table_name, "schema_name": schema_name}
        )
        
        # Get index information
        index_query = """
            SELECT
                indexname AS index_name,
                indexdef AS index_definition
            FROM pg_indexes
            WHERE tablename = :table_name
            AND schemaname = :schema_name
        """
        
        indexes = self.execute_query(
            index_query,
            {"table_name": table_name, "schema_name": schema_name}
        )
        
        # Get foreign key information
        fk_query = """
            SELECT
                tc.constraint_name,
                kcu.column_name,
                ccu.table_schema AS foreign_table_schema,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_name = :table_name
            AND tc.table_schema = :schema_name
        """
        
        foreign_keys = self.execute_query(
            fk_query,
            {"table_name": table_name, "schema_name": schema_name}
        )
        
        # Combine all information into a comprehensive table info dictionary
        return {
            "table_name": table_name,
            "schema_name": schema_name,
            "columns": columns,
            "primary_keys": primary_keys,
            "indexes": indexes,
            "foreign_keys": foreign_keys
        }
    
    def vacuum_table(self, table_name: str, schema_name: str = 'public') -> None:
        """
        Perform VACUUM operation on a specific table.
        
        Args:
            table_name: Name of the table
            schema_name: Name of the schema (default: 'public')
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        # Validate table and schema names to prevent SQL injection
        if not table_name.isalnum() and not all(c in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' for c in table_name):
            raise ValueError("Invalid table name. Table names must contain only alphanumeric characters and underscores.")
            
        if not schema_name.isalnum() and not all(c in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' for c in schema_name):
            raise ValueError("Invalid schema name. Schema names must contain only alphanumeric characters and underscores.")
            
        self.execute_query(f"VACUUM {schema_name}.{table_name}")
        self.logger.info(f"Vacuum performed on {schema_name}.{table_name}")
    
    def create_index(self, table_name: str, column_names: List[str], index_name: str = None, 
                    schema_name: str = 'public', unique: bool = False) -> None:
        """
        Create an index on a table.
        
        Args:
            table_name: Name of the table
            column_names: List of column names to include in the index
            index_name: Name of the index (if None, a name will be generated)
            schema_name: Name of the schema (default: 'public')
            unique: Whether the index should enforce uniqueness
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        # Validate table and schema names to prevent SQL injection
        if not table_name.isalnum() and not all(c in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' for c in table_name):
            raise ValueError("Invalid table name. Table names must contain only alphanumeric characters and underscores.")
            
        if not schema_name.isalnum() and not all(c in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' for c in schema_name):
            raise ValueError("Invalid schema name. Schema names must contain only alphanumeric characters and underscores.")
            
        for column_name in column_names:
            if not column_name.isalnum() and not all(c in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' for c in column_name):
                raise ValueError("Invalid column name. Column names must contain only alphanumeric characters and underscores.")
        
        # Generate an index name if none provided
        if index_name is None:
            index_name = f"idx_{table_name}_{'_'.join(column_names)}"
            
        # Validate index name
        if not index_name.isalnum() and not all(c in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' for c in index_name):
            raise ValueError("Invalid index name. Index names must contain only alphanumeric characters and underscores.")
        
        # Build the index creation query
        unique_clause = "UNIQUE " if unique else ""
        column_list = ", ".join(column_names)
        
        query = f"CREATE {unique_clause}INDEX {index_name} ON {schema_name}.{table_name} ({column_list})"
        
        self.execute_query(query)
        self.logger.info(f"Created {'' if not unique else 'unique '}index {index_name} on {schema_name}.{table_name}")
    
    def get_server_version(self) -> str:
        """
        Get the PostgreSQL server version.
        
        Returns:
            Server version string
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        result = self.execute_query("SHOW server_version")
        return result[0]['server_version'] if result else None