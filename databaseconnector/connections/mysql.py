"""
MySQL database connection module.

This module provides the MySQLConnection class for connecting to
MySQL/MariaDB databases with MySQL-specific functionality.
"""

import logging
from typing import Dict, Any, Optional, List, Tuple

from databaseconnector.connections.base import DatabaseConnection
from databaseconnector.config import DatabaseConfig
from databaseconnector.interfaces import ConnectionStrategy, ConnectionError, QueryError


class MySQLConnection(DatabaseConnection):
    """MySQL-specific database connection.
    
    This class extends the base DatabaseConnection with MySQL-specific
    functionality such as table operations, metadata access, etc.
    
    Attributes:
        config (DatabaseConfig): The database configuration
        logger (logging.Logger): Logger instance
        connection_strategy (ConnectionStrategy): Strategy for handling the connection
    """
    
    def __init__(self, config: DatabaseConfig, logger=None, connection_strategy: ConnectionStrategy = None):
        """
        Initialize a MySQL database connection.
        
        Args:
            config: Database configuration
            logger: Optional logger instance
            connection_strategy: Strategy for handling the connection
        """
        super().__init__(config, logger, connection_strategy)
        
        # Ensure the driver parameter is set
        if 'driver' not in config.connection_params:
            config.connection_params['driver'] = 'mysql+pymysql'
            
    def show_tables(self) -> List[Dict[str, Any]]:
        """
        Get list of tables in the current database.
        
        Returns:
            List of tables in the current database
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        return self.execute_query("SHOW TABLES")
        
    def get_server_version(self) -> str:
        """
        Get the MySQL server version.
        
        Returns:
            Server version string
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        result = self.execute_query("SELECT VERSION() AS version")
        return result[0]['version'] if result else None
        
    def show_databases(self) -> List[Dict[str, Any]]:
        """
        Get list of all databases on the server.
        
        Returns:
            List of databases
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        return self.execute_query("SHOW DATABASES")
        
    def show_table_status(self) -> List[Dict[str, Any]]:
        """
        Get detailed status information about all tables in the current database.
        
        Returns:
            List of table status information
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        return self.execute_query("SHOW TABLE STATUS")
        
    def get_table_structure(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get detailed structure information about a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of column information dictionaries
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        # Validate table name to prevent SQL injection
        if not table_name.isalnum() and not all(c in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' for c in table_name):
            raise ValueError("Invalid table name. Table names must contain only alphanumeric characters and underscores.")
            
        return self.execute_query(f"DESCRIBE {table_name}")
        
    def get_create_table(self, table_name: str) -> str:
        """
        Get the CREATE TABLE statement for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            CREATE TABLE statement as a string
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        # Validate table name to prevent SQL injection
        if not table_name.isalnum() and not all(c in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' for c in table_name):
            raise ValueError("Invalid table name. Table names must contain only alphanumeric characters and underscores.")
            
        result = self.execute_query(f"SHOW CREATE TABLE {table_name}")
        return result[0]['Create Table'] if result else None
        
    def optimize_table(self, table_name: str) -> None:
        """
        Optimize a table.
        
        Args:
            table_name: Name of the table
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        # Validate table name to prevent SQL injection
        if not table_name.isalnum() and not all(c in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' for c in table_name):
            raise ValueError("Invalid table name. Table names must contain only alphanumeric characters and underscores.")
            
        self.execute_query(f"OPTIMIZE TABLE {table_name}")
        self.logger.info(f"Optimized table: {table_name}")
        
    def analyze_table(self, table_name: str) -> None:
        """
        Analyze a table.
        
        Args:
            table_name: Name of the table
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        # Validate table name to prevent SQL injection
        if not table_name.isalnum() and not all(c in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' for c in table_name):
            raise ValueError("Invalid table name. Table names must contain only alphanumeric characters and underscores.")
            
        self.execute_query(f"ANALYZE TABLE {table_name}")
        self.logger.info(f"Analyzed table: {table_name}")
        
    def repair_table(self, table_name: str) -> None:
        """
        Repair a table.
        
        Args:
            table_name: Name of the table
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        # Validate table name to prevent SQL injection
        if not table_name.isalnum() and not all(c in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' for c in table_name):
            raise ValueError("Invalid table name. Table names must contain only alphanumeric characters and underscores.")
            
        self.execute_query(f"REPAIR TABLE {table_name}")
        self.logger.info(f"Repaired table: {table_name}")
        
    def check_table(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Check a table for errors.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of check results
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        # Validate table name to prevent SQL injection
        if not table_name.isalnum() and not all(c in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' for c in table_name):
            raise ValueError("Invalid table name. Table names must contain only alphanumeric characters and underscores.")
            
        return self.execute_query(f"CHECK TABLE {table_name}")
        
    def get_variables(self, pattern: str = None) -> List[Dict[str, Any]]:
        """
        Get MySQL system variables.
        
        Args:
            pattern: Optional pattern to filter variable names
            
        Returns:
            List of system variables
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        query = "SHOW VARIABLES"
        if pattern:
            # Use parameterized query to prevent SQL injection
            query += " LIKE :pattern"
            return self.execute_query(query, {"pattern": pattern})
        else:
            return self.execute_query(query)