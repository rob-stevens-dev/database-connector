"""
SQLite database connection module.

This module provides the SQLiteConnection class for connecting to
SQLite databases with SQLite-specific functionality.
"""

import os
import logging
import sqlalchemy
from typing import Dict, Any, Optional, List, Tuple

from databaseconnector.interfaces import ConnectionInterface, ConnectionError, QueryError, TransactionError
from databaseconnector.config import DatabaseConfig


class SQLiteConnection(ConnectionInterface):
    """SQLite database connection.
    
    This class implements the ConnectionInterface for SQLite databases.
    Unlike other database implementations, SQLiteConnection does not use
    a connection strategy because SQLite connections are always direct
    file access and don't need tunneling or remote capabilities.
    
    Attributes:
        db_path (str): Path to the SQLite database file
        logger (logging.Logger): Logger instance
        connection (Any): The active database connection
        engine (Any): The SQLAlchemy engine
        transaction (Any): The active transaction, if any
    """
    
    def __init__(self, db_path: str, logger=None):
        """
        Initialize a SQLite database connection.
        
        Args:
            db_path: Path to the SQLite database file
            logger: Optional logger instance
        """
        self.db_path = db_path
        self.logger = logger or logging.getLogger(__name__)
        self.connection = None
        self.engine = None
        self.transaction = None
        
        # Validate db_path
        if db_path != ":memory:" and not os.path.isdir(os.path.dirname(os.path.abspath(db_path))):
            raise ValueError(f"Directory for SQLite database does not exist: {os.path.dirname(os.path.abspath(db_path))}")
        
    def connect(self) -> Any:
        """
        Connect to the SQLite database.
        
        Returns:
            The database connection object
            
        Raises:
            ConnectionError: If connection fails
        """
        try:
            # Create database directory if it doesn't exist (for file-based databases)
            if self.db_path != ":memory:" and not os.path.exists(os.path.dirname(os.path.abspath(self.db_path))):
                os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
                
            connection_string = f"sqlite:///{self.db_path}"
            self.logger.debug(f"Connecting to SQLite database at: {self.db_path}")
            
            self.engine = sqlalchemy.create_engine(connection_string)
            self.connection = self.engine.connect()
            
            self.logger.info(f"Connected to SQLite database at: {self.db_path}")
            return self.connection
        except sqlalchemy.exc.SQLAlchemyError as e:
            error_msg = f"Failed to connect to SQLite database: {str(e)}"
            self.logger.error(error_msg)
            raise ConnectionError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error connecting to SQLite database: {str(e)}"
            self.logger.error(error_msg)
            raise ConnectionError(error_msg) from e
        
    def disconnect(self) -> None:
        """
        Close the database connection.
        
        Raises:
            ConnectionError: If disconnection fails
        """
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                self.logger.info("Disconnected from SQLite database")
                
            if self.engine:
                self.engine.dispose()
                self.engine = None
        except sqlalchemy.exc.SQLAlchemyError as e:
            error_msg = f"Error disconnecting from SQLite database: {str(e)}"
            self.logger.error(error_msg)
            raise ConnectionError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error disconnecting from SQLite database: {str(e)}"
            self.logger.error(error_msg)
            raise ConnectionError(error_msg) from e
        
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute a query on the database.
        
        Args:
            query: SQL query to execute
            params: Optional parameters for the query
            
        Returns:
            Query result
            
        Raises:
            ConnectionError: If not connected to database
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        try:
            self.logger.debug(f"Executing query: {query}")
            if params:
                self.logger.debug(f"With parameters: {params}")
                
            if params:
                result = self.connection.execute(sqlalchemy.text(query), params)
            else:
                result = self.connection.execute(sqlalchemy.text(query))
                
            # For SELECT queries, return the results as a list of dictionaries
            if query.strip().upper().startswith("SELECT"):
                return [dict(row) for row in result]
            return result
        except sqlalchemy.exc.SQLAlchemyError as e:
            error_msg = f"Error executing query: {str(e)}"
            self.logger.error(error_msg)
            raise QueryError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error executing query: {str(e)}"
            self.logger.error(error_msg)
            raise QueryError(error_msg) from e
        
    def is_connected(self) -> bool:
        """
        Check if the connection is active.
        
        Returns:
            True if connected, False otherwise
        """
        if self.connection is None:
            return False
            
        try:
            # Execute a simple query to check connection
            self.connection.execute(sqlalchemy.text("SELECT 1"))
            return True
        except:
            return False
        
    def begin_transaction(self) -> Any:
        """
        Begin a database transaction.
        
        Returns:
            Transaction object
            
        Raises:
            ConnectionError: If not connected to database
            TransactionError: If transaction creation fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        try:
            self.transaction = self.connection.begin()
            self.logger.debug("Transaction started")
            return self.transaction
        except sqlalchemy.exc.SQLAlchemyError as e:
            error_msg = f"Error beginning transaction: {str(e)}"
            self.logger.error(error_msg)
            raise TransactionError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error beginning transaction: {str(e)}"
            self.logger.error(error_msg)
            raise TransactionError(error_msg) from e
        
    def commit(self) -> None:
        """
        Commit the current transaction.
        
        Raises:
            TransactionError: If no active transaction or commit fails
        """
        if not self.transaction:
            raise TransactionError("No active transaction to commit")
            
        try:
            self.transaction.commit()
            self.transaction = None
            self.logger.debug("Transaction committed")
        except sqlalchemy.exc.SQLAlchemyError as e:
            error_msg = f"Error committing transaction: {str(e)}"
            self.logger.error(error_msg)
            raise TransactionError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error committing transaction: {str(e)}"
            self.logger.error(error_msg)
            raise TransactionError(error_msg) from e
        
    def rollback(self) -> None:
        """
        Rollback the current transaction.
        
        Raises:
            TransactionError: If no active transaction or rollback fails
        """
        if not self.transaction:
            raise TransactionError("No active transaction to rollback")
            
        try:
            self.transaction.rollback()
            self.transaction = None
            self.logger.debug("Transaction rolled back")
        except sqlalchemy.exc.SQLAlchemyError as e:
            error_msg = f"Error rolling back transaction: {str(e)}"
            self.logger.error(error_msg)
            raise TransactionError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error rolling back transaction: {str(e)}"
            self.logger.error(error_msg)
            raise TransactionError(error_msg) from e
            
    def pragma(self, name: str, value: Any = None) -> Any:
        """
        Execute a PRAGMA statement.
        
        Args:
            name: Name of the PRAGMA
            value: Optional value to set
            
        Returns:
            PRAGMA results if value is None, else None
            
        Raises:
            ConnectionError: If not connected
            QueryError: If PRAGMA execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        # Validate PRAGMA name to prevent SQL injection
        if not all(c in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' for c in name):
            raise ValueError("Invalid PRAGMA name. PRAGMA names must contain only alphanumeric characters and underscores.")
            
        try:
            if value is not None:
                # Set PRAGMA value
                self.execute_query(f"PRAGMA {name} = {value}")
                return None
            else:
                # Get PRAGMA value
                return self.execute_query(f"PRAGMA {name}")
        except QueryError:
            raise
        except Exception as e:
            error_msg = f"Error executing PRAGMA: {str(e)}"
            self.logger.error(error_msg)
            raise QueryError(error_msg) from e
            
    def get_table_info(self, table: str) -> List[Dict[str, Any]]:
        """
        Get information about a table's columns.
        
        Args:
            table: Name of the table
            
        Returns:
            List of column information dictionaries
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        try:
            return self.execute_query(f"PRAGMA table_info({table})")
        except QueryError:
            raise
        except Exception as e:
            error_msg = f"Error getting table info: {str(e)}"
            self.logger.error(error_msg)
            raise QueryError(error_msg) from e
            
    def vacuum(self) -> None:
        """
        Rebuild the database file, repacking it to minimize disk space.
        
        Raises:
            ConnectionError: If not connected
            QueryError: If VACUUM execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        try:
            self.execute_query("VACUUM")
            self.logger.info("Database vacuumed")
        except QueryError:
            raise
        except Exception as e:
            error_msg = f"Error vacuuming database: {str(e)}"
            self.logger.error(error_msg)
            raise QueryError(error_msg) from e
            
    def get_sqlite_version(self) -> str:
        """
        Get the SQLite version.
        
        Returns:
            SQLite version string
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        try:
            result = self.execute_query("SELECT sqlite_version() AS version")
            return result[0]['version'] if result else None
        except QueryError:
            raise
        except Exception as e:
            error_msg = f"Error getting SQLite version: {str(e)}"
            self.logger.error(error_msg)
            raise QueryError(error_msg) from e
            
    def get_all_tables(self) -> List[Dict[str, Any]]:
        """
        Get list of all tables in the database.
        
        Returns:
            List of table information dictionaries
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        try:
            return self.execute_query("""
                SELECT 
                    name AS table_name,
                    type AS table_type,
                    sql
                FROM sqlite_master
                WHERE type='table'
                AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
        except QueryError:
            raise
        except Exception as e:
            error_msg = f"Error getting tables: {str(e)}"
            self.logger.error(error_msg)
            raise QueryError(error_msg) from e