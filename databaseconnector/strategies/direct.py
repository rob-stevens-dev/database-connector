"""
Direct database connection strategy module.

This module provides the DirectConnection class which implements a strategy
for connecting directly to a database server.
"""

import logging
import sqlalchemy
from typing import Dict, Any, Optional, List, Tuple

from databaseconnector.interfaces import ConnectionStrategy, ConnectionError, QueryError, TransactionError
from databaseconnector.config import DatabaseConfig


class DirectConnection(ConnectionStrategy):
    """Connection strategy for directly accessible database servers.
    
    This class implements a strategy for connecting directly to a database
    server without any intermediaries (like SSH tunnels). It supports both
    local and remote connections with appropriate timeout settings.
    
    Attributes:
        config (DatabaseConfig): The database configuration
        logger (logging.Logger): Logger instance
        is_remote (bool): Whether this is a remote connection
        connection (Any): The active database connection
        engine (Any): The SQLAlchemy engine
        transaction (Any): The active transaction, if any
    """
    
    def __init__(self, config: DatabaseConfig, logger=None, is_remote: bool = False):
        """
        Initialize a direct connection strategy.
        
        Args:
            config: Database configuration object
            logger: Optional logger instance
            is_remote: Whether this is a remote connection (affects timeout settings)
        """
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.is_remote = is_remote
        self.connection = None
        self.engine = None
        self.transaction = None
        
    def connect(self) -> Any:
        """
        Connect to the database directly.
        
        Returns:
            The database connection object
            
        Raises:
            ConnectionError: If connection fails
        """
        try:
            connection_string = self.config.get_connection_string()
            
            # Configure connection parameters based on whether it's remote
            connect_args = self.config.get_connection_args().copy()
            if self.is_remote and 'connect_timeout' not in connect_args:
                connect_args['connect_timeout'] = 30
                
            self.logger.debug(f"Connecting to database with connection string: "
                             f"{connection_string.replace(self.config.password, '****')}")
            
            self.engine = sqlalchemy.create_engine(connection_string, connect_args=connect_args)
            self.connection = self.engine.connect()
            
            connection_type = "remote" if self.is_remote else "local"
            self.logger.info(f"Connected to {connection_type} database at "
                            f"{self.config.host}:{self.config.port}")
            return self.connection
        except Exception as e:
            error_msg = f"Failed to connect to database: {str(e)}"
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
                self.logger.info("Disconnected from database")
                
            if self.engine:
                self.engine.dispose()
                self.engine = None
        except Exception as e:
            error_msg = f"Error disconnecting from database: {str(e)}"
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
                # Convert rows to dictionaries
                return [dict(row._mapping) for row in result]
            return result
        except Exception as e:
            error_msg = f"Error executing query: {str(e)}"
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
        except Exception as e:
            error_msg = f"Error beginning transaction: {str(e)}"
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
        except Exception as e:
            error_msg = f"Error committing transaction: {str(e)}"
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
        except Exception as e:
            error_msg = f"Error rolling back transaction: {str(e)}"
            self.logger.error(error_msg)
            raise TransactionError(error_msg) from e