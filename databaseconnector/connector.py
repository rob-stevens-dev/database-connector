"""
Database connector module.

This module provides the DatabaseConnector class that serves as a high-level
interface for working with database connections.
"""

import logging
from typing import Dict, Any, Optional, List, Tuple, Callable

from databaseconnector.interfaces import ConnectionInterface, ConnectionError, QueryError, TransactionError


class DatabaseConnector:
    """High-level database connector.
    
    This class provides a simplified interface for working with database
    connections, including transaction management and query execution.
    
    Attributes:
        connection (ConnectionInterface): The database connection
        logger (logging.Logger): Logger instance
    """
    
    def __init__(self, connection: ConnectionInterface, logger=None):
        """
        Initialize a database connector.
        
        Args:
            connection: Database connection interface
            logger: Optional logger instance
        """
        self.connection = connection
        self.logger = logger or logging.getLogger(__name__)
        
    def connect(self) -> Any:
        """
        Connect to the database.
        
        Returns:
            Connection object
            
        Raises:
            ConnectionError: If connection fails
        """
        self.logger.debug("Connecting to database using connector")
        return self.connection.connect()
        
    def close(self) -> None:
        """
        Close the database connection.
        
        Raises:
            ConnectionError: If disconnection fails
        """
        self.logger.debug("Closing database connection")
        return self.connection.disconnect()
        
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute a query on the database.
        
        Args:
            query: SQL query string
            params: Optional query parameters
            
        Returns:
            Query results
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        self.logger.debug(f"Executing query: {query}")
        return self.connection.execute_query(query, params)
        
    def execute_transaction(self, queries: List[Tuple[str, Optional[Dict[str, Any]]]]) -> List[Any]:
        """
        Execute multiple queries in a transaction.
        
        Args:
            queries: List of (query, params) tuples
            
        Returns:
            List of query results
            
        Raises:
            ConnectionError: If not connected
            TransactionError: If transaction fails
        """
        if not self.connection.is_connected():
            raise ConnectionError("Not connected to database")
            
        self.logger.debug(f"Executing transaction with {len(queries)} queries")
        
        try:
            transaction = self.connection.begin_transaction()
            results = []
            
            for query, params in queries:
                self.logger.debug(f"Transaction step: {query}")
                result = self.connection.execute_query(query, params)
                results.append(result)
                
            self.connection.commit()
            self.logger.debug("Transaction committed successfully")
            return results
        except (ConnectionError, QueryError) as e:
            self.logger.error(f"Transaction failed: {str(e)}")
            try:
                self.connection.rollback()
                self.logger.debug("Transaction rolled back")
            except TransactionError as rollback_error:
                self.logger.error(f"Error rolling back transaction: {str(rollback_error)}")
            raise TransactionError(f"Transaction failed: {str(e)}") from e
        except Exception as e:
            self.logger.error(f"Unexpected error in transaction: {str(e)}")
            try:
                self.connection.rollback()
                self.logger.debug("Transaction rolled back")
            except TransactionError as rollback_error:
                self.logger.error(f"Error rolling back transaction: {str(rollback_error)}")
            raise TransactionError(f"Unexpected error in transaction: {str(e)}") from e
            
    def with_transaction(self, callable_func: Callable[[ConnectionInterface], Any]) -> Any:
        """
        Execute a callable within a transaction context.
        
        Args:
            callable_func: Function that takes a connection and returns a result
            
        Returns:
            Result of the callable
            
        Raises:
            ConnectionError: If not connected
            TransactionError: If transaction fails
        """
        if not self.connection.is_connected():
            raise ConnectionError("Not connected to database")
            
        self.logger.debug("Executing callable within transaction context")
        
        try:
            transaction = self.connection.begin_transaction()
            result = callable_func(self.connection)
            self.connection.commit()
            self.logger.debug("Transaction committed successfully")
            return result
        except (ConnectionError, QueryError) as e:
            self.logger.error(f"Transaction failed: {str(e)}")
            try:
                self.connection.rollback()
                self.logger.debug("Transaction rolled back")
            except TransactionError as rollback_error:
                self.logger.error(f"Error rolling back transaction: {str(rollback_error)}")
            raise TransactionError(f"Transaction failed: {str(e)}") from e
        except Exception as e:
            self.logger.error(f"Unexpected error in transaction: {str(e)}")
            try:
                self.connection.rollback()
                self.logger.debug("Transaction rolled back")
            except TransactionError as rollback_error:
                self.logger.error(f"Error rolling back transaction: {str(rollback_error)}")
            raise TransactionError(f"Unexpected error in transaction: {str(e)}") from e
            
    def is_connected(self) -> bool:
        """
        Check if the connection is active.
        
        Returns:
            True if connected, False otherwise
        """
        return self.connection.is_connected()
        
    def __enter__(self):
        """
        Context manager entry point - connects to the database.
        
        Returns:
            Self (DatabaseConnector instance)
            
        Raises:
            ConnectionError: If connection fails
        """
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit point - closes the database connection.
        
        Args:
            exc_type: Exception type (if any)
            exc_val: Exception value (if any)
            exc_tb: Exception traceback (if any)
            
        Returns:
            False to propagate exceptions
        """
        try:
            # If there was an exception and we're connected, try to rollback
            if exc_type is not None and self.is_connected():
                try:
                    self.connection.rollback()
                    self.logger.debug("Transaction rolled back due to exception")
                except (TransactionError, ConnectionError):
                    # Ignore errors during rollback in __exit__
                    pass
                    
            # Always try to close the connection
            self.close()
        except ConnectionError:
            # Ignore connection errors during __exit__
            pass
            
        # Propagate any exceptions
        return False