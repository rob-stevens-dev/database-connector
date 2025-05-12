"""
Base database connection module.

This module provides the abstract DatabaseConnection class that serves as a
template for database-specific connection implementations.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Tuple

from databaseconnector.interfaces import ConnectionInterface, ConnectionStrategy
from databaseconnector.config import DatabaseConfig


class DatabaseConnection(ConnectionInterface):
    """Base class for database connections using a strategy.
    
    This abstract class implements the ConnectionInterface using the
    Strategy pattern. It delegates connection operations to a ConnectionStrategy
    object while allowing concrete subclasses to add database-specific
    functionality.
    
    Attributes:
        config (DatabaseConfig): The database configuration
        logger (logging.Logger): Logger instance
        connection_strategy (ConnectionStrategy): Strategy for handling the connection
    """
    
    def __init__(self, config: DatabaseConfig, logger=None, connection_strategy: ConnectionStrategy = None):
        """
        Initialize a database connection with a strategy.
        
        Args:
            config: Database configuration
            logger: Optional logger instance
            connection_strategy: Strategy for handling the connection
        """
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.connection_strategy = connection_strategy
        
        if connection_strategy is None:
            raise ValueError("A connection strategy must be provided")
        
    def connect(self) -> Any:
        """
        Connect using the strategy.
        
        Returns:
            Connection object from the strategy
            
        Raises:
            ConnectionError: If connection fails
        """
        self.logger.debug(f"Connecting to database using {self.connection_strategy.__class__.__name__}")
        return self.connection_strategy.connect()
        
    def disconnect(self) -> None:
        """
        Disconnect using the strategy.
        
        Raises:
            ConnectionError: If disconnection fails
        """
        self.logger.debug(f"Disconnecting from database using {self.connection_strategy.__class__.__name__}")
        return self.connection_strategy.disconnect()
        
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute query using the strategy.
        
        Args:
            query: SQL query string
            params: Optional query parameters
            
        Returns:
            Query results
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        return self.connection_strategy.execute_query(query, params)
        
    def is_connected(self) -> bool:
        """
        Check connection status using the strategy.
        
        Returns:
            True if connected, False otherwise
        """
        return self.connection_strategy.is_connected()
        
    def begin_transaction(self) -> Any:
        """
        Begin transaction using the strategy.
        
        Returns:
            Transaction object
            
        Raises:
            ConnectionError: If not connected
            TransactionError: If transaction creation fails
        """
        self.logger.debug("Beginning transaction")
        return self.connection_strategy.begin_transaction()
        
    def commit(self) -> None:
        """
        Commit using the strategy.
        
        Raises:
            TransactionError: If no active transaction or commit fails
        """
        self.logger.debug("Committing transaction")
        return self.connection_strategy.commit()
        
    def rollback(self) -> None:
        """
        Rollback using the strategy.
        
        Raises:
            TransactionError: If no active transaction or rollback fails
        """
        self.logger.debug("Rolling back transaction")
        return self.connection_strategy.rollback()