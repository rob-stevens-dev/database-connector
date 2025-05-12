"""
Database connection interfaces module.

This module defines the abstract base classes and interfaces for the database
connection system, including:
- ConnectionInterface: For all database connections
- ConnectionStrategy: For different connection strategies
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Tuple


class DatabaseError(Exception):
    """Base class for all database-related exceptions."""
    pass


class ConnectionError(DatabaseError):
    """Exception raised for connection-related errors."""
    pass


class QueryError(DatabaseError):
    """Exception raised for query execution errors."""
    pass


class TransactionError(DatabaseError):
    """Exception raised for transaction-related errors."""
    pass


class ConnectionInterface(ABC):
    """Abstract base class for all database connections.
    
    This interface defines the core methods that all database connection
    implementations must provide, regardless of the specific database type
    or connection strategy used.
    """
    
    @abstractmethod
    def connect(self) -> Any:
        """Establish a connection to the database.
        
        Returns:
            The database connection object
            
        Raises:
            ConnectionError: If the connection attempt fails
        """
        pass
        
    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection.
        
        Raises:
            ConnectionError: If the disconnection attempt fails
        """
        pass
        
    @abstractmethod
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a query on the database.
        
        Args:
            query: SQL query string
            params: Optional parameters for the query
            
        Returns:
            Query results
            
        Raises:
            ConnectionError: If the connection is not active
            QueryError: If the query execution fails
        """
        pass
        
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if the connection is active.
        
        Returns:
            True if connected, False otherwise
        """
        pass
        
    @abstractmethod
    def begin_transaction(self) -> Any:
        """Begin a database transaction.
        
        Returns:
            Transaction object
            
        Raises:
            ConnectionError: If the connection is not active
            TransactionError: If the transaction cannot be started
        """
        pass
        
    @abstractmethod
    def commit(self) -> None:
        """Commit the current transaction.
        
        Raises:
            ConnectionError: If the connection is not active
            TransactionError: If no active transaction or commit fails
        """
        pass
        
    @abstractmethod
    def rollback(self) -> None:
        """Rollback the current transaction.
        
        Raises:
            ConnectionError: If the connection is not active
            TransactionError: If no active transaction or rollback fails
        """
        pass


class ConnectionStrategy(ABC):
    """Interface for database connection strategies.
    
    This interface defines the methods that all connection strategy
    implementations must provide. Strategies encapsulate different ways
    of connecting to a database (direct, SSH tunnel, etc.).
    """
    
    @abstractmethod
    def connect(self) -> Any:
        """Establish a connection to the database.
        
        Returns:
            The database connection object
            
        Raises:
            ConnectionError: If the connection attempt fails
        """
        pass
        
    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection.
        
        Raises:
            ConnectionError: If the disconnection attempt fails
        """
        pass
        
    @abstractmethod
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a query on the database.
        
        Args:
            query: SQL query string
            params: Optional parameters for the query
            
        Returns:
            Query results
            
        Raises:
            ConnectionError: If the connection is not active
            QueryError: If the query execution fails
        """
        pass
        
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if the connection is active.
        
        Returns:
            True if connected, False otherwise
        """
        pass
        
    @abstractmethod
    def begin_transaction(self) -> Any:
        """Begin a database transaction.
        
        Returns:
            Transaction object
            
        Raises:
            ConnectionError: If the connection is not active
            TransactionError: If the transaction cannot be started
        """
        pass
        
    @abstractmethod
    def commit(self) -> None:
        """Commit the current transaction.
        
        Raises:
            ConnectionError: If the connection is not active
            TransactionError: If no active transaction or commit fails
        """
        pass
        
    @abstractmethod
    def rollback(self) -> None:
        """Rollback the current transaction.
        
        Raises:
            ConnectionError: If the connection is not active
            TransactionError: If no active transaction or rollback fails
        """
        pass