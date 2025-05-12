"""
Database configuration module.

This module provides the DatabaseConfig class for managing connection information
for different database types.
"""

from typing import Dict, Any, Optional


class DatabaseConfig:
    """Configuration class for database connections.
    
    This class is responsible for storing and providing database connection parameters.
    It can generate connection strings in a format appropriate for SQLAlchemy and
    return additional connection arguments for specific database adaptors.
    
    Attributes:
        host (str): The database server hostname or IP address
        port (int): The database server port
        username (str): The username for authentication
        password (str): The password for authentication
        database (str): The database name
        connection_params (Dict[str, Any]): Additional connection parameters
    """
    
    def __init__(self, host: str, port: int, username: str, password: str, 
                 database: str, **kwargs):
        """
        Initialize a new database configuration.
        
        Args:
            host: The database server hostname or IP address
            port: The database server port
            username: The username for authentication
            password: The password for authentication
            database: The database name
            **kwargs: Additional connection parameters
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.connection_params = kwargs
        
    def get_connection_string(self) -> str:
        """
        Generate a connection string for the database.
        
        Returns:
            A connection string in the format appropriate for SQLAlchemy
        """
        # Default to postgresql if no driver is specified
        driver = self.connection_params.get('driver', 'postgresql')
        
        # Special case for SQLite which doesn't use host/port/username/password
        if driver == 'sqlite':
            return f"{driver}:///{self.database}"
        
        # Create the connection string using SQLAlchemy format
        # Note: URL encoding might be needed for special characters in passwords
        return f"{driver}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        
    def get_connection_args(self) -> Dict[str, Any]:
        """
        Get additional connection arguments.
        
        Returns:
            A dictionary of connection arguments
        """
        # Return a copy to prevent accidental modification of the original
        return self.connection_params.copy()
    
    def __str__(self) -> str:
        """Return a string representation of the configuration, hiding the password."""
        return (f"DatabaseConfig(host='{self.host}', port={self.port}, "
                f"username='{self.username}', database='{self.database}')")
    
    def __repr__(self) -> str:
        """Return a string representation of the configuration for debugging."""
        return self.__str__()