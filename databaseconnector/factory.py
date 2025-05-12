"""
Database connection factory module.

This module provides the ConnectionFactory class for creating database
connections of different types with different connection strategies.
"""

import logging
from typing import Dict, Any, Optional, List, Tuple

from databaseconnector.config import DatabaseConfig
from databaseconnector.interfaces import ConnectionInterface
from databaseconnector.strategies import DirectConnection, SSHTunnelConnection
from databaseconnector.connections import (
    PostgreSQLConnection,
    MySQLConnection,
    OracleConnection,
    MSSQLConnection,
    SQLiteConnection
)


class ConnectionFactory:
    """Factory for creating database connections.
    
    This class is responsible for creating database connections of different
    types with appropriate connection strategies. It encapsulates the complexity
    of instantiating the appropriate objects based on configuration parameters.
    """
    
    @staticmethod
    def create_connection(db_type: str, connection_type: str, db_config: DatabaseConfig, 
                        ssh_config: Optional[Dict[str, Any]] = None, 
                        logger=None) -> ConnectionInterface:
        """
        Create a database connection of the specified type.
        
        Args:
            db_type: Database type ("postgres", "mysql", "oracle", "mssql", "sqlite")
            connection_type: Connection type ("direct", "remote", "ssh_tunnel")
            db_config: Database configuration
            ssh_config: SSH configuration (required for ssh_tunnel)
            logger: Optional logger
            
        Returns:
            An instance of ConnectionInterface
            
        Raises:
            ValueError: If unsupported database or connection type
        """
        if logger is None:
            logger = logging.getLogger(__name__)
            
        logger.debug(f"Creating {db_type} connection with {connection_type} strategy")
            
        if db_type == "postgres":
            return ConnectionFactory.create_postgres_connection(connection_type, db_config, ssh_config, logger)
        elif db_type == "mysql":
            return ConnectionFactory.create_mysql_connection(connection_type, db_config, ssh_config, logger)
        elif db_type == "oracle":
            return ConnectionFactory.create_oracle_connection(connection_type, db_config, ssh_config, logger)
        elif db_type == "mssql":
            return ConnectionFactory.create_mssql_connection(connection_type, db_config, ssh_config, logger)
        elif db_type == "sqlite":
            if connection_type not in ["direct", "local"]:
                raise ValueError("SQLite only supports direct/local connections")
            return ConnectionFactory.create_sqlite_connection(db_config.database, logger)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    @staticmethod
    def _create_connection_strategy(connection_type: str, db_config: DatabaseConfig, 
                                  ssh_config: Optional[Dict[str, Any]] = None, 
                                  logger=None) -> DirectConnection | SSHTunnelConnection:
        """
        Create a connection strategy based on the connection type.
        
        Args:
            connection_type: Type of connection ("direct", "remote", "ssh_tunnel")
            db_config: Database configuration
            ssh_config: SSH configuration (required for ssh_tunnel)
            logger: Optional logger
            
        Returns:
            An instance of ConnectionStrategy
            
        Raises:
            ValueError: If unsupported connection type or missing SSH config
        """
        if connection_type == "direct" or connection_type == "local":
            return DirectConnection(db_config, logger, is_remote=False)
        elif connection_type == "remote":
            return DirectConnection(db_config, logger, is_remote=True)
        elif connection_type == "ssh_tunnel":
            if not ssh_config:
                raise ValueError("SSH configuration is required for SSH tunnel connections")
            return SSHTunnelConnection(db_config, ssh_config, logger)
        else:
            raise ValueError(f"Unsupported connection type: {connection_type}")
            
    @staticmethod
    def create_postgres_connection(connection_type: str, db_config: DatabaseConfig, 
                                 ssh_config: Optional[Dict[str, Any]] = None, 
                                 logger=None) -> PostgreSQLConnection:
        """
        Create a PostgreSQL connection.
        
        Args:
            connection_type: Connection type
            db_config: Database configuration
            ssh_config: SSH configuration (for ssh_tunnel)
            logger: Optional logger
            
        Returns:
            PostgreSQL connection instance
        """
        # Ensure config has the proper driver
        if 'driver' not in db_config.connection_params:
            db_config.connection_params['driver'] = 'postgresql'
        
        strategy = ConnectionFactory._create_connection_strategy(
            connection_type, db_config, ssh_config, logger
        )
        
        # Create the PostgreSQL connection
        return PostgreSQLConnection(db_config, logger, strategy)
        
    @staticmethod
    def create_mysql_connection(connection_type: str, db_config: DatabaseConfig, 
                              ssh_config: Optional[Dict[str, Any]] = None, 
                              logger=None) -> MySQLConnection:
        """
        Create a MySQL connection.
        
        Args:
            connection_type: Connection type
            db_config: Database configuration
            ssh_config: SSH configuration (for ssh_tunnel)
            logger: Optional logger
            
        Returns:
            MySQL connection instance
        """
        # Ensure config has the proper driver
        if 'driver' not in db_config.connection_params:
            db_config.connection_params['driver'] = 'mysql+pymysql'
        
        strategy = ConnectionFactory._create_connection_strategy(
            connection_type, db_config, ssh_config, logger
        )
        
        # Create the MySQL connection
        return MySQLConnection(db_config, logger, strategy)
        
    @staticmethod
    def create_oracle_connection(connection_type: str, db_config: DatabaseConfig, 
                               ssh_config: Optional[Dict[str, Any]] = None, 
                               logger=None) -> OracleConnection:
        """
        Create an Oracle connection.
        
        Args:
            connection_type: Connection type
            db_config: Database configuration
            ssh_config: SSH configuration (for ssh_tunnel)
            logger: Optional logger
            
        Returns:
            Oracle connection instance
        """
        # Ensure config has the proper driver
        if 'driver' not in db_config.connection_params:
            db_config.connection_params['driver'] = 'oracle+cx_oracle'
        
        strategy = ConnectionFactory._create_connection_strategy(
            connection_type, db_config, ssh_config, logger
        )
        
        # Create the Oracle connection
        return OracleConnection(db_config, logger, strategy)
        
    @staticmethod
    def create_mssql_connection(connection_type: str, db_config: DatabaseConfig, 
                              ssh_config: Optional[Dict[str, Any]] = None, 
                              logger=None) -> MSSQLConnection:
        """
        Create a Microsoft SQL Server connection.
        
        Args:
            connection_type: Connection type
            db_config: Database configuration
            ssh_config: SSH configuration (for ssh_tunnel)
            logger: Optional logger
            
        Returns:
            Microsoft SQL Server connection instance
        """
        # Ensure config has the proper driver
        if 'driver' not in db_config.connection_params:
            db_config.connection_params['driver'] = 'mssql+pyodbc'
        
        strategy = ConnectionFactory._create_connection_strategy(
            connection_type, db_config, ssh_config, logger
        )
        
        # Create the MSSQL connection
        return MSSQLConnection(db_config, logger, strategy)
        
    @staticmethod
    def create_sqlite_connection(db_path: str, logger=None) -> SQLiteConnection:
        """
        Create a SQLite connection.
        
        Args:
            db_path: Path to the SQLite database file
            logger: Optional logger
            
        Returns:
            SQLite connection instance
        """
        # Create the SQLite connection
        return SQLiteConnection(db_path, logger)