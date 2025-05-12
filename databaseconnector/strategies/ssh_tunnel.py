"""
SSH tunnel database connection strategy module.

This module provides the SSHTunnelConnection class which implements a strategy
for connecting to a database server through an SSH tunnel.
"""

import logging
import sqlalchemy
import copy
from typing import Dict, Any, Optional, List, Tuple

from databaseconnector.interfaces import ConnectionStrategy, ConnectionError, QueryError, TransactionError
from databaseconnector.config import DatabaseConfig


class SSHTunnelConnection(ConnectionStrategy):
    """Connection strategy via SSH tunnel.
    
    This class implements a strategy for connecting to a database through an SSH
    tunnel, useful for accessing databases that are not directly accessible from
    the client network.
    
    Attributes:
        config (DatabaseConfig): The database configuration
        ssh_config (Dict[str, Any]): The SSH tunnel configuration
        logger (logging.Logger): Logger instance
        connection (Any): The active database connection
        engine (Any): The SQLAlchemy engine
        tunnel (Any): The SSH tunnel
        transaction (Any): The active transaction, if any
    """
    
    def __init__(self, config: DatabaseConfig, ssh_config: Dict[str, Any], logger=None):
        """
        Initialize an SSH tunnel connection strategy.
        
        Args:
            config: Database configuration object
            ssh_config: SSH configuration with keys:
                - ssh_host: SSH server hostname
                - ssh_port: SSH server port
                - ssh_username: SSH username
                - ssh_password: SSH password (optional)
                - ssh_key_file: Path to SSH private key file (optional)
                - local_port: Local port for forwarding (optional)
            logger: Optional logger instance
            
        Note:
            Either ssh_password or ssh_key_file must be provided.
        """
        self.config = config
        self.ssh_config = ssh_config
        self.logger = logger or logging.getLogger(__name__)
        self.connection = None
        self.engine = None
        self.tunnel = None
        self.transaction = None
        
        # Validate SSH configuration
        if not all(k in ssh_config for k in ['ssh_host', 'ssh_port', 'ssh_username']):
            missing_keys = [k for k in ['ssh_host', 'ssh_port', 'ssh_username'] if k not in ssh_config]
            raise ValueError(f"Missing required SSH configuration keys: {missing_keys}")
            
        if 'ssh_password' not in ssh_config and 'ssh_key_file' not in ssh_config:
            raise ValueError("Either ssh_password or ssh_key_file must be provided")
        
    def connect(self) -> Any:
        """
        Connect to database through an SSH tunnel.
        
        Returns:
            The database connection object
            
        Raises:
            ConnectionError: If connection fails
        """
        try:
            # Import here to avoid making sshtunnel a hard dependency
            from sshtunnel import SSHTunnelForwarder
            
            self.logger.info(f"Establishing SSH tunnel to {self.ssh_config['ssh_host']}:{self.ssh_config['ssh_port']}")
            
            # Establish SSH tunnel
            tunnel_kwargs = {
                'ssh_username': self.ssh_config['ssh_username'],
                'remote_bind_address': (self.config.host, self.config.port),
                'local_bind_address': ('localhost', self.ssh_config.get('local_port', 0))
            }
            
            # Add authentication method
            if 'ssh_password' in self.ssh_config:
                tunnel_kwargs['ssh_password'] = self.ssh_config['ssh_password']
            else:
                tunnel_kwargs['ssh_pkey'] = self.ssh_config['ssh_key_file']
                
            self.tunnel = SSHTunnelForwarder(
                (self.ssh_config['ssh_host'], self.ssh_config['ssh_port']),
                **tunnel_kwargs
            )
            
            self.tunnel.start()
            local_port = self.tunnel.local_bind_port
            
            self.logger.info(f"SSH tunnel established. Forwarding localhost:{local_port} to "
                           f"{self.config.host}:{self.config.port}")
            
            # Create a modified config for the local connection
            tunnel_config = copy.deepcopy(self.config)
            tunnel_config.host = 'localhost'
            tunnel_config.port = local_port
            
            connection_string = tunnel_config.get_connection_string()
            self.logger.debug(f"Connecting to database with tunnel connection string: "
                             f"{connection_string.replace(tunnel_config.password, '****')}")
            
            self.engine = sqlalchemy.create_engine(connection_string)
            self.connection = self.engine.connect()
            
            self.logger.info(f"Connected to database via SSH tunnel at {self.config.host}:{self.config.port}")
            return self.connection
            
        except ImportError as e:
            error_msg = "sshtunnel package is required for SSH tunnel connections. Install it with 'pip install sshtunnel'."
            self.logger.error(error_msg)
            raise ConnectionError(error_msg) from e
        except Exception as e:
            error_msg = f"Failed to connect via SSH tunnel: {str(e)}"
            self.logger.error(error_msg)
            
            # Clean up any partially established resources
            if hasattr(self, 'tunnel') and self.tunnel:
                try:
                    self.tunnel.close()
                except:
                    pass
                    
            raise ConnectionError(error_msg) from e
        
    def disconnect(self) -> None:
        """
        Close the database connection and SSH tunnel.
        
        Raises:
            ConnectionError: If disconnection fails
        """
        try:
            # Close the database connection
            if self.connection:
                self.connection.close()
                self.connection = None
                self.logger.info("Disconnected from database")
                
            if self.engine:
                self.engine.dispose()
                self.engine = None
                
            # Close the SSH tunnel
            if self.tunnel:
                self.tunnel.close()
                self.tunnel = None
                self.logger.info("Closed SSH tunnel")
                
        except Exception as e:
            error_msg = f"Error during disconnection: {str(e)}"
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
            self.logger.debug(f"Executing query through SSH tunnel: {query}")
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
            error_msg = f"Error executing query through SSH tunnel: {str(e)}"
            self.logger.error(error_msg)
            raise QueryError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error executing query through SSH tunnel: {str(e)}"
            self.logger.error(error_msg)
            raise QueryError(error_msg) from e
    
    def is_connected(self) -> bool:
        """
        Check if the connection and SSH tunnel are active.
        
        Returns:
            True if connected, False otherwise
        """
        if self.connection is None or self.tunnel is None or not self.tunnel.is_active:
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
            self.logger.debug("Transaction started via SSH tunnel")
            return self.transaction
        except sqlalchemy.exc.SQLAlchemyError as e:
            error_msg = f"Error beginning transaction via SSH tunnel: {str(e)}"
            self.logger.error(error_msg)
            raise TransactionError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error beginning transaction via SSH tunnel: {str(e)}"
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
            self.logger.debug("Transaction committed via SSH tunnel")
        except sqlalchemy.exc.SQLAlchemyError as e:
            error_msg = f"Error committing transaction via SSH tunnel: {str(e)}"
            self.logger.error(error_msg)
            raise TransactionError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error committing transaction via SSH tunnel: {str(e)}"
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
            self.logger.debug("Transaction rolled back via SSH tunnel")
        except sqlalchemy.exc.SQLAlchemyError as e:
            error_msg = f"Error rolling back transaction via SSH tunnel: {str(e)}"
            self.logger.error(error_msg)