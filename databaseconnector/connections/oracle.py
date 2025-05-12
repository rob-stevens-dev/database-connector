"""
Oracle database connection module.

This module provides the OracleConnection class for connecting to
Oracle databases with Oracle-specific functionality.
"""

import logging
from typing import Dict, Any, Optional, List, Tuple

from databaseconnector.connections.base import DatabaseConnection
from databaseconnector.config import DatabaseConfig
from databaseconnector.interfaces import ConnectionStrategy, ConnectionError, QueryError


class OracleConnection(DatabaseConnection):
    """Oracle-specific database connection.
    
    This class extends the base DatabaseConnection with Oracle-specific
    functionality such as executing procedures, getting privileges, etc.
    
    Attributes:
        config (DatabaseConfig): The database configuration
        logger (logging.Logger): Logger instance
        connection_strategy (ConnectionStrategy): Strategy for handling the connection
    """
    
    def __init__(self, config: DatabaseConfig, logger=None, connection_strategy: ConnectionStrategy = None):
        """
        Initialize an Oracle database connection.
        
        Args:
            config: Database configuration
            logger: Optional logger instance
            connection_strategy: Strategy for handling the connection
        """
        super().__init__(config, logger, connection_strategy)
        
        # Ensure the driver parameter is set
        if 'driver' not in config.connection_params:
            config.connection_params['driver'] = 'oracle+cx_oracle'
            
    def execute_procedure(self, name: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute a stored procedure.
        
        Args:
            name: Name of the procedure
            params: Parameters for the procedure
            
        Returns:
            Procedure execution result
            
        Raises:
            ConnectionError: If not connected
            QueryError: If procedure execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        # Validate procedure name to prevent SQL injection
        if not all(c in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.' for c in name):
            raise ValueError("Invalid procedure name. Procedure names must contain only alphanumeric characters, underscores, and dots.")
            
        # Build the procedure call
        if params:
            param_names = list(params.keys())
            placeholders = ", ".join([f":{param}" for param in param_names])
            query = f"BEGIN {name}({placeholders}); END;"
            return self.execute_query(query, params)
        else:
            query = f"BEGIN {name}; END;"
            return self.execute_query(query)
        
    def get_table_privileges(self, table_name: str = None) -> List[Dict[str, Any]]:
        """
        Get table privileges.
        
        Args:
            table_name: Optional table name to filter results
            
        Returns:
            List of table privileges
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        query = """
            SELECT 
                GRANTEE, 
                OWNER, 
                TABLE_NAME, 
                GRANTOR, 
                PRIVILEGE, 
                GRANTABLE, 
                HIERARCHY 
            FROM ALL_TAB_PRIVS
        """
        
        if table_name:
            query += " WHERE TABLE_NAME = :table_name"
            return self.execute_query(query, {"table_name": table_name.upper()})
        else:
            return self.execute_query(query)
    
    def get_session_info(self) -> Dict[str, Any]:
        """
        Get information about the current session.
        
        Returns:
            Dictionary with session information
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        queries = [
            ("SELECT SYS_CONTEXT('USERENV', 'SESSION_USER') AS username FROM DUAL", "username"),
            ("SELECT SYS_CONTEXT('USERENV', 'INSTANCE_NAME') AS instance FROM DUAL", "instance"),
            ("SELECT SYS_CONTEXT('USERENV', 'HOST') AS host FROM DUAL", "host"),
            ("SELECT SYS_CONTEXT('USERENV', 'IP_ADDRESS') AS ip_address FROM DUAL", "ip_address"),
            ("SELECT SYS_CONTEXT('USERENV', 'OS_USER') AS os_user FROM DUAL", "os_user"),
            ("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AS current_schema FROM DUAL", "current_schema"),
            ("SELECT TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') AS current_time FROM DUAL", "current_time")
        ]
        
        session_info = {}
        for query, key in queries:
            try:
                result = self.execute_query(query)
                if result and len(result) > 0:
                    session_info[key] = result[0][key.lower()] if key.lower() in result[0] else None
            except Exception as e:
                self.logger.warning(f"Error getting session info ({key}): {str(e)}")
                session_info[key] = None
                
        return session_info
        
    def get_tablespaces(self) -> List[Dict[str, Any]]:
        """
        Get information about tablespaces.
        
        Returns:
            List of tablespace information dictionaries
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        query = """
            SELECT 
                TABLESPACE_NAME,
                STATUS,
                CONTENTS,
                LOGGING,
                EXTENT_MANAGEMENT,
                ALLOCATION_TYPE,
                SEGMENT_SPACE_MANAGEMENT,
                BIGFILE
            FROM DBA_TABLESPACES
            ORDER BY TABLESPACE_NAME
        """
        
        try:
            return self.execute_query(query)
        except QueryError:
            # If user doesn't have DBA privileges, try with USER_TABLESPACES
            self.logger.warning("Error querying DBA_TABLESPACES, falling back to USER_TABLESPACES")
            query = """
                SELECT 
                    TABLESPACE_NAME,
                    STATUS,
                    CONTENTS,
                    LOGGING,
                    EXTENT_MANAGEMENT,
                    ALLOCATION_TYPE,
                    SEGMENT_SPACE_MANAGEMENT
                FROM USER_TABLESPACES
                ORDER BY TABLESPACE_NAME
            """
            return self.execute_query(query)
            
    def get_all_tables(self, owner: str = None) -> List[Dict[str, Any]]:
        """
        Get list of tables.
        
        Args:
            owner: Optional owner (schema) name to filter results
            
        Returns:
            List of tables
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        if owner:
            query = """
                SELECT 
                    OWNER,
                    TABLE_NAME,
                    TABLESPACE_NAME,
                    STATUS,
                    NUM_ROWS,
                    BLOCKS,
                    LAST_ANALYZED
                FROM ALL_TABLES
                WHERE OWNER = :owner
                ORDER BY OWNER, TABLE_NAME
            """
            return self.execute_query(query, {"owner": owner.upper()})
        else:
            query = """
                SELECT 
                    OWNER,
                    TABLE_NAME,
                    TABLESPACE_NAME,
                    STATUS,
                    NUM_ROWS,
                    BLOCKS,
                    LAST_ANALYZED
                FROM ALL_TABLES
                ORDER BY OWNER, TABLE_NAME
            """
            return self.execute_query(query)
            
    def get_table_columns(self, table_name: str, owner: str = None) -> List[Dict[str, Any]]:
        """
        Get columns for a specific table.
        
        Args:
            table_name: Name of the table
            owner: Optional owner (schema) name
            
        Returns:
            List of column information dictionaries
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        params = {"table_name": table_name.upper()}
        
        if owner:
            query = """
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    DATA_LENGTH,
                    DATA_PRECISION,
                    DATA_SCALE,
                    NULLABLE,
                    COLUMN_ID
                FROM ALL_TAB_COLUMNS
                WHERE TABLE_NAME = :table_name
                AND OWNER = :owner
                ORDER BY COLUMN_ID
            """
            params["owner"] = owner.upper()
        else:
            query = """
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    DATA_LENGTH,
                    DATA_PRECISION,
                    DATA_SCALE,
                    NULLABLE,
                    COLUMN_ID
                FROM USER_TAB_COLUMNS
                WHERE TABLE_NAME = :table_name
                ORDER BY COLUMN_ID
            """
            
        return self.execute_query(query, params)