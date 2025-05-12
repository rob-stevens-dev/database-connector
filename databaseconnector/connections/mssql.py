"""
Microsoft SQL Server database connection module.

This module provides the MSSQLConnection class for connecting to
Microsoft SQL Server databases with SQL Server-specific functionality.
"""

import logging
from typing import Dict, Any, Optional, List, Tuple

from databaseconnector.connections.base import DatabaseConnection
from databaseconnector.config import DatabaseConfig
from databaseconnector.interfaces import ConnectionStrategy, ConnectionError, QueryError


class MSSQLConnection(DatabaseConnection):
    """SQL Server-specific database connection.
    
    This class extends the base DatabaseConnection with SQL Server-specific
    functionality such as executing stored procedures, managing linked servers, etc.
    
    Attributes:
        config (DatabaseConfig): The database configuration
        logger (logging.Logger): Logger instance
        connection_strategy (ConnectionStrategy): Strategy for handling the connection
    """
    
    def __init__(self, config: DatabaseConfig, logger=None, connection_strategy: ConnectionStrategy = None):
        """
        Initialize a SQL Server database connection.
        
        Args:
            config: Database configuration
            logger: Optional logger instance
            connection_strategy: Strategy for handling the connection
        """
        super().__init__(config, logger, connection_strategy)
        
        # Ensure the driver parameter is set
        if 'driver' not in config.connection_params:
            config.connection_params['driver'] = 'mssql+pyodbc'
            
    def execute_stored_procedure(self, name: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute a stored procedure.
        
        Args:
            name: Name of the stored procedure
            params: Parameters for the stored procedure
            
        Returns:
            Stored procedure execution result
            
        Raises:
            ConnectionError: If not connected
            QueryError: If stored procedure execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        # Validate procedure name to prevent SQL injection
        if not all(c in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.[]' for c in name):
            raise ValueError("Invalid procedure name. Procedure names must contain only alphanumeric characters, underscores, dots, and square brackets.")
            
        # Build the procedure call
        if params:
            param_list = []
            param_values = {}
            
            for key, value in params.items():
                param_name = f"@{key}"
                param_list.append(f"{param_name} = :{key}")
                param_values[key] = value
                
            param_string = ", ".join(param_list)
            query = f"EXEC {name} {param_string}"
            return self.execute_query(query, param_values)
        else:
            query = f"EXEC {name}"
            return self.execute_query(query)
        
    def get_linked_servers(self) -> List[Dict[str, Any]]:
        """
        Get list of linked servers.
        
        Returns:
            List of linked servers
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        query = """
            SELECT 
                name AS server_name,
                product AS product_name,
                provider AS provider_name,
                data_source,
                catalog
            FROM sys.servers
            WHERE is_linked = 1
            ORDER BY name
        """
        return self.execute_query(query)
        
    def get_db_settings(self) -> Dict[str, Any]:
        """
        Get database settings.
        
        Returns:
            Dictionary with database settings
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        queries = [
            ("""
                SELECT 
                    name,
                    recovery_model_desc AS recovery_model,
                    compatibility_level,
                    collation_name,
                    user_access_desc AS user_access,
                    state_desc AS state,
                    is_read_only,
                    is_auto_shrink_on,
                    is_auto_close_on
                FROM sys.databases
                WHERE name = DB_NAME()
            """, "db_settings"),
            ("""
                SELECT 
                    SERVERPROPERTY('ProductVersion') AS version,
                    SERVERPROPERTY('Edition') AS edition,
                    SERVERPROPERTY('ProductLevel') AS level,
                    SERVERPROPERTY('ServerName') AS server_name
                
            """, "server_settings")
        ]
        
        settings = {}
        for query, key in queries:
            try:
                result = self.execute_query(query)
                if result and len(result) > 0:
                    settings[key] = result[0] if key == "db_settings" else result[0]
            except Exception as e:
                self.logger.warning(f"Error getting database settings ({key}): {str(e)}")
                settings[key] = None
                
        return settings
        
    def backup_database(self, path: str) -> None:
        """
        Backup the current database.
        
        Args:
            path: Backup file path
            
        Raises:
            ConnectionError: If not connected
            QueryError: If backup fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        # Escape single quotes in the path
        safe_path = path.replace("'", "''")
        
        query = f"""
            BACKUP DATABASE {self.config.database}
            TO DISK = '{safe_path}'
            WITH FORMAT, MEDIANAME = 'SQLServerBackup', NAME = 'Full Backup';
        """
        
        self.execute_query(query)
        self.logger.info(f"Backed up database {self.config.database} to {path}")
        
    def get_tables(self) -> List[Dict[str, Any]]:
        """
        Get list of tables in the current database.
        
        Returns:
            List of tables
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        query = """
            SELECT 
                t.name AS table_name,
                s.name AS schema_name,
                p.rows AS row_count,
                CAST(ROUND((SUM(a.total_pages) * 8) / 1024.00, 2) AS DECIMAL(18,2)) AS size_mb
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.indexes i ON t.object_id = i.object_id
            JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
            JOIN sys.allocation_units a ON p.partition_id = a.container_id
            WHERE t.is_ms_shipped = 0
            GROUP BY t.name, s.name, p.rows
            ORDER BY s.name, t.name
        """
        return self.execute_query(query)
        
    def get_table_columns(self, table_name: str, schema_name: str = 'dbo') -> List[Dict[str, Any]]:
        """
        Get columns for a specific table.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (default: 'dbo')
            
        Returns:
            List of column information dictionaries
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        query = """
            SELECT 
                c.name AS column_name,
                t.name AS data_type,
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable,
                c.is_identity,
                c.column_id
            FROM sys.columns c
            JOIN sys.types t ON c.user_type_id = t.user_type_id
            JOIN sys.tables tbl ON c.object_id = tbl.object_id
            JOIN sys.schemas s ON tbl.schema_id = s.schema_id
            WHERE tbl.name = :table_name
            AND s.name = :schema_name
            ORDER BY c.column_id
        """
        return self.execute_query(query, {"table_name": table_name, "schema_name": schema_name})
        
    def get_table_indexes(self, table_name: str, schema_name: str = 'dbo') -> List[Dict[str, Any]]:
        """
        Get indexes for a specific table.
        
        Args:
            table_name: Name of the table
            schema_name: Schema name (default: 'dbo')
            
        Returns:
            List of index information dictionaries
            
        Raises:
            ConnectionError: If not connected
            QueryError: If query execution fails
        """
        if not self.is_connected():
            raise ConnectionError("Not connected to database")
            
        query = """
            SELECT 
                i.name AS index_name,
                i.type_desc AS index_type,
                i.is_unique,
                i.is_primary_key,
                i.is_disabled,
                STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            JOIN sys.tables t ON i.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name = :table_name
            AND s.name = :schema_name
            GROUP BY i.name, i.type_desc, i.is_unique, i.is_primary_key, i.is_disabled
            ORDER BY i.name
        """
        return self.execute_query(query, {"table_name": table_name, "schema_name": schema_name})