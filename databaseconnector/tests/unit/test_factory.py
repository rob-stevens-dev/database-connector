"""
Unit tests for the ConnectionFactory class.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from databaseconnector.config import DatabaseConfig
from databaseconnector.factory import ConnectionFactory
from databaseconnector.strategies import DirectConnection, SSHTunnelConnection
from databaseconnector.connections import (
    PostgreSQLConnection,
    MySQLConnection,
    OracleConnection,
    MSSQLConnection,
    SQLiteConnection
)


class TestConnectionFactory:
    """Tests for the ConnectionFactory class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.logger = Mock()
        self.db_config = DatabaseConfig(
            host="localhost",
            port=5432,
            username="test_user",
            password="test_pass",
            database="test_db"
        )
        self.ssh_config = {
            "ssh_host": "ssh_host",
            "ssh_port": 22,
            "ssh_username": "ssh_user",
            "ssh_password": "ssh_pass"
        }
    
    @patch('databaseconnector.factory.ConnectionFactory._create_connection_strategy')
    @patch('databaseconnector.factory.PostgreSQLConnection')
    def test_create_postgres_connection(self, mock_postgres_class, mock_create_strategy):
        """Test create_postgres_connection method."""
        # Set up mocks
        mock_strategy = Mock()
        mock_create_strategy.return_value = mock_strategy
        mock_postgres = Mock()
        mock_postgres_class.return_value = mock_postgres
        
        # Call create_postgres_connection
        result = ConnectionFactory.create_postgres_connection(
            "direct", self.db_config, None, self.logger
        )
        
        # Verify
        mock_create_strategy.assert_called_once()
        mock_postgres_class.assert_called_once_with(
            self.db_config, self.logger, mock_strategy
        )
        assert result == mock_postgres
        
        # Verify driver was set to postgresql
        assert 'driver' in self.db_config.connection_params
        assert self.db_config.connection_params['driver'] == 'postgresql'
    
    @patch('databaseconnector.factory.ConnectionFactory._create_connection_strategy')
    @patch('databaseconnector.factory.MySQLConnection')
    def test_create_mysql_connection(self, mock_mysql_class, mock_create_strategy):
        """Test create_mysql_connection method."""
        # Set up mocks
        mock_strategy = Mock()
        mock_create_strategy.return_value = mock_strategy
        mock_mysql = Mock()
        mock_mysql_class.return_value = mock_mysql
        
        # Call create_mysql_connection
        result = ConnectionFactory.create_mysql_connection(
            "direct", self.db_config, None, self.logger
        )
        
        # Verify
        mock_create_strategy.assert_called_once()
        mock_mysql_class.assert_called_once_with(
            self.db_config, self.logger, mock_strategy
        )
        assert result == mock_mysql
        
        # Verify driver was set to mysql+pymysql
        assert 'driver' in self.db_config.connection_params
        assert self.db_config.connection_params['driver'] == 'mysql+pymysql'
    
    @patch('databaseconnector.factory.ConnectionFactory._create_connection_strategy')
    @patch('databaseconnector.factory.OracleConnection')
    def test_create_oracle_connection(self, mock_oracle_class, mock_create_strategy):
        """Test create_oracle_connection method."""
        # Set up mocks
        mock_strategy = Mock()
        mock_create_strategy.return_value = mock_strategy
        mock_oracle = Mock()
        mock_oracle_class.return_value = mock_oracle
        
        # Call create_oracle_connection
        result = ConnectionFactory.create_oracle_connection(
            "direct", self.db_config, None, self.logger
        )
        
        # Verify
        mock_create_strategy.assert_called_once()
        mock_oracle_class.assert_called_once_with(
            self.db_config, self.logger, mock_strategy
        )
        assert result == mock_oracle
        
        # Verify driver was set to oracle+cx_oracle
        assert 'driver' in self.db_config.connection_params
        assert self.db_config.connection_params['driver'] == 'oracle+cx_oracle'
    
    @patch('databaseconnector.factory.ConnectionFactory._create_connection_strategy')
    @patch('databaseconnector.factory.MSSQLConnection')
    def test_create_mssql_connection(self, mock_mssql_class, mock_create_strategy):
        """Test create_mssql_connection method."""
        # Set up mocks
        mock_strategy = Mock()
        mock_create_strategy.return_value = mock_strategy
        mock_mssql = Mock()
        mock_mssql_class.return_value = mock_mssql
        
        # Call create_mssql_connection
        result = ConnectionFactory.create_mssql_connection(
            "direct", self.db_config, None, self.logger
        )
        
        # Verify
        mock_create_strategy.assert_called_once()
        mock_mssql_class.assert_called_once_with(
            self.db_config, self.logger, mock_strategy
        )
        assert result == mock_mssql
        
        # Verify driver was set to mssql+pyodbc
        assert 'driver' in self.db_config.connection_params
        assert self.db_config.connection_params['driver'] == 'mssql+pyodbc'
    
    @patch('databaseconnector.factory.SQLiteConnection')
    def test_create_sqlite_connection(self, mock_sqlite_class):
        """Test create_sqlite_connection method."""
        # Set up mock
        mock_sqlite = Mock()
        mock_sqlite_class.return_value = mock_sqlite
        
        # Call create_sqlite_connection
        result = ConnectionFactory.create_sqlite_connection(
            ":memory:", self.logger
        )
        
        # Verify
        mock_sqlite_class.assert_called_once_with(
            ":memory:", self.logger
        )
        assert result == mock_sqlite
    
    @patch('databaseconnector.factory.DirectConnection')
    def test_create_connection_strategy_direct(self, mock_direct_class):
        """Test _create_connection_strategy with direct connection."""
        # Set up mock
        mock_direct = Mock()
        mock_direct_class.return_value = mock_direct
        
        # Call _create_connection_strategy with "direct"
        result = ConnectionFactory._create_connection_strategy(
            "direct", self.db_config, None, self.logger
        )
        
        # Verify
        mock_direct_class.assert_called_once_with(
            self.db_config, self.logger, is_remote=False
        )
        assert result == mock_direct
    
    @patch('databaseconnector.factory.DirectConnection')
    def test_create_connection_strategy_local(self, mock_direct_class):
        """Test _create_connection_strategy with local connection."""
        # Set up mock
        mock_direct = Mock()
        mock_direct_class.return_value = mock_direct
        
        # Call _create_connection_strategy with "local"
        result = ConnectionFactory._create_connection_strategy(
            "local", self.db_config, None, self.logger
        )
        
        # Verify
        mock_direct_class.assert_called_once_with(
            self.db_config, self.logger, is_remote=False
        )
        assert result == mock_direct
    
    @patch('databaseconnector.factory.DirectConnection')
    def test_create_connection_strategy_remote(self, mock_direct_class):
        """Test _create_connection_strategy with remote connection."""
        # Set up mock
        mock_direct = Mock()
        mock_direct_class.return_value = mock_direct
        
        # Call _create_connection_strategy with "remote"
        result = ConnectionFactory._create_connection_strategy(
            "remote", self.db_config, None, self.logger
        )
        
        # Verify
        mock_direct_class.assert_called_once_with(
            self.db_config, self.logger, is_remote=True
        )
        assert result == mock_direct
    
    @patch('databaseconnector.factory.SSHTunnelConnection')
    def test_create_connection_strategy_ssh_tunnel(self, mock_ssh_class):
        """Test _create_connection_strategy with SSH tunnel."""
        # Set up mock
        mock_ssh = Mock()
        mock_ssh_class.return_value = mock_ssh
        
        # Call _create_connection_strategy with "ssh_tunnel"
        result = ConnectionFactory._create_connection_strategy(
            "ssh_tunnel", self.db_config, self.ssh_config, self.logger
        )
        
        # Verify
        mock_ssh_class.assert_called_once_with(
            self.db_config, self.ssh_config, self.logger
        )
        assert result == mock_ssh
    
    def test_create_connection_strategy_ssh_tunnel_no_config(self):
        """Test _create_connection_strategy with SSH tunnel but no SSH config."""
        # Call _create_connection_strategy with "ssh_tunnel" but no SSH config
        with pytest.raises(ValueError):
            ConnectionFactory._create_connection_strategy(
                "ssh_tunnel", self.db_config, None, self.logger
            )
    
    def test_create_connection_strategy_invalid(self):
        """Test _create_connection_strategy with invalid connection type."""
        # Call _create_connection_strategy with invalid type
        with pytest.raises(ValueError):
            ConnectionFactory._create_connection_strategy(
                "invalid", self.db_config, None, self.logger
            )
    
    @patch('databaseconnector.factory.ConnectionFactory.create_postgres_connection')
    def test_create_connection_postgres(self, mock_create_postgres):
        """Test create_connection with PostgreSQL."""
        # Set up mock
        mock_postgres = Mock()
        mock_create_postgres.return_value = mock_postgres
        
        # Call create_connection
        result = ConnectionFactory.create_connection(
            "postgres", "direct", self.db_config, None, self.logger
        )
        
        # Verify
        mock_create_postgres.assert_called_once_with(
            "direct", self.db_config, None, self.logger
        )
        assert result == mock_postgres
    
    @patch('databaseconnector.factory.ConnectionFactory.create_mysql_connection')
    def test_create_connection_mysql(self, mock_create_mysql):
        """Test create_connection with MySQL."""
        # Set up mock
        mock_mysql = Mock()
        mock_create_mysql.return_value = mock_mysql
        
        # Call create_connection
        result = ConnectionFactory.create_connection(
            "mysql", "direct", self.db_config, None, self.logger
        )
        
        # Verify
        mock_create_mysql.assert_called_once_with(
            "direct", self.db_config, None, self.logger
        )
        assert result == mock_mysql
    
    @patch('databaseconnector.factory.ConnectionFactory.create_oracle_connection')
    def test_create_connection_oracle(self, mock_create_oracle):
        """Test create_connection with Oracle."""
        # Set up mock
        mock_oracle = Mock()
        mock_create_oracle.return_value = mock_oracle
        
        # Call create_connection
        result = ConnectionFactory.create_connection(
            "oracle", "direct", self.db_config, None, self.logger
        )
        
        # Verify
        mock_create_oracle.assert_called_once_with(
            "direct", self.db_config, None, self.logger
        )
        assert result == mock_oracle
    
    @patch('databaseconnector.factory.ConnectionFactory.create_mssql_connection')
    def test_create_connection_mssql(self, mock_create_mssql):
        """Test create_connection with MSSQL."""
        # Set up mock
        mock_mssql = Mock()
        mock_create_mssql.return_value = mock_mssql
        
        # Call create_connection
        result = ConnectionFactory.create_connection(
            "mssql", "direct", self.db_config, None, self.logger
        )
        
        # Verify
        mock_create_mssql.assert_called_once_with(
            "direct", self.db_config, None, self.logger
        )
        assert result == mock_mssql
    
    @patch('databaseconnector.factory.ConnectionFactory.create_sqlite_connection')
    def test_create_connection_sqlite(self, mock_create_sqlite):
        """Test create_connection with SQLite."""
        # Set up mock
        mock_sqlite = Mock()
        mock_create_sqlite.return_value = mock_sqlite
        
        # Call create_connection
        result = ConnectionFactory.create_connection(
            "sqlite", "direct", self.db_config, None, self.logger
        )
        
        # Verify
        mock_create_sqlite.assert_called_once_with(
            self.db_config.database, self.logger
        )
        assert result == mock_sqlite
    
    def test_create_connection_sqlite_invalid_type(self):
        """Test create_connection with SQLite and invalid connection type."""
        # Call create_connection with SQLite and invalid connection type
        with pytest.raises(ValueError):
            ConnectionFactory.create_connection(
                "sqlite", "ssh_tunnel", self.db_config, None, self.logger
            )
    
    def test_create_connection_invalid_db_type(self):
        """Test create_connection with invalid database type."""
        # Call create_connection with invalid database type
        with pytest.raises(ValueError):
            ConnectionFactory.create_connection(
                "invalid", "direct", self.db_config, None, self.logger
            )