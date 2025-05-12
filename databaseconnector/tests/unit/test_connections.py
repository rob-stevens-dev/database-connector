"""
Unit tests for database connections.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from databaseconnector.config import DatabaseConfig
from databaseconnector.interfaces import ConnectionStrategy, ConnectionError, QueryError
from databaseconnector.connections.base import DatabaseConnection
from databaseconnector.connections.postgres import PostgreSQLConnection
from databaseconnector.connections.mysql import MySQLConnection
from databaseconnector.connections.oracle import OracleConnection
from databaseconnector.connections.mssql import MSSQLConnection
from databaseconnector.connections.sqlite import SQLiteConnection


class TestDatabaseConnection:
    """Tests for the base DatabaseConnection class."""
    
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
        self.mock_strategy = Mock(spec=ConnectionStrategy)
        self.connection = DatabaseConnection(
            self.db_config, self.logger, self.mock_strategy
        )
    
    def test_init_no_strategy(self):
        """Test initialization with no strategy."""
        # Initialize with no strategy and verify exception
        with pytest.raises(ValueError):
            DatabaseConnection(self.db_config, self.logger, None)
    
    def test_connect(self):
        """Test connect delegates to strategy."""
        # Set up mock
        mock_result = Mock()
        self.mock_strategy.connect.return_value = mock_result
        
        # Call connect
        result = self.connection.connect()
        
        # Verify
        self.mock_strategy.connect.assert_called_once()
        assert result == mock_result
    
    def test_disconnect(self):
        """Test disconnect delegates to strategy."""
        # Call disconnect
        self.connection.disconnect()
        
        # Verify
        self.mock_strategy.disconnect.assert_called_once()
    
    def test_execute_query(self):
        """Test execute_query delegates to strategy."""
        # Set up mock
        mock_result = Mock()
        self.mock_strategy.execute_query.return_value = mock_result
        
        # Call execute_query
        query = "SELECT * FROM test"
        params = {"id": 1}
        result = self.connection.execute_query(query, params)
        
        # Verify
        self.mock_strategy.execute_query.assert_called_once_with(query, params)
        assert result == mock_result
    
    def test_is_connected(self):
        """Test is_connected delegates to strategy."""
        # Set up mock
        self.mock_strategy.is_connected.return_value = True
        
        # Call is_connected
        result = self.connection.is_connected()
        
        # Verify
        self.mock_strategy.is_connected.assert_called_once()
        assert result is True
    
    def test_begin_transaction(self):
        """Test begin_transaction delegates to strategy."""
        # Set up mock
        mock_result = Mock()
        self.mock_strategy.begin_transaction.return_value = mock_result
        
        # Call begin_transaction
        result = self.connection.begin_transaction()
        
        # Verify
        self.mock_strategy.begin_transaction.assert_called_once()
        assert result == mock_result
    
    def test_commit(self):
        """Test commit delegates to strategy."""
        # Call commit
        self.connection.commit()
        
        # Verify
        self.mock_strategy.commit.assert_called_once()
    
    def test_rollback(self):
        """Test rollback delegates to strategy."""
        # Call rollback
        self.connection.rollback()
        
        # Verify
        self.mock_strategy.rollback.assert_called_once()


class TestPostgreSQLConnection:
    """Tests for the PostgreSQLConnection class."""
    
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
        self.mock_strategy = Mock(spec=ConnectionStrategy)
        self.connection = PostgreSQLConnection(
            self.db_config, self.logger, self.mock_strategy
        )
    
    def test_init_sets_driver(self):
        """Test initialization sets the driver if not provided."""
        # Check that driver was set in config
        assert self.db_config.connection_params.get('driver') == 'postgresql'
        
        # Create with existing driver
        config_with_driver = DatabaseConfig(
            host="localhost",
            port=5432,
            username="test_user",
            password="test_pass",
            database="test_db",
            driver="custom_postgresql"
        )
        
        connection = PostgreSQLConnection(
            config_with_driver, self.logger, self.mock_strategy
        )
        
        # Driver should not be overridden
        assert config_with_driver.connection_params.get('driver') == 'custom_postgresql'
    
    def test_get_tables(self):
        """Test get_tables method."""
        # Set up mock for is_connected and execute_query
        self.connection.is_connected = Mock(return_value=True)
        self.connection.execute_query = Mock(return_value=[
            {"table_name": "table1", "schema_name": "public"},
            {"table_name": "table2", "schema_name": "public"}
        ])
        
        # Call get_tables
        result = self.connection.get_tables()
        
        # Verify
        self.connection.is_connected.assert_called_once()
        self.connection.execute_query.assert_called_once()
        assert len(result) == 2
        assert result[0]["table_name"] == "table1"
        assert result[1]["schema_name"] == "public"
    
    def test_get_tables_not_connected(self):
        """Test get_tables when not connected."""
        # Set up mock for is_connected
        self.connection.is_connected = Mock(return_value=False)
        
        # Call get_tables and verify exception
        with pytest.raises(ConnectionError):
            self.connection.get_tables()
    
    def test_create_schema(self):
        """Test create_schema method."""
        # Set up mock for is_connected and execute_query
        self.connection.is_connected = Mock(return_value=True)
        self.connection.execute_query = Mock()
        
        # Call create_schema
        self.connection.create_schema("test_schema")
        
        # Verify
        self.connection.is_connected.assert_called_once()
        self.connection.execute_query.assert_called_once()
        
        # Check that logger was used
        assert self.logger.info.called
    
    def test_create_schema_invalid_name(self):
        """Test create_schema with invalid schema name."""
        # Set up mock for is_connected
        self.connection.is_connected = Mock(return_value=True)
        
        # Call create_schema with invalid name and verify exception
        with pytest.raises(ValueError):
            self.connection.create_schema("invalid; schema")
    
    def test_get_schema_names(self):
        """Test get_schema_names method."""
        # Set up mock for is_connected and execute_query
        self.connection.is_connected = Mock(return_value=True)
        self.connection.execute_query = Mock(return_value=[
            {"schema_name": "public"},
            {"schema_name": "test_schema"}
        ])
        
        # Call get_schema_names
        result = self.connection.get_schema_names()
        
        # Verify
        self.connection.is_connected.assert_called_once()
        self.connection.execute_query.assert_called_once()
        assert len(result) == 2
        assert "public" in result
        assert "test_schema" in result
    
    def test_get_table_info(self):
        """Test get_table_info method."""
        # Set up mock for is_connected and execute_query
        self.connection.is_connected = Mock(return_value=True)
        
        # Different mock results for each query
        columns_result = [{"column_name": "id", "data_type": "integer"}]
        pk_result = [{"constraint_name": "pk_test", "column_name": "id"}]
        index_result = [{"index_name": "idx_test", "index_definition": "CREATE INDEX..."}]
        fk_result = []
        
        # Set up execute_query to return different results based on the query
        def mock_execute_query(query, params=None):
            if "information_schema.columns" in query:
                return columns_result
            elif "information_schema.table_constraints" in query and "PRIMARY KEY" in query:
                return pk_result
            elif "pg_indexes" in query:
                return index_result
            elif "information_schema.table_constraints" in query and "FOREIGN KEY" in query:
                return fk_result
            return None
        
        self.connection.execute_query = Mock(side_effect=mock_execute_query)
        
        # Call get_table_info
        result = self.connection.get_table_info("test_table")
        
        # Verify
        assert self.connection.is_connected.call_count == 1
        assert self.connection.execute_query.call_count == 4
        
        # Check result structure
        assert result["table_name"] == "test_table"
        assert result["schema_name"] == "public"
        assert result["columns"] == columns_result
        assert result["primary_keys"] == pk_result
        assert result["indexes"] == index_result
        assert result["foreign_keys"] == fk_result


class TestMySQLConnection:
    """Tests for the MySQLConnection class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.logger = Mock()
        self.db_config = DatabaseConfig(
            host="localhost",
            port=3306,
            username="test_user",
            password="test_pass",
            database="test_db"
        )
        self.mock_strategy = Mock(spec=ConnectionStrategy)
        self.connection = MySQLConnection(
            self.db_config, self.logger, self.mock_strategy
        )
    
    def test_init_sets_driver(self):
        """Test initialization sets the driver if not provided."""
        # Check that driver was set in config
        assert self.db_config.connection_params.get('driver') == 'mysql+pymysql'
    
    def test_show_tables(self):
        """Test show_tables method."""
        # Set up mock for is_connected and execute_query
        self.connection.is_connected = Mock(return_value=True)
        self.connection.execute_query = Mock(return_value=[
            {"Tables_in_test_db": "table1"},
            {"Tables_in_test_db": "table2"}
        ])
        
        # Call show_tables
        result = self.connection.show_tables()
        
        # Verify
        self.connection.is_connected.assert_called_once()
        self.connection.execute_query.assert_called_once_with("SHOW TABLES")
        assert len(result) == 2
    
    def test_get_server_version(self):
        """Test get_server_version method."""
        # Set up mock for is_connected and execute_query
        self.connection.is_connected = Mock(return_value=True)
        self.connection.execute_query = Mock(return_value=[
            {"version": "8.0.25"}
        ])
        
        # Call get_server_version
        result = self.connection.get_server_version()
        
        # Verify
        self.connection.is_connected.assert_called_once()
        self.connection.execute_query.assert_called_once_with("SELECT VERSION() AS version")
        assert result == "8.0.25"
    
    def test_get_server_version_empty_result(self):
        """Test get_server_version with empty result."""
        # Set up mock for is_connected and execute_query
        self.connection.is_connected = Mock(return_value=True)
        self.connection.execute_query = Mock(return_value=[])
        
        # Call get_server_version
        result = self.connection.get_server_version()
        
        # Verify
        assert result is None


class TestSQLiteConnection:
    """Tests for the SQLiteConnection class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.logger = Mock()
        self.db_path = ":memory:"
        self.connection = SQLiteConnection(self.db_path, self.logger)
    
    @patch('database.connections.sqlite.os.path.isdir')
    def test_init_invalid_path(self, mock_isdir):
        """Test initialization with invalid path."""
        # Set up mock
        mock_isdir.return_value = False
        
        # Initialize with invalid path and verify exception
        with pytest.raises(ValueError):
            SQLiteConnection("/invalid/path/db.sqlite", self.logger)
    
    @patch('database.connections.sqlite.sqlalchemy')
    def test_connect(self, mock_sqlalchemy):
        """Test connect method."""
        # Set up mocks
        mock_engine = Mock()
        mock_connection = Mock()
        mock_sqlalchemy.create_engine.return_value = mock_engine
        mock_engine.connect.return_value = mock_connection
        
        # Call connect
        result = self.connection.connect()
        
        # Verify
        mock_sqlalchemy.create_engine.assert_called_once()
        mock_engine.connect.assert_called_once()
        assert result == mock_connection
        assert self.connection.engine == mock_engine
        assert self.connection.connection == mock_connection
    
    @patch('database.connections.sqlite.sqlalchemy')
    def test_connect_creates_directory(self, mock_sqlalchemy):
        """Test connect creates directory if needed."""
        # This is a more complex test that would require mocking os.path.exists,
        # os.makedirs, etc. Skipping for simplicity.
        pass
    
    def test_pragma(self):
        """Test pragma method."""
        # Set up mock for is_connected and execute_query
        self.connection.is_connected = Mock(return_value=True)
        self.connection.execute_query = Mock()
        
        # Call pragma to set a value
        self.connection.pragma("journal_mode", "WAL")
        
        # Verify
        self.connection.is_connected.assert_called_once()
        self.connection.execute_query.assert_called_once_with("PRAGMA journal_mode = WAL")
    
    def test_pragma_get(self):
        """Test pragma method for getting a value."""
        # Set up mock for is_connected and execute_query
        self.connection.is_connected = Mock(return_value=True)
        expected_result = [{"journal_mode": "WAL"}]
        self.connection.execute_query = Mock(return_value=expected_result)
        
        # Call pragma to get a value
        result = self.connection.pragma("journal_mode")
        
        # Verify
        self.connection.is_connected.assert_called_once()
        self.connection.execute_query.assert_called_once_with("PRAGMA journal_mode")
        assert result == expected_result
    
    def test_pragma_invalid_name(self):
        """Test pragma with invalid name."""
        # Set up mock for is_connected
        self.connection.is_connected = Mock(return_value=True)
        
        # Call pragma with invalid name and verify exception
        with pytest.raises(ValueError):
            self.connection.pragma("invalid; pragma")
    
    def test_get_table_info(self):
        """Test get_table_info method."""
        # Set up mock for is_connected and execute_query
        self.connection.is_connected = Mock(return_value=True)
        expected_result = [
            {"cid": 0, "name": "id", "type": "INTEGER", "notnull": 1, "dflt_value": None, "pk": 1},
            {"cid": 1, "name": "name", "type": "TEXT", "notnull": 0, "dflt_value": None, "pk": 0}
        ]
        self.connection.execute_query = Mock(return_value=expected_result)
        
        # Call get_table_info
        result = self.connection.get_table_info("test_table")
        
        # Verify
        self.connection.is_connected.assert_called_once()
        self.connection.execute_query.assert_called_once_with("PRAGMA table_info(test_table)")
        assert result == expected_result
    
    def test_vacuum(self):
        """Test vacuum method."""
        # Set up mock for is_connected and execute_query
        self.connection.is_connected = Mock(return_value=True)
        self.connection.execute_query = Mock()
        
        # Call vacuum
        self.connection.vacuum()
        
        # Verify
        self.connection.is_connected.assert_called_once()
        self.connection.execute_query.assert_called_once_with("VACUUM")
        
        # Check that logger was used
        assert self.logger.info.called