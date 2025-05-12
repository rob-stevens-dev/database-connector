"""
Unit tests for connection strategies.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import sqlalchemy

from databaseconnector.config import DatabaseConfig
from databaseconnector.strategies.direct import DirectConnection
from databaseconnector.strategies.ssh_tunnel import SSHTunnelConnection
from databaseconnector.interfaces import ConnectionError, QueryError, TransactionError


class TestDirectConnection:
    """Tests for the DirectConnection class."""
    
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
        self.connection = DirectConnection(self.db_config, self.logger)
    
    @patch('database.strategies.direct.sqlalchemy')
    def test_connect_success(self, mock_sqlalchemy):
        """Test successful connection."""
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
        
        # Verify logger was used
        assert self.logger.info.called
    
    @patch('database.strategies.direct.sqlalchemy')
    def test_connect_failure(self, mock_sqlalchemy):
        """Test connection failure."""
        # Set up mocks
        mock_engine = Mock()
        mock_sqlalchemy.create_engine.return_value = mock_engine
        mock_engine.connect.side_effect = sqlalchemy.exc.SQLAlchemyError("Connection failed")
        
        # Call connect and verify exception
        with pytest.raises(ConnectionError):
            self.connection.connect()
        
        # Verify
        mock_sqlalchemy.create_engine.assert_called_once()
        mock_engine.connect.assert_called_once()
        
        # Verify logger was used
        assert self.logger.error.called
    
    def test_disconnect_not_connected(self):
        """Test disconnection when not connected."""
        # Connection is not set
        self.connection.connection = None
        self.connection.engine = None
        
        # Call disconnect
        self.connection.disconnect()
        
        # Nothing should happen
        assert not self.logger.error.called
    
    @patch('database.strategies.direct.sqlalchemy')
    def test_disconnect_success(self, mock_sqlalchemy):
        """Test successful disconnection."""
        # Set up mocks
        mock_connection = Mock()
        mock_engine = Mock()
        self.connection.connection = mock_connection
        self.connection.engine = mock_engine
        
        # Call disconnect
        self.connection.disconnect()
        
        # Verify
        mock_connection.close.assert_called_once()
        mock_engine.dispose.assert_called_once()
        assert self.connection.connection is None
        assert self.connection.engine is None
        
        # Verify logger was used
        assert self.logger.info.called
    
    @patch('database.strategies.direct.sqlalchemy')
    def test_disconnect_failure(self, mock_sqlalchemy):
        """Test disconnection failure."""
        # Set up mocks
        mock_connection = Mock()
        mock_engine = Mock()
        self.connection.connection = mock_connection
        self.connection.engine = mock_engine
        
        # Set up disconnect to fail
        mock_connection.close.side_effect = sqlalchemy.exc.SQLAlchemyError("Disconnect failed")
        
        # Call disconnect and verify exception
        with pytest.raises(ConnectionError):
            self.connection.disconnect()
        
        # Verify logger was used
        assert self.logger.error.called
    
    def test_execute_query_not_connected(self):
        """Test execute_query when not connected."""
        # Connection is not set
        self.connection.connection = None
        
        # Call execute_query and verify exception
        with pytest.raises(ConnectionError):
            self.connection.execute_query("SELECT 1")
    
    @patch('database.strategies.direct.sqlalchemy')
    def test_execute_query_success(self, mock_sqlalchemy):
        """Test successful query execution."""
        # Set up mocks
        mock_connection = Mock()
        self.connection.connection = mock_connection
        
        # Mock is_connected to return True
        self.connection.is_connected = Mock(return_value=True)
        
        # Set up mock result
        mock_result = Mock()
        mock_row1 = {"id": 1, "name": "Test"}
        mock_row2 = {"id": 2, "name": "Test2"}
        mock_result.__iter__.return_value = [mock_row1, mock_row2]
        mock_connection.execute.return_value = mock_result
        
        # Call execute_query
        result = self.connection.execute_query("SELECT * FROM test")
        
        # Verify
        mock_connection.execute.assert_called_once()
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[1]["name"] == "Test2"
    
    @patch('database.strategies.direct.sqlalchemy')
    def test_execute_query_with_params(self, mock_sqlalchemy):
        """Test query execution with parameters."""
        # Set up mocks
        mock_connection = Mock()
        self.connection.connection = mock_connection
        
        # Mock is_connected to return True
        self.connection.is_connected = Mock(return_value=True)
        
        # Set up mock result
        mock_result = Mock()
        mock_row = {"id": 1, "name": "Test"}
        mock_result.__iter__.return_value = [mock_row]
        mock_connection.execute.return_value = mock_result
        
        # Call execute_query with params
        params = {"id": 1}
        result = self.connection.execute_query("SELECT * FROM test WHERE id = :id", params)
        
        # Verify
        mock_connection.execute.assert_called_once()
        assert len(result) == 1
        assert result[0]["id"] == 1
    
    @patch('database.strategies.direct.sqlalchemy')
    def test_execute_query_failure(self, mock_sqlalchemy):
        """Test query execution failure."""
        # Set up mocks
        mock_connection = Mock()
        self.connection.connection = mock_connection
        
        # Mock is_connected to return True
        self.connection.is_connected = Mock(return_value=True)
        
        # Set up execute to fail
        mock_connection.execute.side_effect = sqlalchemy.exc.SQLAlchemyError("Query failed")
        
        # Call execute_query and verify exception
        with pytest.raises(QueryError):
            self.connection.execute_query("SELECT * FROM test")
        
        # Verify logger was used
        assert self.logger.error.called
    
    @patch('database.strategies.direct.sqlalchemy')
    def test_is_connected_true(self, mock_sqlalchemy):
        """Test is_connected when connected."""
        # Set up mocks
        mock_connection = Mock()
        self.connection.connection = mock_connection
        
        # Call is_connected
        result = self.connection.is_connected()
        
        # Verify
        mock_connection.execute.assert_called_once()
        assert result is True
    
    @patch('database.strategies.direct.sqlalchemy')
    def test_is_connected_false_no_connection(self, mock_sqlalchemy):
        """Test is_connected when connection is None."""
        # Connection is not set
        self.connection.connection = None
        
        # Call is_connected
        result = self.connection.is_connected()
        
        # Verify
        assert result is False
    
    @patch('database.strategies.direct.sqlalchemy')
    def test_is_connected_false_exception(self, mock_sqlalchemy):
        """Test is_connected when execute raises an exception."""
        # Set up mocks
        mock_connection = Mock()
        self.connection.connection = mock_connection
        
        # Set up execute to fail
        mock_connection.execute.side_effect = sqlalchemy.exc.SQLAlchemyError("Connection lost")
        
        # Call is_connected
        result = self.connection.is_connected()
        
        # Verify
        mock_connection.execute.assert_called_once()
        assert result is False
    
    @patch('database.strategies.direct.sqlalchemy')
    def test_begin_transaction_success(self, mock_sqlalchemy):
        """Test successful transaction start."""
        # Set up mocks
        mock_connection = Mock()
        mock_transaction = Mock()
        self.connection.connection = mock_connection
        mock_connection.begin.return_value = mock_transaction
        
        # Mock is_connected to return True
        self.connection.is_connected = Mock(return_value=True)
        
        # Call begin_transaction
        result = self.connection.begin_transaction()
        
        # Verify
        mock_connection.begin.assert_called_once()
        assert result == mock_transaction
        assert self.connection.transaction == mock_transaction
    
    def test_begin_transaction_not_connected(self):
        """Test beginning transaction when not connected."""
        # Mock is_connected to return False
        self.connection.is_connected = Mock(return_value=False)
        
        # Call begin_transaction and verify exception
        with pytest.raises(ConnectionError):
            self.connection.begin_transaction()
    
    @patch('database.strategies.direct.sqlalchemy')
    def test_begin_transaction_failure(self, mock_sqlalchemy):
        """Test transaction start failure."""
        # Set up mocks
        mock_connection = Mock()
        self.connection.connection = mock_connection
        
        # Mock is_connected to return True
        self.connection.is_connected = Mock(return_value=True)
        
        # Set up begin to fail
        mock_connection.begin.side_effect = sqlalchemy.exc.SQLAlchemyError("Transaction failed")
        
        # Call begin_transaction and verify exception
        with pytest.raises(TransactionError):
            self.connection.begin_transaction()
        
        # Verify logger was used
        assert self.logger.error.called
    
    @patch('database.strategies.direct.sqlalchemy')
    def test_commit_success(self, mock_sqlalchemy):
        """Test successful transaction commit."""
        # Set up mocks
        mock_transaction = Mock()
        self.connection.transaction = mock_transaction
        
        # Call commit
        self.connection.commit()
        
        # Verify
        mock_transaction.commit.assert_called_once()
        assert self.connection.transaction is None
    
    def test_commit_no_transaction(self):
        """Test committing when no transaction is active."""
        # No transaction
        self.connection.transaction = None
        
        # Call commit and verify exception
        with pytest.raises(TransactionError):
            self.connection.commit()
    
    @patch('database.strategies.direct.sqlalchemy')
    def test_commit_failure(self, mock_sqlalchemy):
        """Test transaction commit failure."""
        # Set up mocks
        mock_transaction = Mock()
        self.connection.transaction = mock_transaction
        
        # Set up commit to fail
        mock_transaction.commit.side_effect = sqlalchemy.exc.SQLAlchemyError("Commit failed")
        
        # Call commit and verify exception
        with pytest.raises(TransactionError):
            self.connection.commit()
        
        # Verify logger was used
        assert self.logger.error.called
    
    @patch('database.strategies.direct.sqlalchemy')
    def test_rollback_success(self, mock_sqlalchemy):
        """Test successful transaction rollback."""
        # Set up mocks
        mock_transaction = Mock()
        self.connection.transaction = mock_transaction
        
        # Call rollback
        self.connection.rollback()
        
        # Verify
        mock_transaction.rollback.assert_called_once()
        assert self.connection.transaction is None
    
    def test_rollback_no_transaction(self):
        """Test rolling back when no transaction is active."""
        # No transaction
        self.connection.transaction = None
        
        # Call rollback and verify exception
        with pytest.raises(TransactionError):
            self.connection.rollback()
    
    @patch('database.strategies.direct.sqlalchemy')
    def test_rollback_failure(self, mock_sqlalchemy):
        """Test transaction rollback failure."""
        # Set up mocks
        mock_transaction = Mock()
        self.connection.transaction = mock_transaction
        
        # Set up rollback to fail
        mock_transaction.rollback.side_effect = sqlalchemy.exc.SQLAlchemyError("Rollback failed")
        
        # Call rollback and verify exception
        with pytest.raises(TransactionError):
            self.connection.rollback()
        
        # Verify logger was used
        assert self.logger.error.called


class TestSSHTunnelConnection:
    """Tests for the SSHTunnelConnection class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.logger = Mock()
        self.db_config = DatabaseConfig(
            host="remote_host",
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
        
        # Create the connection
        self.connection = SSHTunnelConnection(self.db_config, self.ssh_config, self.logger)
    
    def test_init_missing_ssh_config(self):
        """Test initialization with missing required SSH config keys."""
        # Missing ssh_username
        invalid_ssh_config = {
            "ssh_host": "ssh_host",
            "ssh_port": 22,
            "ssh_password": "ssh_pass"
        }
        
        # Initialize with invalid config and verify exception
        with pytest.raises(ValueError):
            SSHTunnelConnection(self.db_config, invalid_ssh_config, self.logger)
    
    def test_init_missing_auth(self):
        """Test initialization with missing SSH authentication."""
        # No ssh_password or ssh_key_file
        invalid_ssh_config = {
            "ssh_host": "ssh_host",
            "ssh_port": 22,
            "ssh_username": "ssh_user"
        }
        
        # Initialize with invalid config and verify exception
        with pytest.raises(ValueError):
            SSHTunnelConnection(self.db_config, invalid_ssh_config, self.logger)
    
    @patch('database.strategies.ssh_tunnel.SSHTunnelForwarder', autospec=True)
    @patch('database.strategies.ssh_tunnel.sqlalchemy')
    def test_connect_success(self, mock_sqlalchemy, mock_tunnel_class):
        """Test successful connection via SSH tunnel."""
        # Set up mocks
        mock_tunnel = Mock()
        mock_tunnel_class.return_value = mock_tunnel
        mock_tunnel.local_bind_port = 12345
        
        mock_engine = Mock()
        mock_connection = Mock()
        mock_sqlalchemy.create_engine.return_value = mock_engine
        mock_engine.connect.return_value = mock_connection
        
        # Call connect
        result = self.connection.connect()
        
        # Verify
        mock_tunnel_class.assert_called_once()
        mock_tunnel.start.assert_called_once()
        mock_sqlalchemy.create_engine.assert_called_once()
        mock_engine.connect.assert_called_once()
        
        assert result == mock_connection
        assert self.connection.tunnel == mock_tunnel
        assert self.connection.engine == mock_engine
        assert self.connection.connection == mock_connection
        
        # Verify logger was used
        assert self.logger.info.called
    
    @patch('database.strategies.ssh_tunnel.SSHTunnelForwarder', autospec=True)
    def test_connect_failure_tunnel(self, mock_tunnel_class):
        """Test connection failure when SSH tunnel fails."""
        # Set up mock to raise an exception
        mock_tunnel_class.side_effect = Exception("Tunnel failed")
        
        # Call connect and verify exception
        with pytest.raises(ConnectionError):
            self.connection.connect()
        
        # Verify logger was used
        assert self.logger.error.called
    
    @patch('database.strategies.ssh_tunnel.SSHTunnelForwarder', autospec=True)
    @patch('database.strategies.ssh_tunnel.sqlalchemy')
    def test_connect_failure_db(self, mock_sqlalchemy, mock_tunnel_class):
        """Test connection failure when database connection fails."""
        # Set up tunnel mock
        mock_tunnel = Mock()
        mock_tunnel_class.return_value = mock_tunnel
        mock_tunnel.local_bind_port = 12345
        
        # Set up sqlalchemy mock to fail
        mock_engine = Mock()
        mock_sqlalchemy.create_engine.return_value = mock_engine
        mock_engine.connect.side_effect = sqlalchemy.exc.SQLAlchemyError("Connection failed")
        
        # Call connect and verify exception
        with pytest.raises(ConnectionError):
            self.connection.connect()
        
        # Verify tunnel was started and then closed
        mock_tunnel.start.assert_called_once()
        assert mock_tunnel.close.called
        
        # Verify logger was used
        assert self.logger.error.called
    
    def test_disconnect_not_connected(self):
        """Test disconnection when not connected."""
        # Connection and tunnel are not set
        self.connection.connection = None
        self.connection.engine = None
        self.connection.tunnel = None
        
        # Call disconnect
        self.connection.disconnect()
        
        # Nothing should happen
        assert not self.logger.error.called
    
    def test_disconnect_success(self):
        """Test successful disconnection."""
        # Set up mocks
        mock_connection = Mock()
        mock_engine = Mock()
        mock_tunnel = Mock()
        self.connection.connection = mock_connection
        self.connection.engine = mock_engine
        self.connection.tunnel = mock_tunnel
        
        # Call disconnect
        self.connection.disconnect()
        
        # Verify
        mock_connection.close.assert_called_once()
        mock_engine.dispose.assert_called_once()
        mock_tunnel.close.assert_called_once()
        
        assert self.connection.connection is None
        assert self.connection.engine is None
        assert self.connection.tunnel is None
        
        # Verify logger was used
        assert self.logger.info.called
    
    def test_disconnect_failure(self):
        """Test disconnection failure."""
        # Set up mocks
        mock_connection = Mock()
        mock_engine = Mock()
        mock_tunnel = Mock()
        self.connection.connection = mock_connection
        self.connection.engine = mock_engine
        self.connection.tunnel = mock_tunnel
        
        # Set up connection close to fail
        mock_connection.close.side_effect = Exception("Disconnect failed")
        
        # Call disconnect and verify exception
        with pytest.raises(ConnectionError):
            self.connection.disconnect()
        
        # Verify logger was used
        assert self.logger.error.called
    
    def test_execute_query_not_connected(self):
        """Test execute_query when not connected."""
        # Mock is_connected to return False
        self.connection.is_connected = Mock(return_value=False)
        
        # Call execute_query and verify exception
        with pytest.raises(ConnectionError):
            self.connection.execute_query("SELECT 1")
    
    def test_is_connected_no_connection(self):
        """Test is_connected when connection is None."""
        # Connection is not set
        self.connection.connection = None
        
        # Call is_connected
        result = self.connection.is_connected()
        
        # Verify
        assert result is False
    
    def test_is_connected_no_tunnel(self):
        """Test is_connected when tunnel is None."""
        # Set up connection but no tunnel
        self.connection.connection = Mock()
        self.connection.tunnel = None
        
        # Call is_connected
        result = self.connection.is_connected()
        
        # Verify
        assert result is False
    
    def test_is_connected_tunnel_not_active(self):
        """Test is_connected when tunnel is not active."""
        # Set up connection and tunnel
        self.connection.connection = Mock()
        self.connection.tunnel = Mock()
        self.connection.tunnel.is_active = False
        
        # Call is_connected
        result = self.connection.is_connected()
        
        # Verify
        assert result is False