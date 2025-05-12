"""
Unit tests for the DatabaseConnector class.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call

from databaseconnector.connector import DatabaseConnector
from databaseconnector.interfaces import ConnectionInterface, ConnectionError, QueryError, TransactionError


class TestDatabaseConnector:
    """Tests for the DatabaseConnector class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.logger = Mock()
        self.mock_connection = Mock(spec=ConnectionInterface)
        self.connector = DatabaseConnector(self.mock_connection, self.logger)
    
    def test_connect(self):
        """Test connect method."""
        # Set up mock
        mock_result = Mock()
        self.mock_connection.connect.return_value = mock_result
        
        # Call connect
        result = self.connector.connect()
        
        # Verify
        self.mock_connection.connect.assert_called_once()
        assert result == mock_result
    
    def test_close(self):
        """Test close method."""
        # Call close
        self.connector.close()
        
        # Verify
        self.mock_connection.disconnect.assert_called_once()
    
    def test_execute_query(self):
        """Test execute_query method."""
        # Set up mock
        mock_result = Mock()
        self.mock_connection.execute_query.return_value = mock_result
        
        # Call execute_query
        query = "SELECT * FROM test"
        params = {"id": 1}
        result = self.connector.execute_query(query, params)
        
        # Verify
        self.mock_connection.execute_query.assert_called_once_with(query, params)
        assert result == mock_result
    
    def test_execute_transaction_success(self):
        """Test execute_transaction method with successful transaction."""
        # Set up mocks
        self.mock_connection.is_connected.return_value = True
        mock_transaction = Mock()
        self.mock_connection.begin_transaction.return_value = mock_transaction
        
        mock_result1 = Mock()
        mock_result2 = Mock()
        
        # Set up execute_query to return different results for different queries
        def mock_execute_query(query, params=None):
            if query == "INSERT INTO test VALUES (:id)":
                return mock_result1
            elif query == "UPDATE test SET name = :name WHERE id = :id":
                return mock_result2
            return None
        
        self.mock_connection.execute_query.side_effect = mock_execute_query
        
        # Call execute_transaction
        queries = [
            ("INSERT INTO test VALUES (:id)", {"id": 1}),
            ("UPDATE test SET name = :name WHERE id = :id", {"id": 1, "name": "Test"})
        ]
        result = self.connector.execute_transaction(queries)
        
        # Verify
        self.mock_connection.is_connected.assert_called_once()
        self.mock_connection.begin_transaction.assert_called_once()
        
        # Verify execute_query was called for each query
        assert self.mock_connection.execute_query.call_count == 2
        self.mock_connection.execute_query.assert_has_calls([
            call(queries[0][0], queries[0][1]),
            call(queries[1][0], queries[1][1])
        ])
        
        # Verify commit was called
        self.mock_connection.commit.assert_called_once()
        
        # Verify results
        assert len(result) == 2
        assert result[0] == mock_result1
        assert result[1] == mock_result2
    
    def test_execute_transaction_not_connected(self):
        """Test execute_transaction method when not connected."""
        # Set up mock
        self.mock_connection.is_connected.return_value = False
        
        # Call execute_transaction and verify exception
        with pytest.raises(ConnectionError):
            self.connector.execute_transaction([("SELECT 1", None)])
        
        # Verify
        self.mock_connection.is_connected.assert_called_once()
        assert not self.mock_connection.begin_transaction.called
    
    def test_execute_transaction_query_error(self):
        """Test execute_transaction method with query error."""
        # Set up mocks
        self.mock_connection.is_connected.return_value = True
        mock_transaction = Mock()
        self.mock_connection.begin_transaction.return_value = mock_transaction
        
        # Set up execute_query to raise QueryError
        self.mock_connection.execute_query.side_effect = QueryError("Query failed")
        
        # Call execute_transaction and verify exception
        with pytest.raises(TransactionError):
            self.connector.execute_transaction([("SELECT 1", None)])
        
        # Verify
        self.mock_connection.is_connected.assert_called_once()
        self.mock_connection.begin_transaction.assert_called_once()
        self.mock_connection.execute_query.assert_called_once()
        
        # Verify rollback was called
        self.mock_connection.rollback.assert_called_once()
    
    def test_execute_transaction_connection_error(self):
        """Test execute_transaction method with connection error."""
        # Set up mocks
        self.mock_connection.is_connected.return_value = True
        mock_transaction = Mock()
        self.mock_connection.begin_transaction.return_value = mock_transaction
        
        # Set up execute_query to raise ConnectionError
        self.mock_connection.execute_query.side_effect = ConnectionError("Connection lost")
        
        # Call execute_transaction and verify exception
        with pytest.raises(TransactionError):
            self.connector.execute_transaction([("SELECT 1", None)])
        
        # Verify
        self.mock_connection.is_connected.assert_called_once()
        self.mock_connection.begin_transaction.assert_called_once()
        self.mock_connection.execute_query.assert_called_once()
        
        # Verify rollback was called
        self.mock_connection.rollback.assert_called_once()
    
    def test_execute_transaction_other_error(self):
        """Test execute_transaction method with unexpected error."""
        # Set up mocks
        self.mock_connection.is_connected.return_value = True
        mock_transaction = Mock()
        self.mock_connection.begin_transaction.return_value = mock_transaction
        
        # Set up execute_query to raise unexpected error
        self.mock_connection.execute_query.side_effect = Exception("Unexpected error")
        
        # Call execute_transaction and verify exception
        with pytest.raises(TransactionError):
            self.connector.execute_transaction([("SELECT 1", None)])
        
        # Verify
        self.mock_connection.is_connected.assert_called_once()
        self.mock_connection.begin_transaction.assert_called_once()
        self.mock_connection.execute_query.assert_called_once()
        
        # Verify rollback was called
        self.mock_connection.rollback.assert_called_once()
    
    def test_execute_transaction_rollback_error(self):
        """Test execute_transaction method with error during rollback."""
        # Set up mocks
        self.mock_connection.is_connected.return_value = True
        mock_transaction = Mock()
        self.mock_connection.begin_transaction.return_value = mock_transaction
        
        # Set up execute_query to raise QueryError
        self.mock_connection.execute_query.side_effect = QueryError("Query failed")
        
        # Set up rollback to raise TransactionError
        self.mock_connection.rollback.side_effect = TransactionError("Rollback failed")
        
        # Call execute_transaction and verify exception
        with pytest.raises(TransactionError):
            self.connector.execute_transaction([("SELECT 1", None)])
        
        # Verify
        self.mock_connection.is_connected.assert_called_once()
        self.mock_connection.begin_transaction.assert_called_once()
        self.mock_connection.execute_query.assert_called_once()
        self.mock_connection.rollback.assert_called_once()
        
        # Verify error was logged
        assert self.logger.error.called
    
    def test_with_transaction_success(self):
        """Test with_transaction method with successful callback."""
        # Set up mocks
        self.mock_connection.is_connected.return_value = True
        mock_transaction = Mock()
        self.mock_connection.begin_transaction.return_value = mock_transaction
        
        # Set up mock callback
        mock_result = Mock()
        mock_callback = Mock(return_value=mock_result)
        
        # Call with_transaction
        result = self.connector.with_transaction(mock_callback)
        
        # Verify
        self.mock_connection.is_connected.assert_called_once()
        self.mock_connection.begin_transaction.assert_called_once()
        mock_callback.assert_called_once_with(self.mock_connection)
        self.mock_connection.commit.assert_called_once()
        assert result == mock_result
    
    def test_with_transaction_not_connected(self):
        """Test with_transaction method when not connected."""
        # Set up mock
        self.mock_connection.is_connected.return_value = False
        
        # Call with_transaction and verify exception
        with pytest.raises(ConnectionError):
            self.connector.with_transaction(lambda conn: None)
        
        # Verify
        self.mock_connection.is_connected.assert_called_once()
        assert not self.mock_connection.begin_transaction.called
    
    def test_with_transaction_callback_error(self):
        """Test with_transaction method with error in callback."""
        # Set up mocks
        self.mock_connection.is_connected.return_value = True
        mock_transaction = Mock()
        self.mock_connection.begin_transaction.return_value = mock_transaction
        
        # Set up mock callback to raise exception
        mock_callback = Mock(side_effect=Exception("Callback failed"))
        
        # Call with_transaction and verify exception
        with pytest.raises(TransactionError):
            self.connector.with_transaction(mock_callback)
        
        # Verify
        self.mock_connection.is_connected.assert_called_once()
        self.mock_connection.begin_transaction.assert_called_once()
        mock_callback.assert_called_once_with(self.mock_connection)
        self.mock_connection.rollback.assert_called_once()
    
    def test_is_connected(self):
        """Test is_connected method."""
        # Set up mock
        self.mock_connection.is_connected.return_value = True
        
        # Call is_connected
        result = self.connector.is_connected()
        
        # Verify
        self.mock_connection.is_connected.assert_called_once()
        assert result is True
    
    def test_context_manager_success(self):
        """Test context manager with successful execution."""
        # Set up mocks
        mock_connection_result = Mock()
        self.mock_connection.connect.return_value = mock_connection_result
        
        # Use context manager
        with self.connector as conn:
            # Verify connect was called
            self.mock_connection.connect.assert_called_once()
            assert conn == self.connector
        
        # Verify close was called
        self.mock_connection.disconnect.assert_called_once()
    
    def test_context_manager_with_exception(self):
        """Test context manager with exception."""
        # Set up mocks
        mock_connection_result = Mock()
        self.mock_connection.connect.return_value = mock_connection_result
        self.mock_connection.is_connected.return_value = True
        
        # Use context manager with exception
        try:
            with self.connector:
                # Verify connect was called
                self.mock_connection.connect.assert_called_once()
                
                # Raise exception
                raise ValueError("Test exception")
        except ValueError:
            pass
        
        # Verify rollback and close were called
        self.mock_connection.rollback.assert_called_once()
        self.mock_connection.disconnect.assert_called_once()
    
    def test_context_manager_rollback_error(self):
        """Test context manager with exception during rollback."""
        # Set up mocks
        mock_connection_result = Mock()
        self.mock_connection.connect.return_value = mock_connection_result
        self.mock_connection.is_connected.return_value = True
        
        # Set up rollback to raise exception
        self.mock_connection.rollback.side_effect = TransactionError("Rollback failed")
        
        # Use context manager with exception
        try:
            with self.connector:
                # Verify connect was called
                self.mock_connection.connect.assert_called_once()
                
                # Raise exception
                raise ValueError("Test exception")
        except ValueError:
            pass
        
        # Verify rollback was called and close was still called
        self.mock_connection.rollback.assert_called_once()
        self.mock_connection.disconnect.assert_called_once()