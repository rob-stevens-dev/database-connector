"""
Integration tests for SQLite connections.
"""

import pytest
import os

from databaseconnector.factory import ConnectionFactory
from databaseconnector.connector import DatabaseConnector
from databaseconnector.interfaces import ConnectionError, QueryError, TransactionError


class TestSQLiteIntegration:
    """Integration tests for SQLite database."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Create a temporary file for SQLite database
        self.test_db_path = "test_sqlite.db"
        
        # Create connection and connector
        self.connection = ConnectionFactory.create_sqlite_connection(self.test_db_path)
        self.connector = DatabaseConnector(self.connection)
        
    def teardown_method(self):
        """Clean up after tests."""
        # Close connection
        if hasattr(self, 'connector') and self.connector.is_connected():
            self.connector.close()
            
        # Delete test database file
        if os.path.exists(self.test_db_path):
            os.remove(self.test_db_path)
    
    def test_connection(self):
        """Test basic connection to SQLite."""
        # Connect to database
        self.connector.connect()
        
        # Verify connection is successful
        assert self.connector.is_connected()
        
        # Get SQLite version
        version = self.connection.get_sqlite_version()
        assert version is not None
        assert "." in version  # Basic version format check
    
    def test_create_table(self):
        """Test creating a table."""
        # Connect to database
        self.connector.connect()
        
        # Create table
        self.connector.execute_query("""
            CREATE TABLE test (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value REAL
            )
        """)
        
        # Verify table exists
        tables = self.connection.get_all_tables()
        assert len(tables) == 1
        assert tables[0]["table_name"] == "test"
    
    def test_insert_and_select(self):
        """Test inserting and selecting data."""
        # Connect to database
        self.connector.connect()
        
        # Create table
        self.connector.execute_query("""
            CREATE TABLE test (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value REAL
            )
        """)
        
        # Insert data
        self.connector.execute_query(
            "INSERT INTO test (name, value) VALUES (:name, :value)",
            {"name": "test1", "value": 1.23}
        )
        self.connector.execute_query(
            "INSERT INTO test (name, value) VALUES (:name, :value)",
            {"name": "test2", "value": 4.56}
        )
        
        # Select data
        result = self.connector.execute_query("SELECT * FROM test ORDER BY id")
        
        # Verify result
        assert len(result) == 2
        assert result[0]["name"] == "test1"
        assert result[0]["value"] == 1.23
        assert result[1]["name"] == "test2"
        assert result[1]["value"] == 4.56
    
    def test_transaction_commit(self):
        """Test transaction with commit."""
        # Connect to database
        self.connector.connect()
        
        # Create table
        self.connector.execute_query("""
            CREATE TABLE test (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        """)
        
        # Execute transaction
        self.connector.execute_transaction([
            ("INSERT INTO test (name) VALUES (:name)", {"name": "test1"}),
            ("INSERT INTO test (name) VALUES (:name)", {"name": "test2"})
        ])
        
        # Verify data was committed
        result = self.connector.execute_query("SELECT * FROM test ORDER BY id")
        assert len(result) == 2
        assert result[0]["name"] == "test1"
        assert result[1]["name"] == "test2"
    
    def test_transaction_rollback(self):
        """Test transaction with rollback."""
        # Connect to database
        self.connector.connect()
        
        # Create table
        self.connector.execute_query("""
            CREATE TABLE test (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        """)
        
        # Insert initial data
        self.connector.execute_query(
            "INSERT INTO test (name) VALUES (:name)",
            {"name": "initial"}
        )
        
        # Execute transaction that will fail
        try:
            self.connector.execute_transaction([
                ("INSERT INTO test (name) VALUES (:name)", {"name": "test1"}),
                ("INSERT INTO test (id, name) VALUES (:id, :name)", {"id": 1, "name": "duplicate-id"})
                # This will fail because id=1 already exists (from the first insert)
            ])
        except TransactionError:
            pass  # Expected exception
        
        # Verify transaction was rolled back
        result = self.connector.execute_query("SELECT * FROM test")
        assert len(result) == 1
        assert result[0]["name"] == "initial"
    
    def test_with_transaction(self):
        """Test with_transaction method."""
        # Connect to database
        self.connector.connect()
        
        # Create table
        self.connector.execute_query("""
            CREATE TABLE test (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        """)
        
        # Execute with_transaction
        def callback(conn):
            conn.execute_query("INSERT INTO test (name) VALUES (:name)", {"name": "test1"})
            conn.execute_query("INSERT INTO test (name) VALUES (:name)", {"name": "test2"})
            return "success"
        
        result = self.connector.with_transaction(callback)
        
        # Verify callback result
        assert result == "success"
        
        # Verify data was committed
        query_result = self.connector.execute_query("SELECT * FROM test ORDER BY id")
        assert len(query_result) == 2
        assert query_result[0]["name"] == "test1"
        assert query_result[1]["name"] == "test2"
    
    def test_with_transaction_rollback(self):
        """Test with_transaction method with rollback."""
        # Connect to database
        self.connector.connect()
        
        # Create table
        self.connector.execute_query("""
            CREATE TABLE test (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        """)
        
        # Insert initial data
        self.connector.execute_query(
            "INSERT INTO test (name) VALUES (:name)",
            {"name": "initial"}
        )
        
        # Execute with_transaction that will fail
        def callback(conn):
            conn.execute_query("INSERT INTO test (name) VALUES (:name)", {"name": "test1"})
            # This will fail because id=1 already exists (from the first insert)
            conn.execute_query("INSERT INTO test (id, name) VALUES (:id, :name)", {"id": 1, "name": "duplicate-id"})
            return "success"
        
        try:
            self.connector.with_transaction(callback)
        except TransactionError:
            pass  # Expected exception
        
        # Verify transaction was rolled back
        result = self.connector.execute_query("SELECT * FROM test")
        assert len(result) == 1
        assert result[0]["name"] == "initial"
    
    def test_context_manager(self):
        """Test context manager."""
        # Use context manager
        with self.connector as conn:
            # Verify connection is successful
            assert conn.is_connected()
            
            # Create table
            conn.execute_query("""
                CREATE TABLE test (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                )
            """)
            
            # Insert data
            conn.execute_query(
                "INSERT INTO test (name) VALUES (:name)",
                {"name": "test1"}
            )
            
            # Verify data
            result = conn.execute_query("SELECT * FROM test")
            assert len(result) == 1
            assert result[0]["name"] == "test1"
        
        # Verify connection is closed
        assert not self.connector.is_connected()
    
    def test_context_manager_with_exception(self):
        """Test context manager with exception."""
        # Insert initial data
        with self.connector as conn:
            # Create table
            conn.execute_query("""
                CREATE TABLE test (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                )
            """)
            
            # Insert initial data
            conn.execute_query(
                "INSERT INTO test (name) VALUES (:name)",
                {"name": "initial"}
            )
        
        # Use context manager with exception
        try:
            with self.connector as conn:
                # Insert data
                conn.execute_query(
                    "INSERT INTO test (name) VALUES (:name)",
                    {"name": "test1"}
                )
                
                # Raise exception
                raise ValueError("Test exception")
        except ValueError:
            pass  # Expected exception
        
        # Reconnect to verify data
        self.connector.connect()
        
        # Verify transaction was rolled back
        result = self.connector.execute_query("SELECT * FROM test")
        assert len(result) == 1
        assert result[0]["name"] == "initial"
        
        # Close connection
        self.connector.close()
    
    def test_pragma(self):
        """Test SQLite-specific PRAGMA command."""
        # Connect to database
        self.connector.connect()
        
        # Set PRAGMA
        self.connection.pragma("journal_mode", "WAL")
        
        # Get PRAGMA value
        result = self.connection.pragma("journal_mode")
        
        # Verify result
        assert len(result) == 1
        assert result[0]["journal_mode"] == "wal"
    
    def test_get_table_info(self):
        """Test SQLite-specific get_table_info method."""
        # Connect to database
        self.connector.connect()
        
        # Create table
        self.connector.execute_query("""
            CREATE TABLE test (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value REAL DEFAULT 0.0
            )
        """)
        
        # Get table info
        table_info = self.connection.get_table_info("test")
        
        # Verify result
        assert len(table_info) == 3  # Three columns
        
        # Check id column
        id_col = [col for col in table_info if col["name"] == "id"][0]
        assert id_col["type"] == "INTEGER"
        assert id_col["pk"] == 1
        
        # Check name column
        name_col = [col for col in table_info if col["name"] == "name"][0]
        assert name_col["type"] == "TEXT"
        assert name_col["notnull"] == 1
        
        # Check value column
        value_col = [col for col in table_info if col["name"] == "value"][0]
        assert value_col["type"] == "REAL"
        assert value_col["dflt_value"] == "0.0"
    
    def test_vacuum(self):
        """Test SQLite-specific vacuum method."""
        # Connect to database
        self.connector.connect()
        
        # Create and populate table
        self.connector.execute_query("""
            CREATE TABLE test (
                id INTEGER PRIMARY KEY,
                data TEXT
            )
        """)
        
        # Insert and delete data to create free space
        for i in range(100):
            self.connector.execute_query(
                "INSERT INTO test (data) VALUES (:data)",
                {"data": "x" * 1000}  # 1KB of data
            )
        
        # Delete half the rows
        self.connector.execute_query("DELETE FROM test WHERE id % 2 = 0")
        
        # Vacuum database
        self.connection.vacuum()