"""
Integration tests for PostgreSQL connections.
"""

import pytest

from databaseconnector.factory import ConnectionFactory
from databaseconnector.connector import DatabaseConnector
from databaseconnector.interfaces import ConnectionError, QueryError, TransactionError


# Mark all tests in this module as requiring Docker
pytestmark = pytest.mark.skipif(
    not pytest.importorskip("docker"),
    reason="Docker not available"
)


class TestPostgreSQLIntegration:
    """Integration tests for PostgreSQL database."""
    
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, postgres_container, postgres_config):
        """Set up test fixtures and clean up after tests."""
        # Create connection and connector
        self.connection = ConnectionFactory.create_connection(
            "postgres", "direct", postgres_config
        )
        self.connector = DatabaseConnector(self.connection)
        
        # Connect to database
        self.connector.connect()
        
        # Clean up any existing test tables
        try:
            self.connector.execute_query("DROP TABLE IF EXISTS test_table")
            self.connector.execute_query("DROP SCHEMA IF EXISTS test_schema CASCADE")
        except:
            pass
        
        # Run the test
        yield
        
        # Clean up after test
        try:
            self.connector.execute_query("DROP TABLE IF EXISTS test_table")
            self.connector.execute_query("DROP SCHEMA IF EXISTS test_schema CASCADE")
        except:
            pass
        
        # Close connection
        if self.connector.is_connected():
            self.connector.close()
    
    def test_connection(self):
        """Test basic connection to PostgreSQL."""
        # Verify connection is successful
        assert self.connector.is_connected()
        
        # Get server version
        version = self.connector.execute_query("SELECT version()")
        assert len(version) == 1
        assert "PostgreSQL" in version[0]["version"]
    
    def test_create_table(self):
        """Test creating a table."""
        # Create table
        self.connector.execute_query("""
            CREATE TABLE test_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value NUMERIC(10, 2) DEFAULT 0
            )
        """)
        
        # Verify table exists
        tables = self.connection.get_tables()
        assert len(tables) > 0
        assert any(t["table_name"] == "test_table" for t in tables)
    
    def test_insert_and_select(self):
        """Test inserting and selecting data."""
        # Create table
        self.connector.execute_query("""
            CREATE TABLE test_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value NUMERIC(10, 2) DEFAULT 0
            )
        """)
        
        # Insert data
        self.connector.execute_query(
            "INSERT INTO test_table (name, value) VALUES (:name, :value)",
            {"name": "test1", "value": 1.23}
        )
        self.connector.execute_query(
            "INSERT INTO test_table (name, value) VALUES (:name, :value)",
            {"name": "test2", "value": 4.56}
        )
        
        # Select data
        result = self.connector.execute_query("SELECT * FROM test_table ORDER BY id")
        
        # Verify result
        assert len(result) == 2
        assert result[0]["name"] == "test1"
        assert float(result[0]["value"]) == 1.23
        assert result[1]["name"] == "test2"
        assert float(result[1]["value"]) == 4.56
    
    def test_transaction_commit(self):
        """Test transaction with commit."""
        # Create table
        self.connector.execute_query("""
            CREATE TABLE test_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            )
        """)
        
        # Execute transaction
        self.connector.execute_transaction([
            ("INSERT INTO test_table (name) VALUES (:name)", {"name": "test1"}),
            ("INSERT INTO test_table (name) VALUES (:name)", {"name": "test2"})
        ])
        
        # Verify data was committed
        result = self.connector.execute_query("SELECT * FROM test_table ORDER BY id")
        assert len(result) == 2
        assert result[0]["name"] == "test1"
        assert result[1]["name"] == "test2"
    
    def test_transaction_rollback(self):
        """Test transaction with rollback."""
        # Create table
        self.connector.execute_query("""
            CREATE TABLE test_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL UNIQUE
            )
        """)
        
        # Insert initial data
        self.connector.execute_query(
            "INSERT INTO test_table (name) VALUES (:name)",
            {"name": "initial"}
        )
        
        # Execute transaction that will fail
        try:
            self.connector.execute_transaction([
                ("INSERT INTO test_table (name) VALUES (:name)", {"name": "test1"}),
                # This will fail because of the UNIQUE constraint on name
                ("INSERT INTO test_table (name) VALUES (:name)", {"name": "initial"})
            ])
        except TransactionError:
            pass  # Expected exception
        
        # Verify transaction was rolled back
        result = self.connector.execute_query("SELECT * FROM test_table")
        assert len(result) == 1
        assert result[0]["name"] == "initial"
    
    def test_create_schema(self):
        """Test PostgreSQL-specific create_schema method."""
        # Create schema
        self.connection.create_schema("test_schema")
        
        # Verify schema exists
        schemas = self.connection.get_schema_names()
        assert "test_schema" in schemas
        
        # Create table in schema
        self.connector.execute_query("""
            CREATE TABLE test_schema.test_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            )
        """)
        
        # Insert data
        self.connector.execute_query(
            "INSERT INTO test_schema.test_table (name) VALUES (:name)",
            {"name": "test_in_schema"}
        )
        
        # Query data
        result = self.connector.execute_query("SELECT * FROM test_schema.test_table")
        assert len(result) == 1
        assert result[0]["name"] == "test_in_schema"
    
    def test_get_table_info(self):
        """Test PostgreSQL-specific get_table_info method."""
        # Create table
        self.connector.execute_query("""
            CREATE TABLE test_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value NUMERIC(10, 2) DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Get table info
        table_info = self.connection.get_table_info("test_table")
        
        # Verify basic info
        assert table_info["table_name"] == "test_table"
        assert table_info["schema_name"] == "public"
        
        # Verify columns
        columns = table_info["columns"]
        assert len(columns) == 4
        
        # Find columns by name
        id_col = next((c for c in columns if c["column_name"] == "id"), None)
        name_col = next((c for c in columns if c["column_name"] == "name"), None)
        value_col = next((c for c in columns if c["column_name"] == "value"), None)
        
        assert id_col is not None
        assert name_col is not None
        assert value_col is not None
        
        # Check column properties
        assert id_col["data_type"] == "integer"
        assert name_col["data_type"] == "character varying"
        assert value_col["data_type"] == "numeric"
        
        assert name_col["is_nullable"] == "NO"
        assert int(name_col["character_maximum_length"]) == 100
        
        # Verify primary keys
        primary_keys = table_info["primary_keys"]
        assert len(primary_keys) == 1
        assert primary_keys[0]["column_name"] == "id"
    
    def test_create_index(self):
        """Test PostgreSQL-specific create_index method."""
        # Create table
        self.connector.execute_query("""
            CREATE TABLE test_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100) NOT NULL
            )
        """)
        
        # Create index
        self.connection.create_index(
            table_name="test_table",
            column_names=["name", "email"],
            index_name="idx_test_name_email"
        )
        
        # Get table info to verify index
        table_info = self.connection.get_table_info("test_table")
        
        # Check indexes
        indexes = table_info["indexes"]
        index_names = [idx["index_name"] for idx in indexes]
        
        assert "idx_test_name_email" in index_names