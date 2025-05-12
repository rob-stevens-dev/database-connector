"""
Integration tests for MySQL connections.
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


class TestMySQLIntegration:
    """Integration tests for MySQL database."""
    
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, mysql_container, mysql_config):
        """Set up test fixtures and clean up after tests."""
        # Create connection and connector
        self.connection = ConnectionFactory.create_connection(
            "mysql", "direct", mysql_config
        )
        self.connector = DatabaseConnector(self.connection)
        
        # Connect to database
        self.connector.connect()
        
        # Clean up any existing test tables
        try:
            self.connector.execute_query("DROP TABLE IF EXISTS test_table")
        except:
            pass
        
        # Run the test
        yield
        
        # Clean up after test
        try:
            self.connector.execute_query("DROP TABLE IF EXISTS test_table")
        except:
            pass
        
        # Close connection
        if self.connector.is_connected():
            self.connector.close()
    
    def test_connection(self):
        """Test basic connection to MySQL."""
        # Verify connection is successful
        assert self.connector.is_connected()
        
        # Get server version
        version = self.connection.get_server_version()
        assert version is not None
    
    def test_create_table(self):
        """Test creating a table."""
        # Create table
        self.connector.execute_query("""
            CREATE TABLE test_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value DECIMAL(10, 2) DEFAULT 0
            )
        """)
        
        # Verify table exists
        tables = self.connection.show_tables()
        assert len(tables) > 0
        # Column name may vary based on MySQL version
        table_key = "Tables_in_test_db" if "Tables_in_test_db" in tables[0] else list(tables[0].keys())[0]
        assert any(t[table_key] == "test_table" for t in tables)
    
    def test_insert_and_select(self):
        """Test inserting and selecting data."""
        # Create table
        self.connector.execute_query("""
            CREATE TABLE test_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value DECIMAL(10, 2) DEFAULT 0
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
                id INT AUTO_INCREMENT PRIMARY KEY,
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
        # Create table with a unique constraint
        self.connector.execute_query("""
            CREATE TABLE test_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
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
    
    def test_with_transaction(self):
        """Test with_transaction method."""
        # Create table
        self.connector.execute_query("""
            CREATE TABLE test_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            )
        """)
        
        # Execute with_transaction
        def callback(conn):
            conn.execute_query("INSERT INTO test_table (name) VALUES (:name)", {"name": "test1"})
            conn.execute_query("INSERT INTO test_table (name) VALUES (:name)", {"name": "test2"})
            return "success"
        
        result = self.connector.with_transaction(callback)
        
        # Verify callback result
        assert result == "success"
        
        # Verify data was committed
        query_result = self.connector.execute_query("SELECT * FROM test_table ORDER BY id")
        assert len(query_result) == 2
        assert query_result[0]["name"] == "test1"
        assert query_result[1]["name"] == "test2"
    
    def test_show_databases(self):
        """Test MySQL-specific show_databases method."""
        # Get databases
        databases = self.connection.show_databases()
        
        # Verify result
        assert len(databases) > 0
        # Look for our test database
        db_names = [db.get('Database', '') for db in databases]
        assert "test_db" in db_names
    
    def test_get_table_structure(self):
        """Test MySQL-specific get_table_structure method."""
        # Create table
        self.connector.execute_query("""
            CREATE TABLE test_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value DECIMAL(10, 2) DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Get table structure
        structure = self.connection.get_table_structure("test_table")
        
        # Verify structure
        assert len(structure) == 4  # Four columns
        
        # Find columns by name
        field_key = 'Field'
        id_col = next((col for col in structure if col.get(field_key) == "id"), None)
        name_col = next((col for col in structure if col.get(field_key) == "name"), None)
        value_col = next((col for col in structure if col.get(field_key) == "value"), None)
        
        assert id_col is not None
        assert name_col is not None
        assert value_col is not None
        
        # Check column properties
        type_key = 'Type'
        null_key = 'Null'
        
        assert "int" in id_col.get(type_key, '').lower()
        assert "varchar" in name_col.get(type_key, '').lower()
        assert "decimal" in value_col.get(type_key, '').lower()
        
        assert name_col.get(null_key, '').upper() == 'NO'
    
    def test_get_create_table(self):
        """Test MySQL-specific get_create_table method."""
        # Create table
        self.connector.execute_query("""
            CREATE TABLE test_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value DECIMAL(10, 2) DEFAULT 0
            )
        """)
        
        # Get CREATE TABLE statement
        create_stmt = self.connection.get_create_table("test_table")
        
        # Verify result
        assert create_stmt is not None
        assert "CREATE TABLE" in create_stmt
        assert "test_table" in create_stmt
        assert "AUTO_INCREMENT" in create_stmt
        assert "PRIMARY KEY" in create_stmt
    
    def test_optimize_table(self):
        """Test MySQL-specific optimize_table method."""
        # Create table
        self.connector.execute_query("""
            CREATE TABLE test_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value DECIMAL(10, 2) DEFAULT 0
            )
        """)
        
        # Insert and delete some data to create fragmentation
        for i in range(100):
            self.connector.execute_query(
                "INSERT INTO test_table (name, value) VALUES (:name, :value)",
                {"name": f"test{i}", "value": i}
            )
        
        # Delete half the rows
        self.connector.execute_query("DELETE FROM test_table WHERE id % 2 = 0")
        
        # Optimize table
        self.connection.optimize_table("test_table")
    
    def test_analyze_table(self):
        """Test MySQL-specific analyze_table method."""
        # Create table
        self.connector.execute_query("""
            CREATE TABLE test_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value DECIMAL(10, 2) DEFAULT 0
            )
        """)
        
        # Insert some data
        for i in range(10):
            self.connector.execute_query(
                "INSERT INTO test_table (name, value) VALUES (:name, :value)",
                {"name": f"test{i}", "value": i}
            )
        
        # Analyze table
        self.connection.analyze_table("test_table")
    
    def test_check_table(self):
        """Test MySQL-specific check_table method."""
        # Create table
        self.connector.execute_query("""
            CREATE TABLE test_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value DECIMAL(10, 2) DEFAULT 0
            )
        """)
        
        # Check table
        result = self.connection.check_table("test_table")
        
        # Verify result
        assert len(result) > 0
        assert "test_table" in str(result)
        
        # Look for status column, which might be named differently depending on MySQL version
        status_found = False
        for row in result:
            for key, value in row.items():
                if 'status' in key.lower() and 'ok' in str(value).lower():
                    status_found = True
                    break
        
        assert status_found
    
    def test_get_variables(self):
        """Test MySQL-specific get_variables method."""
        # Get all variables
        all_vars = self.connection.get_variables()
        
        # Verify result
        assert len(all_vars) > 0
        
        # Get specific variable with pattern
        version_vars = self.connection.get_variables("version%")
        
        # Verify result
        assert len(version_vars) > 0
        # At least one variable name should contain "version"
        var_found = False
        for var in version_vars:
            for key, value in var.items():
                if 'variable_name' in key.lower() and 'version' in str(value).lower():
                    var_found = True
                    break
        
        assert var_found