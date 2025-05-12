"""
Unit tests for the DatabaseConfig class.
"""

import pytest
from databaseconnector.config import DatabaseConfig


class TestDatabaseConfig:
    """Tests for the DatabaseConfig class."""
    
    def test_initialization(self):
        """Test proper initialization of a DatabaseConfig object with all parameters."""
        config = DatabaseConfig(
            host="test_host",
            port=1234,
            username="test_user",
            password="test_pass",
            database="test_db",
            ssl=True,
            connect_timeout=30
        )
        
        # Verify basic properties
        assert config.host == "test_host"
        assert config.port == 1234
        assert config.username == "test_user"
        assert config.password == "test_pass"
        assert config.database == "test_db"
        
        # Verify additional parameters
        assert config.connection_params["ssl"] is True
        assert config.connection_params["connect_timeout"] == 30
    
    def test_get_connection_string_postgresql(self):
        """Test PostgreSQL connection string generation."""
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            username="postgres",
            password="password",
            database="testdb",
            driver="postgresql"
        )
        
        connection_string = config.get_connection_string()
        assert connection_string == "postgresql://postgres:password@localhost:5432/testdb"
    
    def test_get_connection_string_mysql(self):
        """Test MySQL connection string generation."""
        config = DatabaseConfig(
            host="localhost",
            port=3306,
            username="mysql_user",
            password="mysql_pass",
            database="testdb",
            driver="mysql+pymysql"
        )
        
        connection_string = config.get_connection_string()
        assert connection_string == "mysql+pymysql://mysql_user:mysql_pass@localhost:3306/testdb"
    
    def test_get_connection_string_oracle(self):
        """Test Oracle connection string generation."""
        config = DatabaseConfig(
            host="localhost",
            port=1521,
            username="system",
            password="oracle",
            database="ORCL",
            driver="oracle+cx_oracle"
        )
        
        connection_string = config.get_connection_string()
        assert connection_string == "oracle+cx_oracle://system:oracle@localhost:1521/ORCL"
    
    def test_get_connection_string_mssql(self):
        """Test MSSQL connection string generation."""
        config = DatabaseConfig(
            host="localhost",
            port=1433,
            username="sa",
            password="mssql_pass",
            database="master",
            driver="mssql+pyodbc"
        )
        
        connection_string = config.get_connection_string()
        assert connection_string == "mssql+pyodbc://sa:mssql_pass@localhost:1433/master"
    
    def test_get_connection_string_sqlite(self):
        """Test SQLite connection string generation."""
        config = DatabaseConfig(
            host="",
            port=0,
            username="",
            password="",
            database=":memory:",
            driver="sqlite"
        )
        
        connection_string = config.get_connection_string()
        assert connection_string == "sqlite:///:memory:"
    
    def test_get_connection_args(self):
        """Test that get_connection_args returns a copy of the connection parameters."""
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            username="user",
            password="pass",
            database="db",
            connect_timeout=30,
            application_name="test_app"
        )
        
        args = config.get_connection_args()
        assert args["connect_timeout"] == 30
        assert args["application_name"] == "test_app"
        
        # Verify that we get a copy by modifying the returned dict
        args["new_param"] = "value"
        assert "new_param" not in config.connection_params
    
    def test_string_representation(self):
        """Test string representation of DatabaseConfig."""
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            username="user",
            password="secret",
            database="db"
        )
        
        # String representation should not include the password
        str_representation = str(config)
        assert "localhost" in str_representation
        assert "5432" in str_representation
        assert "user" in str_representation
        assert "db" in str_representation
        assert "secret" not in str_representation