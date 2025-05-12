"""
Pytest fixtures for integration tests.

This module provides fixtures for setting up Docker containers
for different database types for integration testing.
"""

import os
import time
import pytest
import docker
from typing import Dict, Any, Generator

from databaseconnector.config import DatabaseConfig


# Default waiting time for containers to be ready
CONTAINER_WAIT_TIME = 10  # seconds

# Default container configurations
POSTGRES_CONFIG = {
    "image": "postgres:13",
    "name": "test_postgres",
    "ports": {"5432/tcp": 15432},
    "environment": {
        "POSTGRES_USER": "test_user",
        "POSTGRES_PASSWORD": "test_pass",
        "POSTGRES_DB": "test_db"
    }
}

MYSQL_CONFIG = {
    "image": "mysql:8",
    "name": "test_mysql",
    "ports": {"3306/tcp": 13306},
    "environment": {
        "MYSQL_ROOT_PASSWORD": "root_pass",
        "MYSQL_USER": "test_user",
        "MYSQL_PASSWORD": "test_pass",
        "MYSQL_DATABASE": "test_db"
    }
}

SQLITE_CONFIG = {
    "path": ":memory:"  # In-memory database for testing
}


def is_docker_available() -> bool:
    """
    Check if Docker is available on the system.
    
    Returns:
        True if Docker is available, False otherwise
    """
    try:
        client = docker.from_env()
        client.ping()
        return True
    except:
        return False


@pytest.fixture(scope="session")
def docker_client() -> Generator[docker.DockerClient, None, None]:
    """
    Create a Docker client for managing containers.
    
    Yields:
        Docker client
    """
    if not is_docker_available():
        pytest.skip("Docker is not available")
        
    client = docker.from_env()
    yield client
    client.close()


@pytest.fixture(scope="session")
def postgres_container(docker_client: docker.DockerClient) -> Generator[docker.models.containers.Container, None, None]:
    """
    Start a PostgreSQL container for testing.
    
    Args:
        docker_client: Docker client
        
    Yields:
        PostgreSQL container
    """
    # Check if container already exists
    try:
        container = docker_client.containers.get(POSTGRES_CONFIG["name"])
        # Remove if not running
        if container.status != "running":
            container.remove(force=True)
            container = None
        else:
            # Already running, use it
            yield container
            return
    except docker.errors.NotFound:
        container = None
    
    # Pull image if needed
    try:
        docker_client.images.get(POSTGRES_CONFIG["image"])
    except docker.errors.ImageNotFound:
        print(f"Pulling {POSTGRES_CONFIG['image']}...")
        docker_client.images.pull(POSTGRES_CONFIG["image"])
    
    # Create and start container
    container = docker_client.containers.run(
        POSTGRES_CONFIG["image"],
        name=POSTGRES_CONFIG["name"],
        ports=POSTGRES_CONFIG["ports"],
        environment=POSTGRES_CONFIG["environment"],
        detach=True
    )
    
    # Wait for container to be ready
    print(f"Waiting for PostgreSQL container to be ready...")
    time.sleep(CONTAINER_WAIT_TIME)
    
    # Yield container for tests
    yield container
    
    # Cleanup
    container.remove(force=True)


@pytest.fixture(scope="session")
def mysql_container(docker_client: docker.DockerClient) -> Generator[docker.models.containers.Container, None, None]:
    """
    Start a MySQL container for testing.
    
    Args:
        docker_client: Docker client
        
    Yields:
        MySQL container
    """
    # Check if container already exists
    try:
        container = docker_client.containers.get(MYSQL_CONFIG["name"])
        # Remove if not running
        if container.status != "running":
            container.remove(force=True)
            container = None
        else:
            # Already running, use it
            yield container
            return
    except docker.errors.NotFound:
        container = None
    
    # Pull image if needed
    try:
        docker_client.images.get(MYSQL_CONFIG["image"])
    except docker.errors.ImageNotFound:
        print(f"Pulling {MYSQL_CONFIG['image']}...")
        docker_client.images.pull(MYSQL_CONFIG["image"])
    
    # Create and start container
    container = docker_client.containers.run(
        MYSQL_CONFIG["image"],
        name=MYSQL_CONFIG["name"],
        ports=MYSQL_CONFIG["ports"],
        environment=MYSQL_CONFIG["environment"],
        detach=True
    )
    
    # Wait for container to be ready
    print(f"Waiting for MySQL container to be ready...")
    time.sleep(CONTAINER_WAIT_TIME)
    
    # Yield container for tests
    yield container
    
    # Cleanup
    container.remove(force=True)


@pytest.fixture
def postgres_config() -> DatabaseConfig:
    """
    Create a DatabaseConfig for the PostgreSQL test container.
    
    Returns:
        DatabaseConfig instance
    """
    return DatabaseConfig(
        host="localhost",
        port=15432,  # Mapped port
        username="test_user",
        password="test_pass",
        database="test_db"
    )


@pytest.fixture
def mysql_config() -> DatabaseConfig:
    """
    Create a DatabaseConfig for the MySQL test container.
    
    Returns:
        DatabaseConfig instance
    """
    return DatabaseConfig(
        host="localhost",
        port=13306,  # Mapped port
        username="test_user",
        password="test_pass",
        database="test_db"
    )


@pytest.fixture
def sqlite_config() -> DatabaseConfig:
    """
    Create a DatabaseConfig for SQLite testing.
    
    Returns:
        DatabaseConfig instance
    """
    return DatabaseConfig(
        host="",
        port=0,
        username="",
        password="",
        database=SQLITE_CONFIG["path"]
    )