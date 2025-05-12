"""
Database connection strategies package.

This package contains implementations of different connection strategies
for database connections (direct connections, SSH tunnels, etc.).
"""

from databaseconnector.strategies.direct import DirectConnection
from databaseconnector.strategies.ssh_tunnel import SSHTunnelConnection

__all__ = ['DirectConnection', 'SSHTunnelConnection']