"""
Tests for the database manager module.

This module contains tests for the DatabaseManager class, which handles
database connections and operations for the Funding Rate Analysis application.
"""

import os
import pytest
import sqlite3
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from database.db_manager import DatabaseManager


@pytest.fixture
def sqlite_config():
    """Fixture for SQLite database configuration."""
    return {
        'type': 'sqlite',
        'sqlite': {
            'db_path': ':memory:'  # Use in-memory database for testing
        }
    }


@pytest.fixture
def db_manager(sqlite_config):
    """Fixture for DatabaseManager instance with in-memory SQLite database."""
    manager = DatabaseManager(sqlite_config)
    yield manager
    manager.close()


@pytest.fixture
def sample_funding_rates():
    """Fixture for sample funding rate data."""
    return [
        {
            'symbol': 'BTC_USDT',
            'fundingRate': '0.0001',
            'fundingTime': 1627776000000,  # 2021-08-01T00:00:00Z
            'timestamp': 1627775000000,    # 2021-07-31T23:50:00Z
        },
        {
            'symbol': 'ETH_USDT',
            'fundingRate': '0.0002',
            'fundingTime': 1627776000000,  # 2021-08-01T00:00:00Z
            'timestamp': 1627775000000,    # 2021-07-31T23:50:00Z
        },
        {
            'symbol': 'BTC_USDT',
            'fundingRate': '0.0003',
            'fundingTime': 1627804800000,  # 2021-08-01T08:00:00Z
            'timestamp': 1627803800000,    # 2021-08-01T07:50:00Z
        }
    ]


def test_init_sqlite(sqlite_config):
    """Test initialization with SQLite configuration."""
    manager = DatabaseManager(sqlite_config)
    assert manager.db_type == 'sqlite'
    assert manager.connection is not None
    assert isinstance(manager.connection, sqlite3.Connection)
    manager.close()


@patch('psycopg2.pool.SimpleConnectionPool')
def test_init_postgresql(mock_pool):
    """Test initialization with PostgreSQL configuration."""
    # Mock the connection pool and connection
    mock_conn = MagicMock()
    mock_pool.return_value.getconn.return_value = mock_conn
    
    config = {
        'type': 'postgresql',
        'postgresql': {
            'host': 'localhost',
            'port': 5432,
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_password'
        }
    }
    
    manager = DatabaseManager(config)
    assert manager.db_type == 'postgresql'
    assert manager.connection_pool is not None
    assert manager.connection is not None
    
    # Verify the pool was created with the correct parameters
    mock_pool.assert_called_once_with(
        1, 10,
        dbname='test_db',
        user='test_user',
        password='test_password',
        host='localhost',
        port=5432
    )
    
    manager.close()


def test_insert_funding_rates(db_manager, sample_funding_rates):
    """Test inserting funding rates into the database."""
    # Insert the sample funding rates
    inserted = db_manager.insert_funding_rates(sample_funding_rates)
    
    # Verify the number of inserted records
    assert inserted == 3
    
    # Query the database to verify the data was inserted correctly
    conn = db_manager.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM funding_rates")
    count = cursor.fetchone()[0]
    assert count == 3
    
    # Verify duplicate records are not inserted
    inserted = db_manager.insert_funding_rates(sample_funding_rates)
    assert inserted == 0  # No new records should be inserted
    
    cursor.execute("SELECT COUNT(*) FROM funding_rates")
    count = cursor.fetchone()[0]
    assert count == 3  # Count should still be 3


def test_get_funding_rates(db_manager, sample_funding_rates):
    """Test retrieving funding rates from the database."""
    # Insert the sample funding rates
    db_manager.insert_funding_rates(sample_funding_rates)
    
    # Test retrieving all funding rates
    rates = db_manager.get_funding_rates(limit=10)
    assert len(rates) == 3
    
    # Test filtering by symbol
    rates = db_manager.get_funding_rates(symbol='BTC_USDT', limit=10)
    assert len(rates) == 2
    assert all(rate['symbol'] == 'BTC_USDT' for rate in rates)
    
    # Test filtering by time range
    start_time = datetime.fromtimestamp(1627776000, timezone.utc)  # 2021-08-01T00:00:00Z
    end_time = datetime.fromtimestamp(1627804800, timezone.utc)    # 2021-08-01T08:00:00Z
    
    rates = db_manager.get_funding_rates(
        start_time=start_time,
        end_time=end_time,
        limit=10
    )
    assert len(rates) == 3
    
    # Test with earlier start time
    start_time = datetime.fromtimestamp(1627804800, timezone.utc)  # 2021-08-01T08:00:00Z
    rates = db_manager.get_funding_rates(
        start_time=start_time,
        limit=10
    )
    assert len(rates) == 1


def test_get_top_funding_rates(db_manager, sample_funding_rates):
    """Test retrieving top funding rates by absolute value."""
    # Insert the sample funding rates
    db_manager.insert_funding_rates(sample_funding_rates)
    
    # Test retrieving top funding rates
    top_rates = db_manager.get_top_funding_rates(limit=2)
    assert len(top_rates) == 2
    
    # Verify the rates are sorted by absolute value (descending)
    assert abs(float(top_rates[0]['funding_rate'])) >= abs(float(top_rates[1]['funding_rate']))
    
    # Test with time range filter
    start_time = datetime.fromtimestamp(1627776000, timezone.utc)  # 2021-08-01T00:00:00Z
    end_time = datetime.fromtimestamp(1627776001, timezone.utc)    # Just after 2021-08-01T00:00:00Z
    
    top_rates = db_manager.get_top_funding_rates(
        start_time=start_time,
        end_time=end_time,
        limit=10
    )
    assert len(top_rates) == 2  # Should only include the first two samples