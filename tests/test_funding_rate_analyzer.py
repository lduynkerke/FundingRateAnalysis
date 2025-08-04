"""
Tests for the funding rate analyzer module.

This module contains tests for the FundingRateAnalyzer class, which handles
collecting, storing, and analyzing funding rate data.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta, timezone

from pipeline.funding_rate_analyzer import FundingRateAnalyzer


@pytest.fixture
def mock_client():
    """Fixture for a mock MEXCContractClient."""
    client = MagicMock()
    
    # Mock get_available_perpetual_symbols
    client.get_available_perpetual_symbols.return_value = ['BTC_USDT', 'ETH_USDT', 'XRP_USDT']
    
    # Mock get_all_funding_rates_async
    client.get_all_funding_rates_async.return_value = [
        {
            'symbol': 'BTC_USDT',
            'fundingRate': '0.0001',
            'fundingTime': int(datetime.now(timezone.utc).timestamp() * 1000),
            'timestamp': int(datetime.now(timezone.utc).timestamp() * 1000) - 600000,
        },
        {
            'symbol': 'ETH_USDT',
            'fundingRate': '0.0002',
            'fundingTime': int(datetime.now(timezone.utc).timestamp() * 1000),
            'timestamp': int(datetime.now(timezone.utc).timestamp() * 1000) - 600000,
        },
    ]
    
    # Mock get_all_historical_funding_rates
    client.get_all_historical_funding_rates.return_value = {
        'BTC_USDT': [
            {
                'symbol': 'BTC_USDT',
                'fundingRate': '0.0001',
                'fundingTime': int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp() * 1000),
                'timestamp': int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp() * 1000) - 600000,
            },
            {
                'symbol': 'BTC_USDT',
                'fundingRate': '0.0002',
                'fundingTime': int((datetime.now(timezone.utc) - timedelta(days=2)).timestamp() * 1000),
                'timestamp': int((datetime.now(timezone.utc) - timedelta(days=2)).timestamp() * 1000) - 600000,
            },
        ],
        'ETH_USDT': [
            {
                'symbol': 'ETH_USDT',
                'fundingRate': '0.0003',
                'fundingTime': int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp() * 1000),
                'timestamp': int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp() * 1000) - 600000,
            },
        ],
    }
    
    return client


@pytest.fixture
def mock_db_manager():
    """Fixture for a mock DatabaseManager."""
    db_manager = MagicMock()
    
    # Mock insert_funding_rates
    db_manager.insert_funding_rates.return_value = 3
    
    # Mock get_funding_rates
    db_manager.get_funding_rates.return_value = [
        {
            'id': 1,
            'symbol': 'BTC_USDT',
            'funding_time': datetime.now(timezone.utc) - timedelta(days=1),
            'funding_rate': 0.0001,
            'funding_rate_timestamp': datetime.now(timezone.utc) - timedelta(days=1, minutes=10),
            'created_at': datetime.now(timezone.utc) - timedelta(days=1),
        },
        {
            'id': 2,
            'symbol': 'BTC_USDT',
            'funding_time': datetime.now(timezone.utc) - timedelta(days=2),
            'funding_rate': 0.0002,
            'funding_rate_timestamp': datetime.now(timezone.utc) - timedelta(days=2, minutes=10),
            'created_at': datetime.now(timezone.utc) - timedelta(days=2),
        },
    ]
    
    # Mock get_top_funding_rates
    db_manager.get_top_funding_rates.return_value = [
        {
            'id': 3,
            'symbol': 'ETH_USDT',
            'funding_time': datetime.now(timezone.utc) - timedelta(days=1),
            'funding_rate': 0.0003,
            'funding_rate_timestamp': datetime.now(timezone.utc) - timedelta(days=1, minutes=10),
            'created_at': datetime.now(timezone.utc) - timedelta(days=1),
        },
        {
            'id': 2,
            'symbol': 'BTC_USDT',
            'funding_time': datetime.now(timezone.utc) - timedelta(days=2),
            'funding_rate': 0.0002,
            'funding_rate_timestamp': datetime.now(timezone.utc) - timedelta(days=2, minutes=10),
            'created_at': datetime.now(timezone.utc) - timedelta(days=2),
        },
    ]
    
    return db_manager


@pytest.fixture
def config():
    """Fixture for configuration dictionary."""
    return {
        'snapshot_window_minutes': 10,
        'log_interval_hours': 4,
        'top_n_symbols': 5,
        'time_windows': {
            'daily_days_back': 3,
            'hourly_hours_back': 8,
            'ten_min_hours_before': 2,
            'one_min_minutes_before': 15,
            'one_min_minutes_after': 15,
        },
        'historical': {
            'days_back': 30,
        },
    }


@pytest.fixture
def analyzer(mock_client, mock_db_manager, config):
    """Fixture for FundingRateAnalyzer instance with mocked dependencies."""
    return FundingRateAnalyzer(
        client=mock_client,
        db_manager=mock_db_manager,
        config=config,
    )


def test_init(analyzer, mock_client, mock_db_manager, config):
    """Test initialization of FundingRateAnalyzer."""
    assert analyzer.client == mock_client
    assert analyzer.db_manager == mock_db_manager
    assert analyzer.config == config


def test_collect_historical_data(analyzer, mock_client, mock_db_manager):
    """Test collecting historical funding rate data."""
    # Test with default parameters
    result = analyzer.collect_historical_data()
    
    # Verify client method was called
    mock_client.get_available_perpetual_symbols.assert_called_once()
    mock_client.get_all_historical_funding_rates.assert_called_once()
    
    # Verify db_manager method was called
    assert mock_db_manager.insert_funding_rates.call_count == 2  # Once for each symbol
    
    # Verify result
    assert result == 6  # 3 records per symbol, 2 symbols
    
    # Reset mocks
    mock_client.reset_mock()
    mock_db_manager.reset_mock()
    
    # Test with specific symbols and days_back
    symbols = ['BTC_USDT']
    days_back = 10
    result = analyzer.collect_historical_data(symbols=symbols, days_back=days_back)
    
    # Verify client method was called with correct parameters
    mock_client.get_all_historical_funding_rates.assert_called_once_with(
        symbols=symbols,
        days_back=days_back,
        max_concurrent_requests=5,
    )
    
    # Verify result
    assert result == 3  # 3 records for 1 symbol


def test_update_funding_rates(analyzer, mock_client, mock_db_manager):
    """Test updating funding rates with latest data."""
    result = analyzer.update_funding_rates()
    
    # Verify client methods were called
    mock_client.get_available_perpetual_symbols.assert_called_once()
    mock_client.get_all_funding_rates_async.assert_called_once_with(
        ['BTC_USDT', 'ETH_USDT', 'XRP_USDT']
    )
    
    # Verify db_manager method was called
    mock_db_manager.insert_funding_rates.assert_called_once()
    
    # Verify result
    assert result == 3


def test_get_top_funding_rates(analyzer, mock_db_manager):
    """Test getting top funding rates."""
    result = analyzer.get_top_funding_rates(limit=5)
    
    # Verify db_manager method was called
    mock_db_manager.get_top_funding_rates.assert_called_once()
    
    # Verify result
    assert len(result) == 2
    assert result[0]['symbol'] == 'ETH_USDT'
    assert result[1]['symbol'] == 'BTC_USDT'


def test_get_funding_rates_for_symbol(analyzer, mock_db_manager):
    """Test getting funding rates for a specific symbol."""
    result = analyzer.get_funding_rates_for_symbol(symbol='BTC_USDT', days=7)
    
    # Verify db_manager method was called
    mock_db_manager.get_funding_rates.assert_called_once()
    
    # Verify result
    assert len(result) == 2
    assert all(rate['symbol'] == 'BTC_USDT' for rate in result)


def test_analyze_funding_rate_patterns(analyzer, mock_db_manager):
    """Test analyzing funding rate patterns."""
    result = analyzer.analyze_funding_rate_patterns(days=30)
    
    # Verify db_manager methods were called
    mock_db_manager.get_top_funding_rates.assert_called_once()
    assert mock_db_manager.get_funding_rates.call_count == 2  # Once for each symbol
    
    # Verify result structure
    assert 'period' in result
    assert 'top_symbols' in result
    assert 'total_symbols_analyzed' in result
    assert 'highest_funding_rate' in result
    assert 'average_rates_by_symbol' in result
    
    # Verify result values
    assert result['total_symbols_analyzed'] == 2
    assert len(result['top_symbols']) == 2
    assert 'ETH_USDT' in result['top_symbols']
    assert 'BTC_USDT' in result['top_symbols']
    assert result['highest_funding_rate']['symbol'] == 'ETH_USDT'