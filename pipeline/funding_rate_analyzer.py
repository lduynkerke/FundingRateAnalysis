"""
Funding rate analysis pipeline module.

This module implements the core functionality for collecting, storing, and analyzing
cryptocurrency funding rates. It handles:

1. Fetching historical funding rate data from MEXC exchange
2. Storing the data in a SQL database
3. Analyzing funding rate patterns and identifying high-value opportunities
4. Providing methods to query and visualize the collected data

The module is designed to be called both for initial historical data collection
and for regular updates to keep the database current.
"""

import time
import math
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta, timezone
from api.contract_client import MEXCContractClient
from database.db_manager import DatabaseManager
from utils.logger import get_logger


class FundingRateAnalyzer:
    """
    Main class for funding rate data collection, storage, and analysis.
    """

    def __init__(self, client: MEXCContractClient, db_manager: DatabaseManager, config: Dict[str, Any]):
        """
        Initialize the funding rate analyzer with API client, database manager, and configuration.

        :param client: Initialized MEXCContractClient for API interactions
        :type client: MEXCContractClient
        :param db_manager: Database manager for storing and retrieving data
        :type db_manager: DatabaseManager
        :param config: Configuration dictionary containing funding settings
        :type config: Dict[str, Any]
        """
        self.logger = get_logger()
        self.client = client
        self.db_manager = db_manager
        self.config = config
        self.logger.info("FundingRateAnalyzer initialized")

    def collect_historical_data(self, symbols: Optional[List[str]] = None, days_back: Optional[int] = None) -> int:
        """
        Collect historical funding rate data for specified symbols and store in database.
        Also collect price data for symbols with the highest absolute funding rates.

        If symbols is None, all available perpetual symbols will be fetched.
        If days_back is None, the value from config will be used.

        :param symbols: List of symbols to collect data for (optional)
        :type symbols: Optional[List[str]]
        :param days_back: Number of days to look back for historical data (optional)
        :type days_back: Optional[int]
        :return: Number of funding rate records collected and stored
        :rtype: int
        """
        self.logger.info("Starting historical data collection")
        
        # Use config value if days_back not specified
        if days_back is None:
            days_back = self.config.get('historical', {}).get('days_back', 30)
            
        self.logger.info(f"Collecting historical funding rate data for the past {days_back} days")
        
        # Fetch all available symbols if not specified
        if symbols is None:
            self.logger.info("Fetching all available perpetual symbols")
            symbols = self.client.get_available_perpetual_symbols()
            self.logger.info(f"Found {len(symbols)} available perpetual symbols")
        
        # Collect historical funding rates for all symbols
        total_records = 0
        
        # Process in batches to avoid overwhelming the API
        batch_size = 10
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            self.logger.info(f"Processing batch {i//batch_size + 1} of {(len(symbols) + batch_size - 1) // batch_size}")
            
            # Get historical funding rates for the batch
            historical_rates = self.client.get_all_historical_funding_rates(
                symbols=batch,
                days_back=days_back,
                max_concurrent_requests=5
            )
            
            # Store the data in the database
            for symbol, rates in historical_rates.items():
                if rates:
                    # Add symbol to each rate entry if not already present
                    for rate in rates:
                        if 'symbol' not in rate:
                            rate['symbol'] = symbol
                    
                    # Insert into database
                    inserted = self.db_manager.insert_funding_rates(rates)
                    total_records += inserted
                    self.logger.info(f"Inserted {inserted} historical funding rates for {symbol}")
            
            # Add a small delay between batches to avoid rate limiting
            if i + batch_size < len(symbols):
                time.sleep(1)
        
        self.logger.info(f"Historical funding rate data collection completed. Total records: {total_records}")
        
        # Collect historical price data for top funding rates
        self.logger.info("Collecting historical price data for top funding rates")
        
        # Get current time
        now = datetime.now(timezone.utc)
        
        # Calculate start time based on days_back
        start_time = now - timedelta(days=days_back)
        
        # Get top funding rates from the specified period
        top_n = self.config.get('funding', {}).get('top_n_symbols', 5)
        top_rates = self.db_manager.get_top_funding_rates(
            limit=top_n * 10,  # Get more to ensure we have enough unique symbols
            start_time=start_time,
            end_time=now
        )
        
        if not top_rates:
            self.logger.info("No funding rates found for the specified period")
            return total_records
            
        # Get unique symbols and their highest absolute funding rates
        symbol_highest_rates = {}
        for rate in top_rates:
            symbol = rate['symbol']
            abs_rate = abs(float(rate['funding_rate']))
            
            if symbol not in symbol_highest_rates or abs_rate > abs(float(symbol_highest_rates[symbol]['funding_rate'])):
                symbol_highest_rates[symbol] = rate
                
        # Sort by absolute funding rate and take top_n
        sorted_rates = sorted(
            symbol_highest_rates.values(), 
            key=lambda x: abs(float(x['funding_rate'])), 
            reverse=True
        )[:top_n]
        
        self.logger.info(f"Found {len(sorted_rates)} top funding rates for historical price data collection")
        
        # Collect price data for each top funding rate
        price_records = 0
        for rate in sorted_rates:
            symbol = rate['symbol']
            funding_time = rate['funding_time']
            
            # Check if price data already exists for this funding event
            existing_data = self.db_manager.get_price_data(
                symbol=symbol,
                funding_time=funding_time,
                limit=1
            )
            
            if existing_data:
                self.logger.info(f"Price data already exists for {symbol} at {funding_time}")
                continue
                
            # Fetch and store price data
            records = self.fetch_and_store_price_data(symbol, funding_time)
            price_records += records
            self.logger.info(f"Collected {records} price data records for {symbol} at {funding_time}")
            
            # Add a small delay between symbols to avoid rate limiting
            time.sleep(1)
            
        self.logger.info(f"Historical price data collection completed. Total price records: {price_records}")
        return total_records

    def update_funding_rates(self) -> int:
        """
        Update the database with the latest funding rates for all symbols and collect price data
        for symbols with the highest absolute funding rates.

        :return: Number of new funding rate records added
        :rtype: int
        """
        self.logger.info("Updating funding rates with latest data")
        
        # Get all available symbols
        symbols = self.client.get_available_perpetual_symbols()
        self.logger.info(f"Found {len(symbols)} available perpetual symbols")
        
        # Get current funding rates for all symbols
        funding_rates = self.client.get_all_funding_rates_async(symbols)
        self.logger.info(f"Fetched {len(funding_rates)} current funding rates")
        
        # Insert into database
        inserted = self.db_manager.insert_funding_rates(funding_rates)
        self.logger.info(f"Inserted {inserted} new funding rates")
        
        # Collect price data for top funding rates
        if inserted > 0:
            self.logger.info("Collecting price data for top funding rates")
            price_records = self.collect_price_data_for_top_funding_rates()
            self.logger.info(f"Collected {price_records} price data records for top funding rates")
        
        return inserted

    def get_top_funding_rates(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get the top symbols with highest absolute funding rates from the database.

        :param limit: Maximum number of records to return
        :type limit: int
        :return: List of top funding rate records
        :rtype: List[Dict[str, Any]]
        """
        self.logger.info(f"Getting top {limit} funding rates from database")
        
        # Get current time
        now = datetime.now(timezone.utc)
        
        # Get top funding rates from the last 24 hours
        start_time = now - timedelta(hours=24)
        
        top_rates = self.db_manager.get_top_funding_rates(
            limit=limit,
            start_time=start_time,
            end_time=now
        )
        
        self.logger.info(f"Found {len(top_rates)} top funding rates")
        return top_rates

    def get_funding_rates_for_symbol(self, symbol: str, days: int = 7) -> List[Dict[str, Any]]:
        """
        Get historical funding rates for a specific symbol.

        :param symbol: Symbol to get funding rates for
        :type symbol: str
        :param days: Number of days to look back
        :type days: int
        :return: List of funding rate records for the symbol
        :rtype: List[Dict[str, Any]]
        """
        self.logger.info(f"Getting funding rates for {symbol} for the past {days} days")
        
        # Get current time
        now = datetime.now(timezone.utc)
        
        # Calculate start time
        start_time = now - timedelta(days=days)
        
        # Get funding rates from database
        rates = self.db_manager.get_funding_rates(
            symbol=symbol,
            start_time=start_time,
            end_time=now,
            limit=1000  # Set a high limit to get all records
        )
        
        self.logger.info(f"Found {len(rates)} funding rates for {symbol}")
        return rates

    def _fetch_price_data(self, symbol: str, funding_time: datetime, 
                         granularity: str, position: str) -> List[Dict[str, Any]]:
        """
        Fetch price data for a specific symbol around a funding time with the specified granularity.
        
        :param symbol: Symbol to fetch price data for
        :type symbol: str
        :param funding_time: Funding time to fetch data around
        :type funding_time: datetime
        :param granularity: Data granularity ('1m', '10m', '1h', '1d')
        :type granularity: str
        :param position: Position relative to funding time ('before' or 'after')
        :type position: str
        :return: List of price data records
        :rtype: List[Dict[str, Any]]
        """
        self.logger.info(f"Fetching {granularity} price data {position} funding time for {symbol}")
        
        # Convert funding_time to timestamp in seconds
        funding_timestamp = int(funding_time.timestamp())
        
        # Define interval and time range based on granularity and position
        interval_map = {
            '1m': 'Min1',
            '10m': 'Min10',
            '1h': 'Hour1',
            '1d': 'Day1'
        }
        
        # Calculate start and end times based on config
        if position == 'before':
            if granularity == '1m':
                minutes_before = self.config.get('funding', {}).get('time_windows', {}).get('one_min_minutes_before', 15)
                start_time = funding_timestamp - (minutes_before * 60)
                end_time = funding_timestamp
            elif granularity == '10m':
                hours_before = self.config.get('funding', {}).get('time_windows', {}).get('ten_min_hours_before', 2)
                start_time = funding_timestamp - (hours_before * 3600)
                end_time = funding_timestamp
            elif granularity == '1h':
                hours_back = self.config.get('funding', {}).get('time_windows', {}).get('hourly_hours_back', 8)
                start_time = funding_timestamp - (hours_back * 3600)
                end_time = funding_timestamp
            elif granularity == '1d':
                days_back = self.config.get('funding', {}).get('time_windows', {}).get('daily_days_back', 3)
                start_time = funding_timestamp - (days_back * 86400)
                end_time = funding_timestamp
            else:
                raise ValueError(f"Unsupported granularity: {granularity}")
        elif position == 'after':
            if granularity == '1m':
                minutes_after = self.config.get('funding', {}).get('time_windows', {}).get('one_min_minutes_after', 15)
                start_time = funding_timestamp
                end_time = funding_timestamp + (minutes_after * 60)
            else:
                raise ValueError(f"Unsupported granularity for 'after' position: {granularity}")
        else:
            raise ValueError(f"Unsupported position: {position}")
        
        # Fetch OHLCV data
        interval = interval_map.get(granularity)
        if not interval:
            raise ValueError(f"Unsupported granularity: {granularity}")
            
        ohlcv_data = self.client.get_futures_ohlcv(
            symbol=symbol,
            interval=interval,
            start=start_time,
            end=end_time
        )
        
        # Convert to price data format
        price_data = []
        for candle in ohlcv_data:
            timestamp = datetime.fromtimestamp(candle[0] / 1000, tz=timezone.utc)
            price_data.append({
                'symbol': symbol,
                'funding_time': funding_time,
                'timestamp': timestamp,
                'granularity': granularity,
                'position': position,
                'open': candle[1],
                'high': candle[2],
                'low': candle[3],
                'close': candle[4],
                'volume': candle[5]
            })
            
        self.logger.info(f"Fetched {len(price_data)} {granularity} price data points {position} funding time for {symbol}")
        return price_data
        
    def fetch_and_store_price_data(self, symbol: str, funding_time: datetime) -> int:
        """
        Fetch and store price data for a specific symbol around a funding time.
        
        :param symbol: Symbol to fetch price data for
        :type symbol: str
        :param funding_time: Funding time to fetch data around
        :type funding_time: datetime
        :return: Number of price data records stored
        :rtype: int
        """
        self.logger.info(f"Fetching and storing price data for {symbol} around funding time {funding_time}")
        
        total_records = 0
        
        # Fetch and store price data before funding time with different granularities
        for granularity in ['1m', '10m', '1h', '1d']:
            price_data = self._fetch_price_data(symbol, funding_time, granularity, 'before')
            if price_data:
                inserted = self.db_manager.insert_price_data(price_data)
                total_records += inserted
                self.logger.info(f"Inserted {inserted} {granularity} price data records before funding time for {symbol}")
        
        # Fetch and store price data after funding time (1m only)
        price_data = self._fetch_price_data(symbol, funding_time, '1m', 'after')
        if price_data:
            inserted = self.db_manager.insert_price_data(price_data)
            total_records += inserted
            self.logger.info(f"Inserted {inserted} 1m price data records after funding time for {symbol}")
            
        return total_records
        
    def collect_price_data_for_top_funding_rates(self) -> int:
        """
        Collect price data for symbols with the highest absolute funding rates.
        
        :return: Number of price data records collected and stored
        :rtype: int
        """
        self.logger.info("Collecting price data for top funding rates")
        
        # Get current time
        now = datetime.now(timezone.utc)
        
        # Get top funding rates from the last 24 hours
        start_time = now - timedelta(hours=24)
        top_n = self.config.get('funding', {}).get('top_n_symbols', 5)
        
        top_rates = self.db_manager.get_top_funding_rates(
            limit=top_n,
            start_time=start_time,
            end_time=now
        )
        
        if not top_rates:
            self.logger.info("No funding rates found in the last 24 hours")
            return 0
            
        self.logger.info(f"Found {len(top_rates)} top funding rates")
        
        # Collect price data for each top funding rate
        total_records = 0
        for rate in top_rates:
            symbol = rate['symbol']
            funding_time = rate['funding_time']
            
            # Check if price data already exists for this funding event
            existing_data = self.db_manager.get_price_data(
                symbol=symbol,
                funding_time=funding_time,
                limit=1
            )
            
            if existing_data:
                self.logger.info(f"Price data already exists for {symbol} at {funding_time}")
                continue
                
            # Fetch and store price data
            records = self.fetch_and_store_price_data(symbol, funding_time)
            total_records += records
            self.logger.info(f"Collected {records} price data records for {symbol} at {funding_time}")
            
            # Add a small delay between symbols to avoid rate limiting
            time.sleep(1)
            
        self.logger.info(f"Price data collection completed. Total records: {total_records}")
        return total_records
        
    def analyze_funding_rate_patterns(self, days: int = 30) -> Dict[str, Any]:
        """
        Analyze funding rate patterns to identify trends and anomalies.

        :param days: Number of days to analyze
        :type days: int
        :return: Dictionary with analysis results
        :rtype: Dict[str, Any]
        """
        self.logger.info(f"Analyzing funding rate patterns for the past {days} days")
        
        # Get current time
        now = datetime.now(timezone.utc)
        
        # Calculate start time
        start_time = now - timedelta(days=days)
        
        # Get top funding rates from the specified period
        top_rates = self.db_manager.get_top_funding_rates(
            limit=100,
            start_time=start_time,
            end_time=now
        )
        
        # Extract unique symbols from top rates
        symbols = list(set(rate['symbol'] for rate in top_rates))
        self.logger.info(f"Found {len(symbols)} unique symbols in top funding rates")
        
        # Get all funding rates for these symbols
        symbol_rates = {}
        for symbol in symbols:
            rates = self.db_manager.get_funding_rates(
                symbol=symbol,
                start_time=start_time,
                end_time=now,
                limit=1000
            )
            if rates:
                symbol_rates[symbol] = rates
        
        # Perform analysis (this is a simplified example)
        analysis = {
            'period': f"{start_time.isoformat()} to {now.isoformat()}",
            'top_symbols': symbols[:10],
            'total_symbols_analyzed': len(symbols),
            'highest_funding_rate': max(top_rates, key=lambda x: abs(float(x['funding_rate']))) if top_rates else None,
            'average_rates_by_symbol': {
                symbol: sum(float(rate['funding_rate']) for rate in rates) / len(rates)
                for symbol, rates in symbol_rates.items() if rates
            }
        }
        
        self.logger.info("Funding rate pattern analysis completed")
        return analysis