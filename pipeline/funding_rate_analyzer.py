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
from typing import List, Dict, Any, Optional
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
        
        self.logger.info(f"Historical data collection completed. Total records: {total_records}")
        return total_records

    def update_funding_rates(self) -> int:
        """
        Update the database with the latest funding rates for all symbols.

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