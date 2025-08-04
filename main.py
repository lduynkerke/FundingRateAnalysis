"""
Main entry point for the Funding Rate Analysis application.

This module initializes the application components and provides the main functionality
for collecting, storing, and analyzing cryptocurrency funding rates. It handles:

1. Setting up the logging system
2. Initializing the MEXC API client
3. Setting up the database connection
4. Providing command-line interface for different operations
5. Scheduling periodic updates of funding rate data

The application can be run in different modes:
- Historical data collection: Fetches and stores historical funding rate data
- Update mode: Updates the database with the latest funding rates
- Analysis mode: Performs analysis on the stored funding rate data
"""

import argparse
import time
import schedule
from datetime import datetime, timezone
from api.contract_client import MEXCContractClient
from database.db_manager import DatabaseManager
from pipeline.funding_rate_analyzer import FundingRateAnalyzer
from utils.config_loader import load_config
from utils.logger import setup_logger, get_logger


def main():
    """
    Main entry point for the Funding Rate Analysis application.
    
    Parses command-line arguments and runs the appropriate functionality.
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Funding Rate Analysis Tool')
    parser.add_argument('--historical', action='store_true', help='Collect historical funding rate data')
    parser.add_argument('--update', action='store_true', help='Update with latest funding rates')
    parser.add_argument('--analyze', action='store_true', help='Analyze funding rate patterns')
    parser.add_argument('--days', type=int, default=30, help='Number of days to look back for historical data')
    parser.add_argument('--schedule', action='store_true', help='Run in scheduled mode')
    parser.add_argument('--interval', type=int, default=60, help='Update interval in minutes (for scheduled mode)')
    
    args = parser.parse_args()
    
    # Initialize the logger
    logger = setup_logger()
    logger.info("Starting Funding Rate Analysis application")
    
    try:
        # Load configuration
        config = load_config()
        logger.info("Configuration loaded successfully")
        
        # Initialize API client
        client = MEXCContractClient(config=config['mexc'])
        logger.info("MEXC client initialized")
        
        # Initialize database manager
        db_manager = DatabaseManager(config=config['database'])
        logger.info("Database manager initialized")
        
        # Initialize funding rate analyzer
        analyzer = FundingRateAnalyzer(client=client, db_manager=db_manager, config=config['funding'])
        logger.info("Funding rate analyzer initialized")
        
        # Run in scheduled mode if specified
        if args.schedule:
            logger.info(f"Running in scheduled mode with {args.interval} minute interval")
            schedule.every(args.interval).minutes.do(run_update, analyzer)
            
            # Run once immediately
            run_update(analyzer)
            
            logger.info("Entering main loop")
            while True:
                schedule.run_pending()
                time.sleep(60)
        
        # Otherwise, run the specified operations
        else:
            # Collect historical data if specified
            if args.historical:
                logger.info(f"Collecting historical data for the past {args.days} days")
                records = analyzer.collect_historical_data(days_back=args.days)
                logger.info(f"Collected {records} historical funding rate records")
            
            # Update with latest data if specified
            if args.update:
                logger.info("Updating with latest funding rates")
                records = analyzer.update_funding_rates()
                logger.info(f"Added {records} new funding rate records")
            
            # Analyze funding rate patterns if specified
            if args.analyze:
                logger.info(f"Analyzing funding rate patterns for the past {args.days} days")
                analysis = analyzer.analyze_funding_rate_patterns(days=args.days)
                
                # Print analysis results
                logger.info("Analysis results:")
                logger.info(f"Period: {analysis['period']}")
                logger.info(f"Top symbols: {', '.join(analysis['top_symbols'])}")
                logger.info(f"Total symbols analyzed: {analysis['total_symbols_analyzed']}")
                
                if analysis['highest_funding_rate']:
                    highest = analysis['highest_funding_rate']
                    logger.info(f"Highest funding rate: {highest['symbol']} at {highest['funding_rate']}")
                
                logger.info("Average rates by symbol:")
                for symbol, avg_rate in sorted(
                    analysis['average_rates_by_symbol'].items(), 
                    key=lambda x: abs(x[1]), 
                    reverse=True
                )[:10]:
                    logger.info(f"  {symbol}: {avg_rate:.6f}")
            
            # If no operation specified, print help
            if not (args.historical or args.update or args.analyze):
                parser.print_help()
        
    except Exception as e:
        logger.critical(f"Fatal error in main application: {e}", exc_info=True)
        raise
    finally:
        # Close database connections
        if 'db_manager' in locals():
            db_manager.close()
            logger.info("Database connections closed")


def run_update(analyzer: FundingRateAnalyzer) -> None:
    """
    Update the database with the latest funding rates.
    
    This function is designed to be called periodically by the scheduler.
    
    :param analyzer: Initialized FundingRateAnalyzer
    :type analyzer: FundingRateAnalyzer
    :return: None
    """
    logger = get_logger()
    try:
        logger.info(f"Scheduled update at {datetime.now(timezone.utc).isoformat()}")
        records = analyzer.update_funding_rates()
        logger.info(f"Added {records} new funding rate records")
    except Exception as e:
        logger.error(f"Error during scheduled update: {e}", exc_info=True)


if __name__ == "__main__":
    main()