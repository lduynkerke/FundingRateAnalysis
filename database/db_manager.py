"""
Database manager module for the Funding Rate Analysis application.

This module provides functionality for connecting to and interacting with
the SQL database that stores historical funding rate data. It supports both
SQLite and PostgreSQL databases and handles database initialization, connection
management, and query execution.
"""

import os
import sqlite3
import logging
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime
import psycopg2
from psycopg2 import pool
from utils.logger import get_logger

class DatabaseManager:
    """
    Manages database connections and operations for the Funding Rate Analysis application.
    
    This class provides an abstraction layer over the database, handling connection
    management, schema creation, and query execution. It supports both SQLite and
    PostgreSQL databases based on configuration.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the database manager with the provided configuration.
        
        :param config: Database configuration dictionary
        :type config: Dict[str, Any]
        """
        self.logger = get_logger()
        self.config = config
        self.db_type = config.get('type', 'sqlite').lower()
        self.connection = None
        self.connection_pool = None
        
        self.logger.info(f"Initializing DatabaseManager with {self.db_type} database")
        
        if self.db_type == 'sqlite':
            self._init_sqlite()
        elif self.db_type == 'postgresql':
            self._init_postgresql()
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
            
        # Initialize database schema
        self._create_schema()
        
    def _init_sqlite(self):
        """
        Initialize SQLite database connection.
        """
        sqlite_config = self.config.get('sqlite', {})
        db_path = sqlite_config.get('db_path', 'database/funding_rates.db')
        
        # Only create directory if not using in-memory database
        if db_path != ':memory:' and os.path.dirname(db_path):
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        self.logger.info(f"Connecting to SQLite database at {db_path}")
        try:
            self.connection = sqlite3.connect(db_path, check_same_thread=False)
            self.connection.row_factory = sqlite3.Row
            self.logger.info("Successfully connected to SQLite database")
        except Exception as e:
            self.logger.error(f"Failed to connect to SQLite database: {e}")
            raise
            
    def _init_postgresql(self):
        """
        Initialize PostgreSQL database connection pool.
        """
        pg_config = self.config.get('postgresql', {})
        
        # Extract connection parameters
        dbname = pg_config.get('database', 'funding_rates')
        user = pg_config.get('user', 'postgres')
        password = pg_config.get('password', '')
        host = pg_config.get('host', 'localhost')
        port = pg_config.get('port', 5432)
        
        self.logger.info(f"Connecting to PostgreSQL database at {host}:{port}/{dbname}")
        try:
            # Create a connection pool
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                1, 10,  # min and max connections
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )
            
            # Get a connection to test and create schema
            self.connection = self.connection_pool.getconn()
            self.logger.info("Successfully connected to PostgreSQL database")
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL database: {e}")
            raise
            
    def _create_schema(self):
        """
        Create the database schema if it doesn't exist.
        """
        self.logger.info("Creating database schema if it doesn't exist")
        
        try:
            cursor = self.connection.cursor()
            
            # Create funding_rates table
            if self.db_type == 'sqlite':
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS funding_rates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    funding_time TIMESTAMP NOT NULL,
                    funding_rate REAL NOT NULL,
                    funding_rate_timestamp TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, funding_time)
                )
                ''')
                
                # Create index on symbol and funding_time
                cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_funding_rates_symbol_time 
                ON funding_rates(symbol, funding_time)
                ''')
                
                # Create price_data table
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS price_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    funding_time TIMESTAMP NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    granularity TEXT NOT NULL,
                    position TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (symbol, funding_time) REFERENCES funding_rates(symbol, funding_time),
                    UNIQUE(symbol, funding_time, timestamp, granularity)
                )
                ''')
                
                # Create indexes for price_data
                cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_price_data_symbol_funding_time 
                ON price_data(symbol, funding_time)
                ''')
                
                cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_price_data_granularity 
                ON price_data(granularity)
                ''')
                
                cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_price_data_position 
                ON price_data(position)
                ''')
                
            elif self.db_type == 'postgresql':
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS funding_rates (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    funding_time TIMESTAMP NOT NULL,
                    funding_rate REAL NOT NULL,
                    funding_rate_timestamp TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, funding_time)
                )
                ''')
                
                # Create index on symbol and funding_time
                cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_funding_rates_symbol_time 
                ON funding_rates(symbol, funding_time)
                ''')
                
                # Create price_data table
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS price_data (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    funding_time TIMESTAMP NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    granularity TEXT NOT NULL,
                    position TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (symbol, funding_time) REFERENCES funding_rates(symbol, funding_time),
                    UNIQUE(symbol, funding_time, timestamp, granularity)
                )
                ''')
                
                # Create indexes for price_data
                cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_price_data_symbol_funding_time 
                ON price_data(symbol, funding_time)
                ''')
                
                cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_price_data_granularity 
                ON price_data(granularity)
                ''')
                
                cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_price_data_position 
                ON price_data(position)
                ''')
            
            self.connection.commit()
            self.logger.info("Database schema created successfully")
        except Exception as e:
            self.logger.error(f"Error creating database schema: {e}")
            self.connection.rollback()
            raise
        finally:
            cursor.close()
            
    def get_connection(self):
        """
        Get a database connection.
        
        For SQLite, returns the single connection.
        For PostgreSQL, returns a connection from the pool.
        
        :return: Database connection
        """
        if self.db_type == 'sqlite':
            return self.connection
        elif self.db_type == 'postgresql':
            return self.connection_pool.getconn()
            
    def release_connection(self, conn):
        """
        Release a connection back to the pool (PostgreSQL only).
        
        :param conn: Connection to release
        """
        if self.db_type == 'postgresql' and conn is not self.connection:
            self.connection_pool.putconn(conn)
            
    def insert_funding_rates(self, funding_rates: List[Dict[str, Any]]) -> int:
        """
        Insert multiple funding rates into the database.
        
        :param funding_rates: List of funding rate dictionaries
        :type funding_rates: List[Dict[str, Any]]
        :return: Number of records inserted
        :rtype: int
        """
        if not funding_rates:
            return 0
            
        self.logger.info(f"Inserting {len(funding_rates)} funding rates into database")
        
        conn = self.get_connection()
        cursor = conn.cursor()
        inserted_count = 0
        
        try:
            for rate in funding_rates:
                symbol = rate.get('symbol')
                funding_time = datetime.fromtimestamp(int(rate.get('fundingTime')) / 1000)
                funding_rate = float(rate.get('fundingRate'))
                funding_rate_timestamp = datetime.fromtimestamp(int(rate.get('timestamp')) / 1000)
                
                if self.db_type == 'sqlite':
                    cursor.execute('''
                    INSERT OR IGNORE INTO funding_rates 
                    (symbol, funding_time, funding_rate, funding_rate_timestamp)
                    VALUES (?, ?, ?, ?)
                    ''', (symbol, funding_time, funding_rate, funding_rate_timestamp))
                else:
                    cursor.execute('''
                    INSERT INTO funding_rates 
                    (symbol, funding_time, funding_rate, funding_rate_timestamp)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (symbol, funding_time) DO NOTHING
                    ''', (symbol, funding_time, funding_rate, funding_rate_timestamp))
                    
                inserted_count += cursor.rowcount
                
            conn.commit()
            self.logger.info(f"Successfully inserted {inserted_count} funding rates")
            return inserted_count
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error inserting funding rates: {e}")
            raise
        finally:
            cursor.close()
            self.release_connection(conn)
            
    def get_funding_rates(self, symbol: Optional[str] = None, 
                         start_time: Optional[datetime] = None,
                         end_time: Optional[datetime] = None,
                         limit: int = 100) -> List[Dict[str, Any]]:
        """
        Retrieve funding rates from the database with optional filtering.
        
        :param symbol: Filter by symbol (optional)
        :type symbol: Optional[str]
        :param start_time: Filter by start time (optional)
        :type start_time: Optional[datetime]
        :param end_time: Filter by end time (optional)
        :type end_time: Optional[datetime]
        :param limit: Maximum number of records to return
        :type limit: int
        :return: List of funding rate dictionaries
        :rtype: List[Dict[str, Any]]
        """
        self.logger.info(f"Retrieving funding rates for symbol={symbol}, start_time={start_time}, end_time={end_time}, limit={limit}")
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            query = "SELECT * FROM funding_rates WHERE 1=1"
            params = []
            
            if symbol:
                query += " AND symbol = ?"
                params.append(symbol)
                
            if start_time:
                query += " AND funding_time >= ?"
                params.append(start_time)
                
            if end_time:
                query += " AND funding_time <= ?"
                params.append(end_time)
                
            query += " ORDER BY funding_time DESC LIMIT ?"
            params.append(limit)
            
            # Adjust parameter placeholders for PostgreSQL
            if self.db_type == 'postgresql':
                query = query.replace('?', '%s')
                
            cursor.execute(query, params)
            
            if self.db_type == 'sqlite':
                results = [dict(row) for row in cursor.fetchall()]
            else:
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                
            self.logger.info(f"Retrieved {len(results)} funding rates")
            return results
        except Exception as e:
            self.logger.error(f"Error retrieving funding rates: {e}")
            raise
        finally:
            cursor.close()
            self.release_connection(conn)
            
    def get_top_funding_rates(self, limit: int = 10, 
                             start_time: Optional[datetime] = None,
                             end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Get the top funding rates by absolute value within a time range.
        
        :param limit: Maximum number of records to return
        :type limit: int
        :param start_time: Filter by start time (optional)
        :type start_time: Optional[datetime]
        :param end_time: Filter by end time (optional)
        :type end_time: Optional[datetime]
        :return: List of funding rate dictionaries
        :rtype: List[Dict[str, Any]]
        """
        self.logger.info(f"Retrieving top {limit} funding rates from {start_time} to {end_time}")
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            query = """
            SELECT * FROM funding_rates 
            WHERE 1=1
            """
            params = []
            
            if start_time:
                query += " AND funding_time >= ?"
                params.append(start_time)
                
            if end_time:
                query += " AND funding_time <= ?"
                params.append(end_time)
                
            if self.db_type == 'sqlite':
                query += " ORDER BY ABS(funding_rate) DESC LIMIT ?"
            else:
                query = query.replace('?', '%s')
                query += " ORDER BY ABS(funding_rate) DESC LIMIT %s"
                
            params.append(limit)
            
            cursor.execute(query, params)
            
            if self.db_type == 'sqlite':
                results = [dict(row) for row in cursor.fetchall()]
            else:
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                
            self.logger.info(f"Retrieved {len(results)} top funding rates")
            return results
        except Exception as e:
            self.logger.error(f"Error retrieving top funding rates: {e}")
            raise
        finally:
            cursor.close()
            self.release_connection(conn)
            
    def insert_price_data(self, price_data: List[Dict[str, Any]]) -> int:
        """
        Insert multiple price data records into the database.
        
        :param price_data: List of price data dictionaries
        :type price_data: List[Dict[str, Any]]
        :return: Number of records inserted
        :rtype: int
        """
        if not price_data:
            return 0
            
        self.logger.info(f"Inserting {len(price_data)} price data records into database")
        
        conn = self.get_connection()
        cursor = conn.cursor()
        inserted_count = 0
        
        try:
            for data in price_data:
                symbol = data.get('symbol')
                funding_time = data.get('funding_time')
                timestamp = data.get('timestamp')
                granularity = data.get('granularity')
                position = data.get('position')
                open_price = float(data.get('open'))
                high = float(data.get('high'))
                low = float(data.get('low'))
                close = float(data.get('close'))
                volume = float(data.get('volume'))
                
                if self.db_type == 'sqlite':
                    cursor.execute('''
                    INSERT OR IGNORE INTO price_data 
                    (symbol, funding_time, timestamp, granularity, position, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (symbol, funding_time, timestamp, granularity, position, open_price, high, low, close, volume))
                elif self.db_type == 'postgresql':
                    cursor.execute('''
                    INSERT INTO price_data 
                    (symbol, funding_time, timestamp, granularity, position, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, funding_time, timestamp, granularity) DO NOTHING
                    ''', (symbol, funding_time, timestamp, granularity, position, open_price, high, low, close, volume))
                
                if cursor.rowcount > 0:
                    inserted_count += 1
            
            conn.commit()
            self.logger.info(f"Successfully inserted {inserted_count} price data records")
            return inserted_count
        except Exception as e:
            self.logger.error(f"Error inserting price data: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            self.release_connection(conn)
    
    def get_price_data(self, symbol: Optional[str] = None, 
                      funding_time: Optional[datetime] = None,
                      granularity: Optional[str] = None,
                      position: Optional[str] = None,
                      limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Get price data from the database with optional filtering.
        
        :param symbol: Filter by symbol (optional)
        :type symbol: Optional[str]
        :param funding_time: Filter by funding time (optional)
        :type funding_time: Optional[datetime]
        :param granularity: Filter by granularity (e.g., '1m', '10m', '1h', '1d') (optional)
        :type granularity: Optional[str]
        :param position: Filter by position ('before' or 'after') (optional)
        :type position: Optional[str]
        :param limit: Maximum number of records to return
        :type limit: int
        :return: List of price data dictionaries
        :rtype: List[Dict[str, Any]]
        """
        self.logger.info(f"Retrieving price data for symbol={symbol}, funding_time={funding_time}, granularity={granularity}, position={position}, limit={limit}")
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            query = "SELECT * FROM price_data WHERE 1=1"
            params = []
            
            if symbol:
                query += " AND symbol = ?"
                params.append(symbol)
                
            if funding_time:
                query += " AND funding_time = ?"
                params.append(funding_time)
                
            if granularity:
                query += " AND granularity = ?"
                params.append(granularity)
                
            if position:
                query += " AND position = ?"
                params.append(position)
                
            query += " ORDER BY timestamp ASC LIMIT ?"
            params.append(limit)
            
            # Adjust parameter placeholders for PostgreSQL
            if self.db_type == 'postgresql':
                query = query.replace('?', '%s')
                
            cursor.execute(query, params)
            
            if self.db_type == 'sqlite':
                results = [dict(row) for row in cursor.fetchall()]
            else:
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                
            self.logger.info(f"Retrieved {len(results)} price data records")
            return results
        except Exception as e:
            self.logger.error(f"Error retrieving price data: {e}")
            raise
        finally:
            cursor.close()
            self.release_connection(conn)
            
    def close(self):
        """
        Close all database connections.
        """
        self.logger.info("Closing database connections")
        
        try:
            if self.db_type == 'sqlite' and self.connection:
                self.connection.close()
            elif self.db_type == 'postgresql' and self.connection_pool:
                self.connection_pool.closeall()
                
            self.logger.info("Database connections closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing database connections: {e}")
            raise