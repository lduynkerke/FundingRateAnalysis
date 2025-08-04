# Funding Rate Analysis

A comprehensive tool for collecting, storing, and analyzing cryptocurrency funding rates from the MEXC exchange. This application fetches historical funding rate data and stores it in a SQL database for analysis.

## Features

- **Historical Data Collection**: Fetch and store historical funding rates as far back as possible
- **Database Storage**: Store funding rates in a SQL database (supports both SQLite and PostgreSQL)
- **Regular Updates**: Schedule automatic updates to keep the database current
- **Data Analysis**: Analyze funding rate patterns to identify trends and opportunities
- **Flexible Configuration**: Configure data collection intervals, database settings, and more

## Installation

### Prerequisites

- Python 3.8 or higher
- pip (Python package manager)
- Git

### Setup

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/FundingRateAnalysis.git
   cd FundingRateAnalysis
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Configure the application by editing `config.yaml`:
   - Set your MEXC API credentials
   - Configure database settings (SQLite or PostgreSQL)
   - Adjust funding rate collection parameters

## Usage

The application can be run in different modes:

### Collect Historical Data

To collect historical funding rate data:

```
python main.py --historical --days 30
```

This will fetch funding rates for all available symbols for the past 30 days and store them in the database.

### Update with Latest Data

To update the database with the latest funding rates:

```
python main.py --update
```

### Run Analysis

To analyze funding rate patterns:

```
python main.py --analyze --days 30
```

This will analyze funding rate patterns for the past 30 days and display the results.

### Scheduled Mode

To run the application in scheduled mode, which will update the database at regular intervals:

```
python main.py --schedule --interval 60
```

This will update the database every 60 minutes.

## Configuration

The application is configured using the `config.yaml` file. Here's an explanation of the key configuration sections:

### MEXC API Configuration

```yaml
mexc:
  api_key: "your_api_key_here"
  secret_key: "your_secret_key_here"
  base_urls:
    spot: "https://api.mexc.com"
    contract: "https://contract.mexc.com"
  timeout: 10
```

### Database Configuration

```yaml
database:
  type: "sqlite"  # Options: sqlite, postgresql
  sqlite:
    db_path: "database/funding_rates.db"
  postgresql:
    host: "localhost"
    port: 5432
    database: "funding_rates"
    user: "postgres"
    password: "your_password_here"
```

### Funding Configuration

```yaml
funding:
  snapshot_window_minutes: 10
  log_interval_hours: 4
  top_n_symbols: 5
  time_windows:
    daily_days_back: 3
    hourly_hours_back: 8
    ten_min_hours_before: 2
    one_min_minutes_before: 15
    one_min_minutes_after: 15
  historical:
    days_back: 30  # How many days of historical data to fetch
```

## Database Schema

The application uses a simple database schema to store funding rate data:

### Funding Rates Table

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| symbol | TEXT | Symbol name (e.g., BTC_USDT) |
| funding_time | TIMESTAMP | Time of the funding rate payout |
| funding_rate | REAL | The funding rate value |
| funding_rate_timestamp | TIMESTAMP | Timestamp when the funding rate was recorded |
| created_at | TIMESTAMP | Timestamp when the record was created |

## Development

### Project Structure

```
FundingRateAnalysis/
├── api/                  # API client modules
│   ├── __init__.py
│   ├── base_client.py    # Base API client
│   └── contract_client.py # MEXC contract API client
├── database/             # Database modules
│   ├── __init__.py
│   └── db_manager.py     # Database connection and operations
├── pipeline/             # Data processing pipeline
│   ├── __init__.py
│   └── funding_rate_analyzer.py # Funding rate analysis logic
├── utils/                # Utility modules
│   ├── __init__.py
│   ├── config_loader.py  # Configuration loading
│   └── logger.py         # Logging utilities
├── tests/                # Test modules
│   └── __init__.py
├── logs/                 # Log files
├── config.yaml           # Configuration file
├── main.py               # Main entry point
└── README.md             # This file
```

### Adding New Features

To add new features to the application:

1. Fork the repository
2. Create a new branch for your feature
3. Implement your changes
4. Write tests for your changes
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- MEXC Exchange for providing the API
- All contributors to the project