# Deployment Guide for Funding Rate Analysis

This guide provides detailed instructions for deploying the Funding Rate Analysis application in different environments.

## Table of Contents

1. [Local Development Deployment](#local-development-deployment)
2. [Production Deployment](#production-deployment)
   - [Using SQLite](#using-sqlite)
   - [Using PostgreSQL](#using-postgresql)
3. [Docker Deployment](#docker-deployment)
4. [Scheduled Execution](#scheduled-execution)
5. [Monitoring and Maintenance](#monitoring-and-maintenance)

## Local Development Deployment

For local development and testing, follow these steps:

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/FundingRateAnalysis.git
   cd FundingRateAnalysis
   ```

2. Create a virtual environment:
   ```
   python -m venv venv
   ```

3. Activate the virtual environment:
   - Windows: `venv\Scripts\activate`
   - Linux/Mac: `source venv/bin/activate`

4. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

5. Configure the application:
   - Copy `config.yaml.example` to `config.yaml` (if not already present)
   - Edit `config.yaml` to set your MEXC API credentials
   - Configure database settings (SQLite is recommended for development)

6. Run the application:
   ```
   python main.py --historical --days 7
   ```

## Production Deployment

### Using SQLite

SQLite is suitable for smaller deployments or when you don't want to manage a separate database server.

1. Follow steps 1-5 from the Local Development Deployment section.

2. Configure the database section in `config.yaml`:
   ```yaml
   database:
     type: "sqlite"
     sqlite:
       db_path: "/path/to/your/data/funding_rates.db"
   ```

3. Set up a systemd service (Linux) or Task Scheduler (Windows) to run the application in scheduled mode:
   ```
   python main.py --schedule --interval 60
   ```

### Using PostgreSQL

PostgreSQL is recommended for production deployments with larger datasets or when multiple applications need to access the data.

1. Install PostgreSQL:
   - Ubuntu: `sudo apt-get install postgresql postgresql-contrib`
   - CentOS/RHEL: `sudo yum install postgresql-server postgresql-contrib`
   - Windows: Download and install from the [PostgreSQL website](https://www.postgresql.org/download/windows/)

2. Create a database and user:
   ```sql
   CREATE DATABASE funding_rates;
   CREATE USER funding_user WITH ENCRYPTED PASSWORD 'your_secure_password';
   GRANT ALL PRIVILEGES ON DATABASE funding_rates TO funding_user;
   ```

3. Configure the database section in `config.yaml`:
   ```yaml
   database:
     type: "postgresql"
     postgresql:
       host: "localhost"
       port: 5432
       database: "funding_rates"
       user: "funding_user"
       password: "your_secure_password"
   ```

4. Follow steps 1, 2, 3, and 4 from the Local Development Deployment section.

5. Set up a systemd service (Linux) or Task Scheduler (Windows) to run the application in scheduled mode.

## Docker Deployment

For containerized deployment using Docker:

1. Create a Dockerfile in the project root:
   ```dockerfile
   FROM python:3.9-slim

   WORKDIR /app

   COPY requirements.txt .
   RUN pip install --no-cache-dir -r requirements.txt

   COPY . .

   CMD ["python", "main.py", "--schedule", "--interval", "60"]
   ```

2. Create a docker-compose.yml file for easier management:
   ```yaml
   version: '3'

   services:
     app:
       build: .
       volumes:
         - ./config.yaml:/app/config.yaml
         - ./database:/app/database
         - ./logs:/app/logs
       restart: always

     # Optional: PostgreSQL service
     db:
       image: postgres:13
       environment:
         POSTGRES_DB: funding_rates
         POSTGRES_USER: funding_user
         POSTGRES_PASSWORD: your_secure_password
       volumes:
         - postgres_data:/var/lib/postgresql/data
       restart: always

   volumes:
     postgres_data:
   ```

3. Build and start the containers:
   ```
   docker-compose up -d
   ```

## Scheduled Execution

For scheduled execution without using the built-in scheduler:

### Linux (Cron)

1. Create a shell script to run the application:
   ```bash
   #!/bin/bash
   cd /path/to/FundingRateAnalysis
   source venv/bin/activate
   python main.py --update
   ```

2. Make the script executable:
   ```
   chmod +x /path/to/run_funding_analysis.sh
   ```

3. Add a cron job to run the script every hour:
   ```
   crontab -e
   ```

4. Add the following line:
   ```
   0 * * * * /path/to/run_funding_analysis.sh >> /path/to/FundingRateAnalysis/logs/cron.log 2>&1
   ```

### Windows (Task Scheduler)

1. Create a batch script to run the application:
   ```batch
   @echo off
   cd /d C:\path\to\FundingRateAnalysis
   call venv\Scripts\activate.bat
   python main.py --update
   ```

2. Open Task Scheduler and create a new task:
   - Trigger: Daily, recur every 1 hour
   - Action: Start a program
   - Program/script: `C:\path\to\run_funding_analysis.bat`

## Monitoring and Maintenance

### Log Monitoring

The application logs are stored in the `logs` directory. You can use tools like `tail` or log monitoring software to keep track of the application's operation:

```
tail -f logs/app_YYYYMMDD.log
```

### Database Maintenance

For SQLite:
- Periodically run `VACUUM` to optimize the database:
  ```sql
  VACUUM;
  ```

For PostgreSQL:
- Set up regular VACUUM and ANALYZE operations:
  ```sql
  VACUUM ANALYZE funding_rates;
  ```

### Backup Strategy

For SQLite:
- Create a cron job or scheduled task to copy the database file to a backup location:
  ```bash
  cp /path/to/funding_rates.db /path/to/backups/funding_rates_$(date +%Y%m%d).db
  ```

For PostgreSQL:
- Use pg_dump to create backups:
  ```bash
  pg_dump -U funding_user -d funding_rates > /path/to/backups/funding_rates_$(date +%Y%m%d).sql
  ```

### Updating the Application

To update the application:

1. Stop the running instance
2. Pull the latest changes:
   ```
   git pull origin main
   ```
3. Update dependencies:
   ```
   pip install -r requirements.txt
   ```
4. Restart the application

For Docker deployments:
```
docker-compose pull
docker-compose up -d --build
```