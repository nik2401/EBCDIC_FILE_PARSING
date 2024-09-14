
# EBCDIC FILE PARSING


## Overview

This project comprises a suite of Python scripts designed to parse and process EBCDIC FILE . It automates the ingestion, validation, and processing of financial transaction data, facilitating streamlined reconciliation and settlement processes.

## Features
- **File Parsing**: Extracts and processes transaction data from AMEX clearing files.
- **Database Interaction**: Inserts processed data into SQL databases for further analysis and reporting.
- **Error Handling**: Implements robust error logging and handling mechanisms to ensure data integrity and facilitate troubleshooting.
- **Email Notifications**: Sends automated email notifications on process milestones or in case of errors.
- **Multiprocessing Support**: Leverages Python's multiprocessing capabilities for efficient data processing.
- **Stored Procedure Integration**: Calls SQL stored procedures for specific data manipulation tasks.

## Prerequisites

- Python 3.6 or newer
- pyodbc
- pandas
- numpy
- smtplib (for email notifications)
- sqlalchemy (optional, for certain database interactions)
- A SQL Server database setup to receive the processed data

## Setup

1. Clone this repository to your local machine.
2. Ensure that Python 3.6+ is installed.
3. Install the required Python packages:

```bash
pip install pyodbc pandas numpy
```

4. Update the database connection strings in `SQL_Connections.py` to point to your SQL Server database.
5. Configure the email settings in `Mail.py` to enable email notifications.

## Usage

1. Place the AMEX clearing files in the designated input directory as specified in `SetUp.py`.
2. Run `AMEXClr_DownLoader.py` to initiate the processing of clearing files:

```bash
python AMEXClr_DownLoader.py
```

3. Monitor the process logs for any errors or confirmation of successful execution.

## Structure

- `AMEXClr_DownLoader.py`: Orchestrates the downloading and initial handling of AMEX clearing files.
- `AMEX_Select_And_Updates.py`: Handles selection and update operations on the database.
- `AMEX_SPCall.py`: Manages calls to stored procedures for data processing.
- `Functions.py`: Contains utility functions used across the project.
- `Logger.py`: Implements logging functionality.
- `Mail.py`: Manages sending of email notifications.
- `MultiProcess.py`: Implements multiprocessing for efficient data processing.
- `SQL_Connections.py`: Manages database connections and queries.

## Contributing

Contributions are welcome. Please open an issue or submit a pull request with your proposed changes or improvements.

