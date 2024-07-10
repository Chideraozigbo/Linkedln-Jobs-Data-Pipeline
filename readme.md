# LinkedIn Data Scraper ETL Project
This project automates the extraction of job listings data from LinkedIn, transforms it into a clean and structured format, and stores it in a MySQL database.

## Table of Contents
1. [Overview](#overview)
2. [Setup](#setup)
3. [Usage](#usage)
4. [Configuration](#configuration)
5. [Dependencies](#dependencies)
6. [Contributing](#contributing)

## Overview
This project aims to gather job listings data from LinkedIn using an API, clean and transform the data, and then store it into a MySQL database for further analysis and reporting. The workflow includes extraction, transformation, and loading (ETL) processes, all managed through Python scripts.

## Setup
1. Clone the repository:

```bash
git clone https://github.com/Chideraozigbo/Linkedln-Jobs-Data-Pipeline.git
cd linkedin-data-scraper
```
2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage
To run the ETL process:

```bash
python pipeline.py
```
Make sure to update the header.json file with your LinkedIn API key before running.

## Configuration
Ensure you have the following configurations set up before running the project:

- MySQL Database:
  - Create a MySQL database and note down the host, user, password, and database details.
  - Store these details in a `.env` file in the project root directory:
```bash
host=your_db_host
user=your_db_user
password=your_db_password
database=your_db_name
```
## Dependencies
- Python 3
- pandas
- requests
- mysql-connector-python
- python-dotenv
- sqlalchemy

## Contributing
Feel free to contribute to this project. Fork it, make your changes, and submit a pull request.
