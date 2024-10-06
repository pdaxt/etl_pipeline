# ETL Pipeline Project

This project demonstrates a simple ETL (Extract, Transform, Load) pipeline built in Python, which reads data from a CSV file, transforms it, and loads it into a MongoDB database.

## Table of Contents
- Project Overview
- Prerequisites
- Project Structure
- Installation
- Usage
- Testing
- Logging
- License

## Project Overview
This project implements an ETL pipeline with the following steps:
1. **Extract**: Read data from a CSV file containing member information.
2. **Transform**: Clean and transform the data by standardizing dates, formatting salaries, and calculating age.
3. **Load**: Load the transformed data into a MongoDB database.

## Prerequisites
Ensure you have the following software installed:
- Python 3.8 or higher
- MongoDB
- Docker (optional for containerization)

## Project Structure
```
etl_pipeline/
    ├── data/
    │   └── member-data.csv        # Input data file
    ├── src/
    │   └── etl.py                # Main ETL script
    ├── tests/
    │   └── test_etl.py           # Unit tests for the ETL functions
    ├── Dockerfile                # Docker configuration
    ├── docker-compose.yml        # Docker Compose setup for MongoDB
    ├── requirements.txt          # Python dependencies
    └── README.md                 # Project documentation
```

## Installation
1. Clone the repository:
```bash
git clone https://github.com/your-repo/etl_pipeline.git
cd etl_pipeline
```

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

3. If using Docker, build and start the MongoDB container:
```bash
docker-compose up --build
```

## Usage
To run the ETL pipeline, execute the following command:
```bash
python src/etl.py
```

This will:
1. Extract data from `data/member-data.csv`.
2. Transform the data by cleaning, formatting, and adding new fields.
3. Load the data into a MongoDB instance.

## Testing
Run the unit tests with:
```bash
python -m unittest discover tests
```

## Logging
Logs are generated during the ETL process and can be found in the console output. To modify log levels or formats, adjust the `logging` configuration in the `src/etl.py` file.

## License
This project is licensed under the MIT License.
