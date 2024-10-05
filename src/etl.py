# src/etl.py

import os
import socket
import logging
from datetime import datetime
from pymongo import MongoClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def read_data(file_path):
    """
    Reads data from a pipe-delimited file and returns a list of records.

    Args:
        file_path (str): The path to the data file.

    Returns:
        List[Dict[str, Any]]: A list of records as dictionaries.
    """
    data = []
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                fields = line.split('|')

                # Skip lines that don't have exactly 12 fields
                if len(fields) != 12:
                    logger.warning(f"Skipping line due to incorrect number of fields: {line}")
                    continue

                record = {
                    'FirstName': fields[0].strip(),
                    'LastName': fields[1].strip(),
                    'Company': fields[2].strip().strip('"'),
                    'BirthDate': fields[3].strip(),
                    'Salary': fields[4].strip(),
                    'Address': fields[5].strip(),
                    'Suburb': fields[6].strip(),
                    'State': fields[7].strip(),
                    'Post': fields[8].strip(),
                    'Phone': fields[9].strip(),
                    'Mobile': fields[10].strip(),
                    'Email': fields[11].strip()
                }
                data.append(record)
        logger.info(f"Extracted {len(data)} records from {file_path}.")
        return data
    except Exception as e:
        logger.error(f"Failed to read data from {file_path}: {e}")
        raise

def transform_data(data):
    """
    Transforms raw data records into a structured format suitable for loading into MongoDB.

    Args:
        data (List[Dict[str, Any]]): The raw data records.

    Returns:
        List[Dict[str, Any]]: The transformed data records.
    """
    transformed_data = []
    skipped_records = 0
    reference_date = datetime(2024, 3, 1)

    for record in data:
        try:
            # Clean and combine names
            first_name = record['FirstName'].strip()
            last_name = record['LastName'].strip()
            full_name = f"{first_name} {last_name}"

            # Process BirthDate
            birth_date_str_raw = record['BirthDate'].strip()
            if not birth_date_str_raw or len(birth_date_str_raw) < 6:
                logger.warning(f"Skipping record due to invalid BirthDate format: '{birth_date_str_raw}' for {full_name}")
                skipped_records += 1
                continue

            birth_date_str = birth_date_str_raw.zfill(8)
            birth_date = datetime.strptime(birth_date_str, '%d%m%Y')
            birth_date_formatted = birth_date.strftime('%d/%m/%Y')

            # Calculate Age
            age = reference_date.year - birth_date.year - (
                (reference_date.month, reference_date.day) < (birth_date.month, birth_date.day)
            )

            # Format Salary
            salary_float = float(record['Salary'])
            salary_formatted = "${:,.2f}".format(salary_float)

            # Determine SalaryBucket
            if salary_float < 50000:
                salary_bucket = 'A'
            elif 50000 <= salary_float <= 100000:
                salary_bucket = 'B'
            else:
                salary_bucket = 'C'

            # Create nested Address
            address = {
                'Street': record['Address'],
                'Suburb': record['Suburb'],
                'State': record['State'],
                'Post': record['Post']
            }

            # Build transformed record
            transformed_record = {
                'FullName': full_name,
                'Company': record['Company'],
                'BirthDate': birth_date_formatted,
                'Age': age,
                'Salary': salary_formatted,
                'SalaryBucket': salary_bucket,
                'Address': address,
                'Phone': record['Phone'],
                'Mobile': record['Mobile'],
                'Email': record['Email']
            }

            transformed_data.append(transformed_record)
        except ValueError as e:
            logger.warning(f"Skipping record due to date parsing error: '{birth_date_str_raw}' for {full_name}. Error: {e}")
            skipped_records += 1
            continue
        except Exception as e:
            logger.error(f"Unexpected error while transforming record for {full_name}: {e}")
            skipped_records += 1
            continue

    logger.info(f"Transformed {len(transformed_data)} records.")
    logger.info(f"Skipped {skipped_records} records due to invalid or missing BirthDate.")
    return transformed_data

def load_data(data):
    """
    Loads transformed data into a MongoDB collection.

    Args:
        data (List[Dict[str, Any]]): The transformed data records.
    """
    try:
        # Determine the MongoDB host
        try:
            socket.gethostbyname('mongo')
            mongo_host = 'mongo'  # Inside Docker
        except socket.gaierror:
            mongo_host = 'localhost'  # On Host Machine

        client = MongoClient(f'mongodb://{mongo_host}:27017/')
        db = client['etl_database']
        collection = db['employees']
        collection.insert_many(data)
        client.close()
        logger.info("Data loaded into MongoDB successfully.")
    except Exception as e:
        logger.error(f"An error occurred while loading data into MongoDB: {e}")
        raise

def main():
    """
    The main function orchestrates the ETL process.
    """
    file_path = 'data/member-data.csv'
    logger.info("Starting ETL process...")
    try:
        raw_data = read_data(file_path)
        transformed_data = transform_data(raw_data)
        load_data(transformed_data)
        logger.info("ETL process completed successfully.")
    except Exception as e:
        logger.error(f"ETL process failed: {e}")

if __name__ == '__main__':
    main()
