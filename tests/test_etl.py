# tests/test_etl.py

import unittest
import os
import sys
import logging

# Configure logging for the test module
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Add the parent directory to sys.path to import src.etl
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.etl import read_data, transform_data, load_data
from unittest.mock import patch, MagicMock

class TestETL(unittest.TestCase):
    """
    Test suite for the ETL process.
    """

    def test_read_data(self):
        """
        Test the read_data function to ensure it reads and parses data correctly.
        """
        # Get the directory of the current script
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Construct the absolute path to the data file
        file_path = os.path.join(current_dir, '..', 'data', 'member-data.csv')
        # Normalize the path
        file_path = os.path.normpath(file_path)

        logger.info(f"Attempting to read data from: {file_path}")

        data = read_data(file_path)

        # Check that data is a list
        self.assertIsInstance(data, list, "Data should be a list")

        # Check that the list is not empty
        self.assertGreater(len(data), 0, "Data list should not be empty")

        # Check that each item is a dictionary
        self.assertIsInstance(data[0], dict, "Each data item should be a dictionary")

        # Check that expected keys are present
        expected_keys = {
            'FirstName', 'LastName', 'Company', 'BirthDate', 'Salary',
            'Address', 'Suburb', 'State', 'Post', 'Phone', 'Mobile', 'Email'
        }
        self.assertEqual(set(data[0].keys()), expected_keys, "Data keys do not match expected keys")

    def test_transform_data(self):
        """
        Test the transform_data function to ensure data is transformed correctly.
        """
        raw_data = [{
            'FirstName': ' John ',
            'LastName': 'Doe ',
            'Company': 'Example Corp',
            'BirthDate': '15011980',
            'Salary': '75000.00',
            'Address': '123 Main St',
            'Suburb': 'Anytown',
            'State': 'NSW',
            'Post': '2000',
            'Phone': '0123456789',
            'Mobile': '0987654321',
            'Email': 'john.doe@example.com'
        }]

        transformed = transform_data(raw_data)
        self.assertEqual(len(transformed), 1, "Transformed data should contain one record")
        record = transformed[0]

        # Check FullName
        self.assertEqual(record['FullName'], 'John Doe', "FullName is not formatted correctly")

        # Check BirthDate format
        self.assertEqual(record['BirthDate'], '15/01/1980', "BirthDate is not formatted correctly")

        # Check Age (assuming reference date is 2024-03-01)
        self.assertEqual(record['Age'], 44, "Age is not calculated correctly")

        # Check Salary formatting
        self.assertEqual(record['Salary'], '$75,000.00', "Salary is not formatted correctly")

        # Check SalaryBucket
        self.assertEqual(record['SalaryBucket'], 'B', "SalaryBucket is not assigned correctly")

        # Ensure FirstName and LastName are not present in the transformed record
        self.assertNotIn('FirstName', record, "FirstName should not be in transformed record")
        self.assertNotIn('LastName', record, "LastName should not be in transformed record")

        # Check nested Address
        expected_address = {
            'Street': '123 Main St',
            'Suburb': 'Anytown',
            'State': 'NSW',
            'Post': '2000'
        }
        self.assertEqual(record['Address'], expected_address, "Address is not nested correctly")

    @patch('src.etl.MongoClient')
    @patch.dict(os.environ, {"DOCKER_ENV": "true"})
    def test_load_data(self, mock_mongo_client):
        """
        Test the load_data function to ensure data is loaded into MongoDB correctly.
        """
        # Arrange
        transformed_data = [{
            'FullName': 'John Doe',
            'Company': 'Example Corp',
            'BirthDate': '15/01/1980',
            'Age': 44,
            'Salary': '$75,000.00',
            'SalaryBucket': 'B',
            'Address': {
                'Street': '123 Main St',
                'Suburb': 'Anytown',
                'State': 'NSW',
                'Post': '2000'
            },
            'Phone': '0123456789',
            'Mobile': '0987654321',
            'Email': 'john.doe@example.com'
        }]

        # Set up the mock MongoClient instance
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance

        # Mock the database and collection
        mock_db = MagicMock()
        mock_client_instance.__getitem__.return_value = mock_db  # For client['etl_database']

        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection  # For db['employees']

        # Act
        load_data(transformed_data)

        # Assert
        mock_collection.insert_many.assert_called_once_with(transformed_data)
        mock_client_instance.close.assert_called_once()

if __name__ == '__main__':
    unittest.main()
