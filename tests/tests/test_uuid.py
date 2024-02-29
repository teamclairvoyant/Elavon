import unittest
import json
from collections import Counter
from src.ConfigProcessor import get_config_details

class TestDuplicateCheck(unittest.TestCase):
    def test_column_uuid_duplicates(self):
        # Read the JSON file line by line
        folder = get_config_details()['Paths']['uuid_json']
        with open(folder, "r") as file:
            column_data = []
            for line in file:
                try:
                    data = json.loads(line)
                    column_data.append(data.get("uuid_column", None))  # Adjust column name
                except json.JSONDecodeError as e:
                    print(f"Invalid JSON line: {line}. Error: {e}")

        # Count occurrences of each element in the column data
        element_count = Counter(column_data)

        # Find duplicates
        duplicates = [element for element, count in element_count.items() if count > 1]

        # Assert that no duplicates exist
        self.assertEqual(len(duplicates), 0, f"Found duplicates: {duplicates}")

    def test_column_hash_duplicates(self):
        # Read the JSON file line by line
        folder = get_config_details()['Paths']['uuid_json']
        with open(folder, "r") as file:
            column_data = []
            for line in file:
                try:
                    data = json.loads(line)
                    column_data.append(data["Total_Revenue"])  # Adjust column name
                except json.JSONDecodeError:
                    print("Invalid JSON line:", line)

        # Count occurrences of each element in the column data
        element_count = Counter(column_data)

        # Find duplicates
        duplicates = [element for element, count in element_count.items() if count > 1]

        # Assert that no duplicates exist
        self.assertEqual(len(duplicates), 0, f"Found duplicates: {duplicates}")