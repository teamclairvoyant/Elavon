import json
import unittest
import csv
# needs some work
class TestRecordCount(unittest.TestCase):
    def test_record_counts(self):
        # Read the CSV file and count records
        file_path = "C:\\Users\\Prasad\\Pictures\\visa\\atoz\\part-00000-81615b71-d508-4040-a557-b181a088fb7b-c000.json"

        with open(file_path, "r") as json_file:
            data = json.load(json_file)
            json_record_count = len(data)
            print(json_record_count)

        # Read the pipe-delimited text file and count second values
        with open("C:\\Users\\Prasad\\Pictures\\visa\\atoz\\part-00000-81615b71-d508-4040-a557-b181a088fb7b-c000_qc_data.txt", "r") as txt_file_1:
            for line in txt_file_1:
                # Split the line by pipe delimiter and get the second value
                values = line.strip().split("|")

                # Extract the second value
                first_value = values[1].strip()



        # Compare record counts
        self.assertEqual(json_record_count, first_value, "Record counts do not match")


