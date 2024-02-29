import hashlib
import os
import logging
from datetime import datetime


class QualityCheck:
    """Class for performing quality checks on decrypted data."""

    def __init__(self, spark_session):
        """
         Initialize the QualityCheck object.

         Parameters:
         - spark_session (SparkSession): The Spark session object.
        """
        self.spark = spark_session

    def perform_qc(self, spark):
        """
                Perform quality checks on decrypted data.

                Parameters:
                - spark (SparkSession): The Spark session object.
        """
        try:
            # Read decrypted data from JSON and write it to the QC directory
            uuid_data = spark.read.json(self['Paths']['uuid_output_path'])
            uuid_data.write.mode("append").json(self['Paths']['qc'])

            # Function to calculate MD5 hash for a file
            def calculate_md5(file_path):
                """
                    Calculate MD5 hash for a given file.

                    Parameters:
                    - file_path (str): The path to the file.

                     Returns:
                    - md5_hash (str): The MD5 hash of the file.
                    - file_size (int): The size of the file in bytes.
                """
                with open(file_path, 'rb') as file_to_check:
                    data = file_to_check.read()
                    count = len(data)
                    md5_returned = hashlib.md5(data).hexdigest()
                    return md5_returned, count

            # Filter only .json files from the directory
            json_files = [i for i in os.listdir(self['Paths']['uuid_output_path']) if i.endswith('.json')]

            # Iterate through each .json file, calculate MD5, and save QC data
            for file_name in json_files:
                file_path = os.path.join(self['Paths']['uuid_output_path'], file_name)
                md5_hash, file_size = calculate_md5(file_path)
                qc_data = f"{file_name} | {file_size} | {md5_hash}"
                new_file_name = f"{file_name.split('.')[0]}_qc_data.txt"
                new_file_path = os.path.join(self['Paths']['qc'], new_file_name)

                # Writing the QC data to a separate file
                with open(new_file_path, 'w') as qc_file:
                    qc_file.write(qc_data + '\n')

                # Log the location of qc_data.txt
                logging.info(f"QC data saved at: {new_file_path}")

        except Exception as e:
            # Handle any exception and log the error
            logging.error(f"An error occurred: {str(e)}")


# Example usage:
# spark_session = ...  # Initialize your Spark session
# qc = QualityCheck(spark_session)
# qc.perform_qc(get_config_details, spark_session)
