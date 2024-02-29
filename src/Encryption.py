from cryptography.fernet import Fernet
import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession

class EncryptionDriver:
    """
    Class for encrypting data using Fernet encryption.

    Parameters:
        spark_session (pyspark.sql.SparkSession): The Spark session object.

    Attributes:
        spark (pyspark.sql.SparkSession): The Spark session object.


    """

    def __init__(self, spark_session):
        """
        Initializes the EncryptionDriver with the provided Spark session.

        Parameters:
            spark_session (pyspark.sql.SparkSession): The Spark session object.
        """
        self.spark = spark_session

    def encrypt_data(conf,spark):
        """
        Encrypts data from a CSV file using Fernet encryption and saves the encrypted data to a file.

        Parameters:
            config_details (dict): A dictionary containing file paths and configurations.

        Raises:
            Exception: If an error occurs during the encryption process.
        """
        try:
            # Generate or load encryption key
            key_file = conf['Paths']['key_file']
            if not os.path.exists(key_file):
                key = Fernet.generate_key()
                with open(key_file, 'wb') as mykey:
                    mykey.write(key)
            else:
                with open(key_file, 'rb') as mykey:
                    key = mykey.read()

            # Create a Fernet object with the key
            f = Fernet(key)

            # Read CSV file using Spark
            csv_file = conf['Paths']['csv_file']
            original_df = spark.read.format("csv").option("header", "true").load(csv_file)

            # Convert Spark DataFrame to Pandas DataFrame
            original_pandas = original_df.toPandas()

            # Convert Pandas DataFrame to JSON
            original_json = original_pandas.to_json(orient='records')

            # Encrypt the JSON data
            encrypted = f.encrypt(original_json.encode())

            # Save the encrypted data to a file
            encrypted_file_path = conf['Paths']['encrypted_file']
            with open(encrypted_file_path, 'wb') as encrypted_file:
                encrypted_file.write(encrypted)

            # Log the information
            logging.info(f"Encrypted data saved successfully at: {encrypted_file_path}")

        except Exception as e:
            logging.error(f"Error occurred: {str(e)}")
