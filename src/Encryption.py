from cryptography.fernet import Fernet
import os
import logging
from datetime import datetime


class EncryptionDriver:
    """Class for Encrypting the file"""

    def __init__(self):
        pass
    def encrypt_data(self, conf,spark):
        """
        sdjsdjsbfksnkdsn
        :param spark:
        :return:
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


# Example usage:
# spark_session = ...  # Provide the actual Spark session
# get_config_details = {'Paths': {'log': 'logs', 'key_file': 'key.key', 'csv_file': 'data.csv', 'encrypted_file': 'encrypted_data.bin'}}
# encryption_driver = EncryptionDriver(spark_session)
# encryption_driver.encrypt_data(get_config_details)

# Example usage:
# encryption_driver = EncryptionDriver(spark_session)
# encryption_driver.encrypt_data(get_config_details)