import logging
import json
from cryptography.fernet import Fernet


class DecryptionDriver:
    """Class for Decrypting and Reading Encrypted Data"""

    def __init__(self, spark_session):
        """
        Initialize the DecryptionDriver.
        Parameters:
            - spark_session (pyspark.sql.SparkSession): The Spark session object.
        """
        self.spark = spark_session

    def decrypt_and_read_data(self, conf):
        """
        Decrypts the encrypted data and saves it to a JSON file.

        Parameters:
            - conf (dict): Configuration dictionary containing file paths and settings.

        Raises:
            - Exception: Any exception raised during the decryption process.

        Returns:
            None
        """
        try:
            key_file = conf['Paths']['key_file']
            with open(key_file, 'rb') as mykey:
                key = mykey.read()

            f = Fernet(key)

            encrypted_file_path = conf['Paths']['encrypted_file']
            with open(encrypted_file_path, 'rb') as encrypted_file:
                encrypted_data = encrypted_file.read()

            decrypted = f.decrypt(encrypted_data)
            decrypted_data = json.loads(decrypted)

            decrypted_output_file_path = conf['Paths']['decrypted_output_file']
            with open(decrypted_output_file_path, 'w') as decrypted_output_file:
                json.dump(decrypted_data, decrypted_output_file)

            logging.info("Decrypted data saved successfully.")

        except Exception as e:
            logging.error(f"Error during decryption: {str(e)}")
