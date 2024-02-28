import json
import os
from datetime import datetime

from cryptography.fernet import Fernet
import logging

class DecryptionDriver:
    """Class for Decrypting the file"""

    def __init__(self, spark_session):
        self.spark = spark_session
        self.logger = logging.getLogger(__name__)

    def decrypt_and_read_data(self):
        try:
            # Set up logging
            log_filename = os.path.join(self['Paths']['log'],
                                        f"log_decryption_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt")
            logging.basicConfig(filename=log_filename, level=logging.INFO,
                                format='%(asctime)s - %(levelname)s - %(message)s')

            key_file = self['Paths']['key_file']
            with open(key_file, 'rb') as mykey:
                key = mykey.read()

            f = Fernet(key)

            encrypted_file_path = self['Paths']['encrypted_file']
            with open(encrypted_file_path, 'rb') as encrypted_file:
                encrypted_data = encrypted_file.read()

            decrypted = f.decrypt(encrypted_data)
            decrypted_data = json.loads(decrypted)

            decrypted_output_file_path = self['Paths']['decrypted_output_file']
            with open(decrypted_output_file_path, 'w') as decrypted_output_file:
                json.dump(decrypted_data, decrypted_output_file)

            logging.info("Decrypted data saved successfully.")

        except Exception as e:
            logging.error(f"Error during decryption: {str(e)}")

# Example usage:
# Assuming you have a SparkSession created, you can instantiate DecryptionDriver like this:
# decryption_driver = DecryptionDriver(spark_session)
# get_config_details = {'Paths': {'log': 'logs', 'key_file': 'key.key', 'encrypted_file': 'encrypted_data.bin', 'decrypted_output_file': 'decrypted_data.json'}}
# decryption_driver.decrypt_and_read_data(get_config_details)
