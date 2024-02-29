import json

from cryptography.fernet import Fernet
from pyspark.sql import SparkSession
import pandas as pd
import os
os.environ['HADOOP_HOME'] = 'C:\\hadoop-3.3.6'


class DecryptionDriver:
    """Class for Decrypting the file"""

    def __init__(self, spark_session):
        self.spark = spark_session

    def decrypt_data(self, spark):
        try:
            # Load the encryption key
            key_file = 'mykey.key'  # Replace with the actual path
            with open(key_file, 'rb') as mykey:
                key = mykey.read()

            # Create a Fernet object with the key
            f = Fernet(key)

            # Load the encrypted data
            encrypted_file_path = self['Paths']['encrypted_file']  # Replace with the actual path
            encrypted_data = spark.read.text(encrypted_file_path).first()[0]

            # Decrypt the data
            decrypted_json = f.decrypt(encrypted_data.encode()).decode()  #str
            #print("-------------")
            #print(type(decrypted_json))
            #return decrypted_json

            #print(type(decrypted_json))

            # Load JSON using json.loads
            decrypted_data = json.loads(decrypted_json)

            return decrypted_data

            # Show the decrypted data
            #decrypted_df.show()

        except Exception as e:
            print(f"Error occurred: {str(e)}")

