from cryptography.fernet import Fernet
import json
from pyspark.sql import SparkSession
import os
import configparser

# Load configurations from config.ini
config = configparser.ConfigParser()
config.read('config.ini')

# Set HADOOP_HOME
os.environ['HADOOP_HOME'] = 'C:\\hadoop-3.3.6'

# Create a Spark session
spark = SparkSession.builder.appName("DecryptAndRead").getOrCreate()

# Load the encryption key from the file
key_file = config['Paths']['key_file']
with open(key_file, 'rb') as mykey:
    key = mykey.read()

# Create Fernet object with the key
f = Fernet(key)

# Load the encrypted data from the JSON file
encrypted_file = config['Paths']['encrypted_file']
with open(encrypted_file, 'rb') as encrypted_file:
    encrypted_data = encrypted_file.read()

# Decrypt the data
decrypted = f.decrypt(encrypted_data)

# Convert decrypted data to a list of dictionaries
decrypted_data = json.loads(decrypted)

# Define the path for saving decrypted data
decrypted_output_file = config['Paths']['decrypted_output_file']

# Save decrypted data to a JSON file
with open(decrypted_output_file, 'w') as decrypted_output_file:
    json.dump(decrypted_data, decrypted_output_file)

print("Decrypted data saved successfully.")
