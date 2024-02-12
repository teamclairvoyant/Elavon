from cryptography.fernet import Fernet

from pyspark.sql import SparkSession
import os
import configparser

config = configparser.ConfigParser()
config.read('config.ini')


os.environ['HADOOP_HOME'] = 'C:\\hadoop-3.3.6'


spark = SparkSession.builder.appName("EncryptAndSave").getOrCreate()

key_file = config['Paths']['key_file']
if not os.path.exists(key_file):
    key = Fernet.generate_key()
    with open(key_file, 'wb') as mykey:
        mykey.write(key)
else:
    with open(key_file, 'rb') as mykey:
        key = mykey.read()

f = Fernet(key)

csv_file = config['Paths']['csv_file']
original_df = spark.read.format("csv").option("header", "true").load(csv_file)

original_pandas = original_df.toPandas()

original_json = original_pandas.to_json(orient='records')

# Encrypt the data
encrypted = f.encrypt(original_json.encode())

# Save the encrypted data to a file
encrypted_file = config['Paths']['encrypted_file']
with open(encrypted_file, 'wb') as encrypted_file:
    encrypted_file.write(encrypted)

print("Encrypted data saved successfully.")