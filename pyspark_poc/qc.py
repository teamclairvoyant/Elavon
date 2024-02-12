import hashlib
import os
import configparser

# Load configuration from config.ini
config = configparser.ConfigParser()
config.read('config.ini')

# Get the directory path from config.ini
directory_path = config['DEFAULT']['DirectoryPath']


# Function to calculate MD5 hash for a file
def calculate_md5(file_path):
    with open(file_path, 'rb') as file_to_check:
        data = file_to_check.read()
        count = len(data)
        print(count)
        md5_returned = hashlib.md5(data).hexdigest()
        return md5_returned, count


# Filter only .json files from the directory
json_files = [i for i in os.listdir(directory_path) if i.endswith('.json')]

# Iterate through each .json file, calculate MD5, and save QC data
for file_name in json_files:
    file_path = os.path.join(directory_path, file_name)
    md5_hash, file_size = calculate_md5(file_path)
    qc_data = f"{file_name} | {file_size} | {md5_hash}"

    # Writing the QC data to a file
    with open(config['DEFAULT']['OutputFilePath'], 'a') as qc_file:
        qc_file.write(qc_data + '\n')
