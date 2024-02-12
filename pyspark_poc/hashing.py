import json
import hashlib
import os
import pandas as pd
import configparser


def hash_column(input_file, output_file, column_names):
    with open(input_file, 'r') as f:
        data = json.load(f)

    for column_name in column_names:
        if column_name not in data[0]:
            print(f"Column '{column_name}' does not exist in the data.")
            return

    for row in data:
        for column_name in column_names:
            value = row[column_name]
            hashed_value = hashlib.sha256(value.encode()).hexdigest()
            row[column_name] = hashed_value

    with open(output_file, 'w') as f:
        json.dump(data, f)

    print(f"Hashed data saved in {output_file}")


# Read paths from config.ini
config = configparser.ConfigParser()
config.read('config.ini')

# Path to the input JSON file
input_file = config['Paths']['decrypted_output_file']

# Check if the input file exists
if not os.path.exists(input_file):
    print("Input file does not exist.")
else:
    # User input for the column(s) to hash
    columns_input = config['Paths']['column_names']
    column_names = [col.strip() for col in columns_input.split(',')]

    # Path to save the hashed JSON file
    output_file = config['Paths']['hashed_output_file']

    # Apply hashing and save the result
    hash_column(input_file, output_file, column_names)

# Read the hashed JSON file into a DataFrame
df = pd.read_json(output_file)
print(df)

# Optional: Print the DataFrame contents as tuples
x = [tuple(i) for i in df.values]
for i in x:
    print(i)