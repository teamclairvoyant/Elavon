import json
import time
import requests
from pyspark.sql.functions import col


# Define the hashing function
class HashingDriver:
    """Class for Encrypting the file"""

    def __init__(self, spark_session):
        self.spark = spark_session

    @staticmethod
    def hashing(self, spark):
        def send_data_and_get_hash(data_batch, columns_to_hash):
            start_time = time.time()
            api_url = 'http://localhost:5000/calculate_hash'
            payload = {'data_batch': data_batch, 'columns_to_hash': columns_to_hash}
            response = requests.post(api_url, json=payload)
            result = response.json()
            end_time = time.time()
            execution_time = end_time - start_time
            print(f"Execution time: {execution_time} seconds")
            if 'hash_value' in result:
                return result['hash_value']
            elif 'error' in result:
                raise Exception(result['error'])
            else:
                raise Exception('Unexpected response from the server')

        # Read JSON data into DataFrame
        json_file_path = self['Paths']['decrypted_output_file']
        print(json_file_path)
        df = spark.read.option("multiline", "true").json(json_file_path)
        print(df)
        df.show()

        # Define batch size
        batch_size = 20000

        # Calculate the number of batches
        num_batches = (df.count() // batch_size) + (1 if df.count() % batch_size != 0 else 0)
        print(num_batches)

        # Specify columns to hash
        # should come from config
        columns_to_hash = ['Total_Revenue']

        # Process records in batches
        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, df.count())
            print(end_idx)

            batch = df.filter((col("ID") >= start_idx) & (col("ID") < end_idx))

            # Convert batch DataFrame to a list of dictionaries
            data_batch = [{column: record[column] for column in columns_to_hash + ['ID']} for record in
                          batch.collect()]

            # Call the hashing function
            hashed_values = send_data_and_get_hash(data_batch, columns_to_hash)

            # Create a DataFrame from hashed values
            sampledf = spark.createDataFrame(hashed_values)

            # Dropping the columns from the actual df which are sent for hashing
            for column in columns_to_hash:
                df = df.drop(column)

            # Join the hashed values DataFrame with the original DataFrame
            joined_df = df.join(sampledf, "ID", "inner")
            print(joined_df)
            print(type(joined_df))
            return joined_df
            #return joined_df