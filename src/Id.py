from pyspark.sql.functions import *
import os
import logging
import uuid  # Import the uuid library

from pyspark.sql import SparkSession


# Assuming you have already created a SparkSession
# spark = SparkSession.builder.appName("YourAppName").getOrCreate()

class IdDriver:
    """Class for unique id the file"""

    def __init__(self, spark_session):
        self.spark = spark_session

    def process_data_uuid(self, hashed_values):
        try:
            # Generate a new column with unique identifiers using uuid.uuid4()
            #decrypted_df = spark.createDataFrame(decrypted)
            df_with_uuid = hashed_values.withColumn("uuid_column",
                                                    sha2(concat(col(self['Paths']['uuid_column']),
                                                                lit(str(uuid.uuid4()))), 256))

            # Save the DataFrame to a local file path in JSON format using Spark's write operation
            output_path = os.path.join(self['Paths']['uuid_output_path'])

            # You can change the file format and options according to your requirement
            df_with_uuid.coalesce(2).write.mode("overwrite").json(output_path)

            logging.info(f"DataFrame with UUID saved to: {output_path}")

        except Exception as e:
            logging.error(f"Error during data processing: {str(e)}")

# Example usage:
# paths_config should contain the 'output' key for specifying the output path
# id_driver = IdDriver(spark_session=spark, paths_config={'output': '/your/local/output/path'})

# Assuming 'hashed_values' is your input DataFrame
# id_driver.process_data_uuid(hashed_values)