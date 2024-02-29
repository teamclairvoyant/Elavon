from pyspark.sql.functions import *
import os
import logging
import uuid  # Import the uuid library

from pyspark.sql import SparkSession


# Assuming you have already created a SparkSession
# spark = SparkSession.builder.appName("YourAppName").getOrCreate()

class IdDriver:
    """Class for unique id generation and DataFrame processing."""

    def __init__(self, spark_session):
        self.spark = spark_session
        """
                Initialize IdDriver with a SparkSession.

                :param spark_session: The SparkSession instance.
        """

    def process_data_uuid(conf, hashed_values):
        """
                Process DataFrame by adding a UUID column and save it to a local file in JSON format.

                :param conf: Configuration dictionary containing 'Paths' with 'uuid_column' and 'uuid_output_path'.
                :param hashed_values: Input DataFrame.
        """
        try:
            # Generate a new column with unique identifiers using uuid.uuid4()
            #decrypted_df = spark.createDataFrame(decrypted)
            df_with_uuid = hashed_values.withColumn("uuid_column",
                                                    sha2(concat(col(conf['Paths']['uuid_column']),
                                                                lit(str(uuid.uuid4()))), 256))

            # Save the DataFrame to a local file path in JSON format using Spark's write operation
            output_path = os.path.join(conf['Paths']['uuid_output_path'])

            # You can change the file format and options according to your requirement
            df_with_uuid.coalesce(2).write.mode("overwrite").json(output_path)

            logging.info(f"DataFrame with UUID saved to: {output_path}")

        except Exception as e:
            logging.error(f"Error during data processing: {str(e)}")
