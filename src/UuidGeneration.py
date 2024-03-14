from pyspark.sql.functions import sha2, concat, col, lit
import os
import logging
import uuid

class IdDriver:
    """
    Class for unique ID generation and DataFrame processing.
    """

    def __init__(self, spark_session):
        """
        Initialize IdDriver with a SparkSession.

        :param spark_session: The SparkSession instance.
        """
        self.spark = spark_session

    def process_data_uuid(self, conf, hashed_values):
        """
        Process DataFrame by adding a UUID column and save it to a local file in JSON format.

        :param conf: Configuration dictionary containing 'Paths' with 'uuid_column' and 'uuid_output_path'.
        :param hashed_values: Input DataFrame.
        """
        try:
            # Generate a new column with unique identifiers using uuid.uuid4()
            df_with_uuid = hashed_values.withColumn("uuid_column",
                                                    sha2(concat(col(conf['Paths']['uuid_column']),
                                                                lit(str(uuid.uuid4()))), 256))

            output_path = os.path.join(conf['Paths']['uuid_output_path'])

            df_with_uuid.coalesce(1).write.mode("overwrite").json(output_path)

            logging.info(f"DataFrame with UUID saved to: {output_path}")

            return df_with_uuid

        except Exception as e:
            logging.error(f"Error during data processing: {str(e)}")
