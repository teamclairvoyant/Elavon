from pyspark.sql import SparkSession
import os
import logging
from datetime import datetime
import time
from Encryption import EncryptionDriver
from Decryption import DecryptionDriver
from Hashing import HashingDriver
from UuidGeneration import IdDriver
from QualityCheck import QualityCheck
from AzureStorage import AdlsUpload
from ConfigProcessor import get_config_details

# Create the output directory if it doesn't exist

output_directory = get_config_details()['Paths']['log']

# Set up the logger
log_file_path = os.path.join(output_directory, f"log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt")
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DataProcessingDriver:
    """
    A class for handling data processing operations using Apache Spark and other utility functions.
    """

    def create_spark_session(self):
        """
        Creates and returns a Spark session.

        Returns:
            pyspark.sql.SparkSession: A Spark session object.
        """
        spark = SparkSession.builder.appName("YourSparkJob").getOrCreate()
        return spark


if __name__ == "__main__":
    try:
        # Step 1: Initialization and object creation
        start_time = time.time()
        data_processing = DataProcessingDriver()

        # Step 2: Get configuration details
        conf = get_config_details()

        # Step 3: Create a Spark session
        spark = data_processing.create_spark_session()

        # Step 4: Encryption
        ED = EncryptionDriver(spark)
        encrypted = ED.encrypt_data(conf, spark)

        # Step 5: Decryption
        decrypted_data = DecryptionDriver(spark).decrypt_data(spark, encrypted)

        # Step 6: Hashing
        hashed_values = HashingDriver(spark).hashing(spark, decrypted_data)

        # Step 7: UUID Generation
        ID = IdDriver(spark)
        df_with_uuid = ID.process_data_uuid(conf, hashed_values)

        # Step 8: Quality Check
        QC = QualityCheck(spark)
        QC.quality_check(df_with_uuid)

        # Step 9: ADLS Upload
        Ad = AdlsUpload(conf)
        Ad.upload_files_to_blob_storage(conf)

        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time} seconds")

    except Exception as e:
        logging.INFO(f"An error occurred: {str(e)}")
        raise  # Re-raise the exception after logging
