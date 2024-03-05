import hashlib
import logging


class QualityCheck:
    """Class for performing quality checks on decrypted data."""

    def __init__(self, spark_session):
        """
         Initialize the QualityCheck object.

         Parameters:
         - spark_session (SparkSession): The Spark session object.
        """
        self.spark = spark_session

    def quality_check(self,df_with_uuid):
        """
            Perform quality checks on the DataFrame.

            :param df_with_uuid: DataFrame with UUID column.
            """

        try:
            # Check count of rows
            count_rows = len(df_with_uuid)
            print(count_rows)
            logging.info(f"Number of rows in DataFrame: {count_rows}")

            # Calculate MD5 checksum
            md5_hash = hashlib.md5()
            # Converting each row to a string and updating the hash
            for row in df_with_uuid.collect():
                md5_hash.update(str(row.asDict()).encode('utf-8'))

            md5_checksum = md5_hash.hexdigest()
            print(md5_checksum)
            logging.info(f"MD5 checksum of DataFrame: {md5_checksum}")

        except Exception as e:
            logging.error(f"Error during quality check: {str(e)}")


        # Example usage:
        # Assuming you have already created a SparkSession
        # spark = SparkSession.builder.appName("YourAppName").getOrCreate()

