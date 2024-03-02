# Pre-Processing-Batch-FWK

This document is designed to be read in parallel with the code in the pre-processing-batch-FWK project repository. Together, these constitute how we can Encrypt, Decrypt, Applying REST API as Crypto Service, adding UUID field and finally generating QC file along with the Data file. This project addresses the following topics:
•	how to read data file and encrypts it by generating a key.
•	how to decrypt the encrypted file using the same key.
•	how to use REST API as a cryptoservice to apply hashing on top of PI level columns.
•	how to generate UUID value to add unique column.
•	how to create QC file
•	how to send both Data,QC files to ADLS.

The basic project structure is as follows:
.idea
  .idea/.gitignore
  .idea/Spring-Pyspark-Framework.iml
  .idea/inspectionProfiles
  .idea/inspectionProfiles/Project_Default.xml
  .idea/inspectionProfiles/profiles_settings.xml
  .idea/misc.xml
  .idea/modules.xml
  .idea/vcs.xml
Config
   Config/config.ini
Ouput
   Ouput/log_2024-02-28_11-11-36.txt
   Ouput/log_2024-02-28_19-26-39.txt
README.md
src
   src/ApiCreation.py
   src/AzureStorage.py
   src/ConfigProcessor.py
   src/Decryption.py
   src/Driver.py
   src/Encryption.py
   src/Hashing.py
   src/QualityCheck.py
   src/UuidGeneration.py
   src/mykey.key
tests
   tests/tests
   tests/tests/__init__.py
   tests/tests/test_Decryption.py
   tests/tests/test_Encryption.py
   tests/tests/test_QualityCheck.py
   tests/tests/test_uuid.py


Encrypting Data file:
 We are encrypting data using Fernet encryption.
  Parameters:
       -  spark_session (pyspark.sql.SparkSession): The Spark session object.
       - conf (dict): Configuration dictionary containing file paths and settings.
Decryption:
Decrypts the encrypted data and saves it to a JSON file.
Parameters:
       -  spark_session (pyspark.sql.SparkSession): The Spark session object.
       - conf (dict): Configuration dictionary containing file paths and settings.
Hashing:
Hashes specified columns of a DataFrame in batches using a remote API.
Parameters:
        - spark (pyspark.sql.SparkSession): Spark session object.
 Returns:
        - pyspark.sql.DataFrame: DataFrame with hashed values joined back to the original DataFrame.
UUID Generation:
Process DataFrame by adding a UUID column using uuid.uuid4() and save it to a local file in JSON format.
Parameters:
 conf: Configuration dictionary containing 'Paths' with 'uuid_column' and 'uuid_output_path'.
QC file Generation:
performing quality checks on decrypted data. This QC file will contain the data file name, Record count and MD5 checksum value separated by a (|) symbol.

Parameters:
-  spark_session (pyspark.sql.SparkSession): The Spark session object.
- conf (dict): Configuration dictionary containing file paths and settings.
Azure Storage:
Uploading both data and QC files to Azure Blob storage. 

Args:
                file_path (str): The local path of the file.
                file_name (str): The name of the file.
