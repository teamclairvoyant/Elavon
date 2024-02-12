from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
import os
import configparser

# Set HADOOP_HOME
config = configparser.ConfigParser()
config.read('config.ini')
os.environ['HADOOP_HOME'] = 'C:\\hadoop-3.3.6'
# Create a SparkSession
spark = SparkSession.builder.appName("UUIDJob").getOrCreate()
# Input path
input_path = config['Paths']['hashed_output_file']

# Read the JSON file into a DataFrame
df = spark.read.json(input_path)

# Add a new column "sample_UUID" using withColumn and concat functions
df = df.withColumn("sample_UUID",
                   concat(
                       lit("visa"),
                       col(config['Paths']['uuid_column']),
                       lit("_"),
                       lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                   )
                   )

# Show the DataFrame with the new column
df.show(truncate=False)
num_partitions = 3
# Repartition the DataFrame
df_repartitioned = df.repartition(num_partitions)
uuid_repartitioned_output_path = config['Paths']['uuid_repartitioned_output_path']
df_repartitioned.write.format('json').save(uuid_repartitioned_output_path)
df.write.format('json').save(config['Paths']['uuid_output_path'])
