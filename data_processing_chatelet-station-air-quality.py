from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Create a Spark configuration and context
conf = SparkConf().setAppName("DataProcessingChateletStationAirQuality")
sc = SparkContext(conf=conf)

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Set log level to avoid excessive log messages
sc.setLogLevel("ERROR")

# Load the dataset into a Spark DataFrame
data_file = "chatelet-station-air-quality.csv"
df = spark.read.csv(data_file, sep=";", header=True, inferSchema=True)

# Explore the Data
print("==================================")
print("========== EXPLORE DATA ==========")
print("==================================")
print("View the data:")
df.show()
print("View the schema:")
df.printSchema()
print("==================================")

# Data Transformation and Filtering
column_names = df.columns

print(df[column_names[0]])

# 1) Days where max/min for the first columun (excluding the date column)
day_max_no = df.select(column_names[0]).where(max(df[column_names[0]]))
day_min_no = df.select(column_names[0]).where(min(df[column_names[0]]))

print(day_max_no, day_min_no)
# Data Aggregation

# ... Additional tasks ...

# Display or Save Results


# Stop the SparkContext
sc.stop()
