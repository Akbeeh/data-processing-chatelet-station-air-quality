from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, max, avg

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

# Preprocessing to remove all "ND" by None values
column_names = df.columns
for column_name in column_names[1:]:
    df = df.withColumn(
        column_name,
        when(col(column_name) == "ND", None).otherwise(col(column_name).cast("float")),
    )

# Create SQL table
table_name = "chatelet_station_air_quality"
df.createOrReplaceTempView(table_name)

# Data Transformation and Filtering

## 1) Select first column
print("===================================================")
print("1) Select first column (with DF API and SQL select query)")
print("===================================================")

days_df = df.select(column_names[0])
days_sql = spark.sql(
    f"""
    SELECT `{column_names[0]}`
    FROM {table_name}
    """
)

days_df.show(5)
days_sql.show(5)

## 2) Where the second and third columns exceed recommended threshold
print("===================================================")
print("2) Where the second and third columns exceed recommended threshold")
print("===================================================")

day_no_sup_nitro_mono = df.select("*").where(f"{column_names[1]} >= 120")
day_no2_sup_nitro_dio = spark.sql(
    f"""
    SELECT *
    FROM {table_name}
    WHERE {column_names[2]} >= 200
    """
)

day_no_sup_nitro_mono.show(5)
day_no2_sup_nitro_dio.show(5)

## 3) Filter the fourth column to remove the values superior than 50
print("===================================================")
print("3) Filter the fourth column to remove the values superior than 50")
print("===================================================")

df_no_nd = df.filter(df[column_names[3]] < 50)
df_no_nd.show(5)

## 4) Increment 5 to the fifth column values
print("===================================================")
print("4) Increment 5 to the fifth column values")
print("===================================================")

df_incr = df.select(
    column_names[0],
    column_names[4],
    when(col(column_names[4]).isNotNull(), col(column_names[4]) + 5)
    .otherwise(5)
    .alias(f"new_{column_names[4]}"),
).sort(df[column_names[4]].desc())
df_incr.show(5)

## 5) Sort the sixth column by Ascending/Descending
print("===================================================")
print("5) Sort the sixth column by Ascending/Descending")
print("===================================================")

# we can also write df.filter(expr(f"{column_names[5]} NOT IN ('null', 'ND')"))
df_sort_6th_asc = df.filter(~(df[column_names[5]].isin("null"))).sort(
    df[column_names[5]].asc()
)
df_sort_6th_desc = spark.sql(
    f"""
    SELECT *
    FROM {table_name}
    ORDER BY {column_names[5]} DESC
    """
)

df_sort_6th_asc.show(5)
df_sort_6th_desc.show(5)

## 6) Get the max/min value of the seventh column
print("===================================================")
print("6) Get the max/min value of the seventh column")
print("===================================================")

df_max_7th = df.filter(
    col(column_names[6])
    == df.select(max(col(column_names[6])).alias("max_value")).first()["max_value"]
)
df_min_7th = spark.sql(
    f"""
    SELECT *
    FROM {table_name}
    WHERE {column_names[6]} = (SELECT MIN({column_names[6]}) FROM {table_name})
    """
)

### Returns SQL table format
# df.select(max(col(column_names[6]))).show()

### Returns Row format -> Row(max(HUMI)=100.0)
# print(df.select(max(col(column_names[6]))).first())

### Returns Row format -> Row(max_value)=100.0)
# print(df.select(max(col(column_names[6])).alias("max_value")).first())

### Returns the value -> 100.0
# print(df.select(max(col(column_names[6])).alias("max_value")).first()["max_value"])

df_max_7th.show()
df_min_7th.show()

# Data Aggregation
print("===================================================")
print("7) Get the average value of every column using agg")
print("===================================================")


df_agg_avg = df.agg(
    *[
        avg(col(column_name)).alias(f"avg_{column_name}")
        for column_name in column_names[1:]
    ]
)

df_agg_avg.show()

# Save Results

df_agg_avg.write.mode("overwrite").options(header="True", delimiter=",").csv(
    "aggregated_avg_results"
)

print("===================================================")
print("The aggregated results are saved to a CSV file")
print("===================================================")

# Stop the SparkContext
sc.stop()
