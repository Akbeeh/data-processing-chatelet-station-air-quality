# Data processing Chatelet Station Air Quality
This small project focuses on the use of Apache Spark to learn more on data processing.

Help from https://spark.apache.org/.

## Overview
In this project, we'll explore Apache Spark. We'll use PySpark (for more details, go check [docs](https://spark.apache.org/docs/latest/api/python/index.html)), the Python API for Spark, to work with a dataset and perform various data processing tasks. 

## Steps followed

### 0. Installation & Setup
Website: https://spark.apache.org/downloads.html (or use `pip install`)

```bash
pip install pyspark
```

### 1. Write a Python script to perform data processing tasks
For the dataset, I chose the [Chatelet Station Air Quality](https://data.ratp.fr/explore/dataset/qualite-de-lair-mesuree-dans-la-station-chatelet/information/).

```python
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Create a Spark configuration and context
conf = SparkConf().setAppName("DataProcessingChateletStationAirQuality")
sc = SparkContext(conf=conf)

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# ...
# Use of select, where, filter, sort, when, expr, col, cast, alias, avg...
# ...
```

### 2. Run the Python script
```bash
python data_processing_chatelet-station-air-quality.py
```

### Where I got a bit stuck / Interesting points
- Even if the parameter `inferSchema` is set to True while reading the CSV file, it might have difficulties to retrieve the Data Type. Therefore, it was interesting to cast the column into other Data Type (String -> Float).
- It may be possible to have bizarre column names such as *DATE/HOUR*. So while using the SQL queries, the use of backticks is primordial to ensure that the entire string is treated as a column identifier.
- The `~` operator in `filter` SQL function is interesting as it is used to have the inverse condition.
- `first()` aggregate function returns the first value in a group as a Row format. To retrieve the value, the nice idea is to put an alias and to get it as a key dictionnary (e.g. examples proceeded with the 7th column in the Python file).
- `*` operator is used to unpack elements from a list and can be passed as arguments to functions.
- Be careful about the amount of data manipulated, that's to say we need to care about the worker nodes and the driver node. If we decide to use `collect()`, it can be risky as the computer (on the driver node) may be run out of memory.

### Extra: Setup of pre-commit
```bash
pip install pre-commit
```

Once the `.pre-commit-config.yaml` completed, we need to set up the git hooks scripts.

```bash
pre-commit install
```