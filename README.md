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
```

### 2. Run the Python script
```bash
python data_processing_chatelet-station-air-quality.py
```

### Extra: Setup of pre-commit
```bash
pip install pre-commit
```

Once the `.pre-commit-config.yaml` completed, we need to set up the git hooks scripts.

```bash
pre-commit install
```