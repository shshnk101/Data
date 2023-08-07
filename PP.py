from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import expr, col

# Create a SparkSession
spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

# Read data from text files and filter rows containing 'Gymnastics'
file1 = spark.read.text("file:///home/cloudera/revdata/file1.txt")
gymdata = file1.filter(expr("value LIKE '%Gymnastics%'"))
gymdata.show()

# Define a schema using namedtuple
from collections import namedtuple
schema = namedtuple("schema", ["txnno", "txndate", "custno", "amount", "category", "product", "city", "state", "spendby"])

# Split the data and map to the defined schema
mapsplit = gymdata.rdd.map(lambda x: x.value.split(","))
schemardd = mapsplit.map(lambda x: schema(*x))
prodrdd = schemardd.filter(lambda x: 'Gymnastics' in x.product)
schemadf = spark.createDataFrame(prodrdd)
schemadf.show()

# Read data from another text file and convert to DataFrame using Row
file2 = spark.read.text("file:///home/cloudera/revdata/file2.txt")
mapsplit1 = file2.rdd.map(lambda x: x.value.split(","))
rowrdd = mapsplit1.map(lambda x: Row(*x))
rowdf = spark.createDataFrame(rowrdd)

# Define schema for the DataFrame
from pyspark.sql.types import StructType, StructField, StringType
rschema = StructType([
    StructField("txnno", StringType(), True),
    StructField("txndate", StringType(), True),
    StructField("custno", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("category", StringType(), True),
    StructField("product", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("spendby", StringType(), True)
])

# Create DataFrame with defined schema
rowdf = spark.createDataFrame(rowrdd, rschema)
rowdf.show()

# Read data from a CSV file
csvdf = spark.read.format("csv").option("header", "true").load("file:///home/cloudera/revdata/file3.txt")
csvdf.show()

# Read data from a JSON file and select specific columns
jsondf = spark.read.format("json").load("file:///home/cloudera/revdata/file4.json")
jsondf = jsondf.select("txnno", "txndate", "custno", "amount", "category", "product", "city", "state", "spendby")
jsondf.show()

# Read data from a Parquet file
parquetdf = spark.read.load("file:///home/cloudera/revdata/file5.parquet")
parquetdf.show()

# Union all DataFrames into one DataFrame
uniondf = schemadf.union(rowdf).union(csvdf).union(jsondf).union(parquetdf)

# Perform transformations on DataFrame
procdf = uniondf.withColumn("txndate", expr("split(txndate, '-')[2]")) \
    .withColumnRenamed("txndate", "year") \
    .withColumn("status", expr("CASE WHEN spendby = 'cash' THEN 1 ELSE 0 END")) \
    .filter(col("txnno") > 50000)

# Write the final DataFrame to Parquet files partitioned by "category"
procdf.write.format("parquet").partitionBy("category").mode("append").save("file:///home/cloudera/resultdf")

# Stop the SparkSession
spark.stop()
