from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, regexp_replace, current_date
from pyspark.sql.types import StructType, StructField, StringType

# Create a SparkSession
spark = SparkSession.builder.appName("RandomUserProcessing").getOrCreate()

# Read Avro data from Amazon S3 and display it
data = spark.read.format("avro").load("s3://aazeyodev/src/")
data.show()

# Fetch random user data from the API endpoint and create a DataFrame
html = requests.get("https://randomuser.me/api/0.8/?results=500")
s = html.json()
schema = StructType([StructField("results", StringType(), True)])
urldf = spark.createDataFrame([str(s)], schema)
urldf = urldf.select(explode(col("results")).alias("results"))
urldf.show()

# Flatten the DataFrame by exploding the "results" array column and selecting specific columns
flatdf = urldf.select("results.nationality", "results.seed", "results.version",
                      "results.user.username", "results.user.cell", "results.user.dob", "results.user.email",
                      "results.user.gender", "results.user.location.city", "results.user.location.state",
                      "results.user.location.street", "results.user.location.zip", "results.user.md5",
                      "results.user.name.first", "results.user.name.last", "results.user.name.title",
                      "results.user.password", "results.user.phone", "results.user.picture.large",
                      "results.user.picture.medium", "results.user.picture.thumbnail", "results.user.registered",
                      "results.user.salt", "results.user.sha1", "results.user.sha256")
flatdf.show()

# Remove numbers from the "username" column in the flat DataFrame
rm = flatdf.withColumn("username", regexp_replace(col("username"), "([0-9])", ""))
rm.show()

# Join the Avro data with the modified random user data using the "username" column
joindf = data.join(rm, on="username", how="left")
joindf.show()

# Filter the joined DataFrame to get rows with "nationality" as null and not null
dfnull = joindf.filter(col("nationality").isNull())
dfnotnull = joindf.filter(col("nationality").isNotNull())
dfnotnull.show()
dfnull.show()

# Fill null values in the "dfnull" DataFrame with "Not Available" and 0
replacenull = dfnull.fillna("Not Available").fillna(0)
replacenull.show()

# Add a "current_date" column to both DataFrames with the current date
from pyspark.sql.functions import lit
noavail = replacenull.withColumn("current_date", lit(current_date()))
avail1 = dfnotnull.withColumn("current_date", lit(current_date()))

# Write the available and not available customer DataFrames to Parquet files on S3
avail1.write.format("parquet").mode("overwrite").save("s3://aazeyodev/dest/availablecustomers")
noavail.write.format("parquet").mode("overwrite").save("s3://aazeyodev/dest/notavailablecustomers")

# Stop the SparkSession
spark.stop()
