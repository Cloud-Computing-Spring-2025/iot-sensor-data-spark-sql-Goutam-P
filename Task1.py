from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
import os

def task1_explore_data():
    spark = SparkSession.builder.appName("Task1_Explore").getOrCreate()

    # Define schema explicitly
    schema = StructType([
        StructField("sensor_id", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("location", StringType(), True),
        StructField("sensor_type", StringType(), True)
    ])

    # Load CSV
    df = spark.read.csv("sensor_data.csv", header=True, schema=schema)

    # Convert to timestamp
    df = df.withColumn("timestamp", df["timestamp"].cast(TimestampType()))

    # Create temp view
    df.createOrReplaceTempView("sensor_readings")

    # Queries
    print("First 5 rows:")
    first_five = spark.sql("SELECT * FROM sensor_readings LIMIT 5")
    first_five.show()

    total_records = spark.sql("SELECT COUNT(*) AS total_records FROM sensor_readings")
    total_records.show()

    distinct_locations = spark.sql("SELECT DISTINCT location FROM sensor_readings")
    distinct_locations.show()

    # Create output directory
    output_folder = "output/task1/"
    os.makedirs(output_folder, exist_ok=True)

    # Write outputs
    distinct_locations.write.mode("overwrite").option("header", True).csv(f"{output_folder}task1a_output")
    total_records.write.mode("overwrite").option("header", True).csv(f"{output_folder}task1b_output")
    first_five.write.mode("overwrite").option("header", True).csv(f"{output_folder}task1c_output")

    spark.stop()

if __name__ == "__main__":
    task1_explore_data()