from pyspark.sql import SparkSession
from pyspark.sql.functions import hour

def task5_pivot_data():
    spark = SparkSession.builder.appName("Task5_Pivot").getOrCreate()
    
    df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

    # Add a new column 'hour_of_day' extracted from 'timestamp'
    df = df.withColumn("hour_of_day", hour("timestamp"))

    # Group by 'location' and pivot by 'hour_of_day' with average temperature as values
    pivot_df = df.groupBy("location").pivot("hour_of_day", range(0, 24)).avg("temperature")

    pivot_df.show()

    # Save the result to a CSV file
    output_folder = "output/task5/"
    pivot_df.write.mode("overwrite").csv(f"{output_folder}task5_output.csv", header=True)
    
    spark.stop()

if __name__ == "__main__":
    task5_pivot_data()
