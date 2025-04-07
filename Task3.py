from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

def task3_time_analysis():
    spark = SparkSession.builder.appName("Task3_Time_Analysis").getOrCreate()
    
    df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

    # Convert the timestamp column to a proper timestamp type
    df = df.withColumn("timestamp_parsed", to_timestamp("timestamp"))

    # Create or replace a temporary view with the new column
    df.createOrReplaceTempView("sensor_readings")

    # Perform the SQL query to get hourly average temperatures
    hourly_avg = spark.sql("""
        SELECT HOUR(timestamp_parsed) AS hour_of_day, 
               ROUND(AVG(temperature), 2) AS avg_temp 
        FROM sensor_readings 
        GROUP BY hour_of_day 
        ORDER BY hour_of_day
    """)

    hourly_avg.show()

    # Save the result to a CSV file
    output_folder = "output/task3/"
    hourly_avg.write.mode("overwrite").csv(f"{output_folder}task3_output.csv", header=True)
    
    spark.stop()

if __name__ == "__main__":
    task3_time_analysis()
