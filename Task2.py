from pyspark.sql import SparkSession

def task2_filter_aggregate():
    spark = SparkSession.builder.appName("Task2_Filter_Aggregate").getOrCreate()
    
    df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)
    df.createOrReplaceTempView("sensor_readings")

    in_range_count = spark.sql("SELECT COUNT(*) as in_range_count FROM sensor_readings WHERE temperature BETWEEN 18 AND 30")
    out_of_range_count = spark.sql("SELECT COUNT(*) as out_of_range_count FROM sensor_readings WHERE temperature < 18 OR temperature > 30")

    print("In-range records:")
    in_range_count.show()
    print("Out-of-range records:")
    out_of_range_count.show()

    agg_df = spark.sql("""
        SELECT location, 
               ROUND(AVG(temperature), 2) AS avg_temperature, 
               ROUND(AVG(humidity), 2) AS avg_humidity 
        FROM sensor_readings 
        GROUP BY location 
        ORDER BY avg_temperature DESC
    """)
    agg_df.show()
    output_folder = "output/task2/"
    agg_df.write.mode("overwrite").csv(f"{output_folder}task2a_output.csv", header=True)
    in_range_count.write.mode("overwrite").csv(f"{output_folder}task2b_output.csv", header=True)
    out_of_range_count.write.mode("overwrite").csv(f"{output_folder}task2c_output.csv", header=True)
    

    
    spark.stop()

if __name__ == "__main__":
    task2_filter_aggregate()
