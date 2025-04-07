from pyspark.sql import SparkSession

def task4_rank_sensors():
    spark = SparkSession.builder.appName("Task4_Rank_Sensors").getOrCreate()
    
    df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)
    df.createOrReplaceTempView("sensor_readings")

    ranked_sensors = spark.sql("""
        WITH avg_temp_per_sensor AS (
    SELECT sensor_id, ROUND(AVG(temperature), 2) AS avg_temp 
    FROM sensor_readings 
    GROUP BY sensor_id
)
SELECT sensor_id, avg_temp, 
       DENSE_RANK() OVER (ORDER BY avg_temp DESC) AS rank_temp 
FROM avg_temp_per_sensor
ORDER BY rank_temp
LIMIT 5

    """)
    ranked_sensors.show()

    output_folder = "output/task4/"
    ranked_sensors.write.mode("overwrite").csv(f"{output_folder}task4_output.csv", header=True)
    
    spark.stop()

if __name__ == "__main__":
    task4_rank_sensors()
