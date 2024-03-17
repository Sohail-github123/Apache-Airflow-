from pyspark.sql import SparkSession, functions as fc

def add_columns_and_save(input_path, output_path):
    spark = SparkSession. \
    builder. \
    appName("add_column"). \
    getOrCreate()

    df = spark.read.parquet(input_path)
    df = df.withColumn("empid", fc.lit(10)).withColumn(
        "empname", fc.lit("sohail")
    )
    df.write.parquet(output_path)

if __name__ == "__main__":
    input_path = "/home/sohail/airflow/dags/parquet"
    output_path = "/home/sohail/airflow/dags/newparquet"
    add_columns_and_save(input_path, output_path)

# spark = SparkSession.builder.appName("ADD_COLUMN").getOrCreate()
# df = spark.read.parquet("/home/sohail/airflow/dags/parquet")

# df = df.withColumn("empid", fc.lit(10)).withColumn(
#     "empname", fc.lit("sohail")
# )

# # df.show(truncate=False)
# df.write.parquet("/home/sohail/airflow/dags/newparquet")

