from pyspark.sql import SparkSession

def csv_to_parquet(input_path, output_path):
    spark = SparkSession.builder.appName("csv-to-parquet").getOrCreate()
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df.write.parquet(output_path)

if __name__ == "__main__":
    input_path = "/home/sohail/airflow/dags/csv/Sample.csv"
    output_path = "/home/sohail/airflow/dags/parquet"
    csv_to_parquet(input_path, output_path)

# spark = SparkSession.builder.appName("CSV_TO_PARQUET").getOrCreate()

# df = spark.read.csv("/home/sohail/airflow/dags/csv/Sample.csv", header=True, inferSchema=True)

# df.write.parquet("/home/sohail/airflow/dags/parquet")

