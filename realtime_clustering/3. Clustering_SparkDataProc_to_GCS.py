from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
import subprocess

from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, StructField, TimestampType
from pyspark.sql.functions import from_json








spark = SparkSession.builder.appName("ClusteringCustomers").getOrCreate()


custom_schema = StructType([
    StructField("json_col", StringType(), nullable=False),
    StructField("time_data", TimestampType(), nullable=True),
    # Add more columns as needed
])

df = spark.readStream.format("csv") \
    .option("sep",";") \
    .schema(custom_schema) \
    .load("gs://customer-clustering-bucket/dataflowoutput/*.csv")


# df = spark.read.format("csv") \
#     .option("sep",";") \
#     .schema(custom_schema) \
#     .load("gs://customer-clustering-bucket/dataflowoutput/output-*")

# print(df.show())


# GET FILE NAME
# df_with_filename = df.withColumn("filename", input_file_name())
# file_names = df_with_filename.select("filename").distinct().rdd.flatMap(lambda x: x).collect()


# # GET JSON TO COLUMN

# json_schema = StructType([
#     StructField("TotalCountUnit", DoubleType(), nullable=True),
#     StructField("TotalCustomerSpent", DoubleType(), nullable=True),
#     StructField("CustomerID", StringType(), nullable=True),
#     # Add more fields as needed
# ])

# df = df.withColumn("parsed_json", from_json(df["json_col"], json_schema))


# df = df.select(
#     df["parsed_json.CustomerID"].alias("CustomerID"),
#     df["parsed_json.TotalCountUnit"].alias("TotalCountUnit"),
#     df["parsed_json.TotalCustomerSpent"].alias("TotalCustomerSpent"),
#     # Add more columns from the parsed_json struct as needed
# )

# # DATA TRANSFORMATION
# df =  df.withColumn("AverageCustomerSpent", col("TotalCustomerSpent") / col("TotalCountUnit"))


# ----------------------------------------------------------------------------

def write_to_gcs_dataproc(df, epoch_id):


    # GET FILE NAME
    df_with_filename = df.withColumn("filename", input_file_name())
    file_names = df_with_filename.select("filename").distinct().rdd.flatMap(lambda x: x).collect()


    # GET JSON TO COLUMN

    json_schema = StructType([
        StructField("TotalCountUnit", DoubleType(), nullable=True),
        StructField("TotalCustomerSpent", DoubleType(), nullable=True),
        StructField("CustomerID", StringType(), nullable=True),
        # Add more fields as needed
    ])

    df = df.withColumn("parsed_json", from_json(df["json_col"], json_schema))


    df = df.select(
        df["parsed_json.CustomerID"].alias("CustomerID"),
        df["parsed_json.TotalCountUnit"].alias("TotalCountUnit"),
        df["parsed_json.TotalCustomerSpent"].alias("TotalCustomerSpent"),
        # Add more columns from the parsed_json struct as needed
    )

    # DATA TRANSFORMATION
    df =  df.withColumn("AverageCustomerSpent", col("TotalCustomerSpent") / col("TotalCountUnit"))

    print("DATAFRAME Before Predict:")
    print(df.show())

    # local_output_path = "output.csv"
    gs_output_path = "gs://final_clustering_bucket/output_sparkdataproc"

    print("MODEL PREDICT")
    print("================================================================================================")

    assembler = VectorAssembler(inputCols=["TotalCountUnit", "AverageCustomerSpent"], outputCol="features")

    loaded_model = KMeansModel.load("gs://customer-clustering-bucket/kmeans_model")
    print("Success load model")


    example_vector = assembler.transform(df)

    print("success jadiin vector")

    predictions = loaded_model.transform(example_vector)

    print("Berhasil.. Show table")

    predictions = predictions.select("CustomerID", "TotalCountUnit", "AverageCustomerSpent", "prediction")


    print("================================================================================================")

    print(type(loaded_model))

    print("DATAFRAME After Predict:")
    print(predictions.show())

    predictions.write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(gs_output_path)


    print("Coba delete")
    for file_name in file_names:
        print("delete ke :",file_name)
        # Construct the full GCS path for the file
        gcs_path = f"{file_name}"

        print(gcs_path)
        
        # Delete the file using gsutil
        subprocess.run(["gsutil", "rm", gcs_path])

        print("delete success")



query = df.writeStream \
    .foreachBatch(write_to_gcs_dataproc) \
    .start()


query.awaitTermination()
