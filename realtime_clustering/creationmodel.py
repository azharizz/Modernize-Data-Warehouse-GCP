from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
import subprocess

from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans, KMeansModel


spark = SparkSession.builder \
    .appName('CustomerClustering') \
    .config("spark.python.worker.timeout", "600") \
    .getOrCreate()

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("gs://customer-clustering-bucket/output_testing/csv_onetime/*.csv")

from pyspark.sql.functions import col

df = df.withColumn("CustomerID", col("CustomerID").cast("string"))

df = df.withColumn("TotalCountUnit", col("TotalCountUnit").cast("double"))

df =  df.withColumn("AverageCustomerSpent", col("TotalCustomerSpent") / col("TotalCountUnit"))




assembler = VectorAssembler(inputCols=["TotalCountUnit", "AverageCustomerSpent"], outputCol="features")
featureVector = assembler.transform(df)




kmeans = KMeans(k=5, seed=42)
model = kmeans.fit(featureVector)


df_with_clusters = model.transform(featureVector).select("CustomerID", "TotalCountUnit", "AverageCustomerSpent", "prediction")
df_with_clusters.show(20)


grouped_df = df_with_clusters.groupBy("prediction").count()
grouped_df.show()


model.save("gs://customer-clustering-bucket/kmeans_model")