from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, pandas_udf, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
import pandas as pd
import numpy as np
import random
from confluent_kafka import Producer
import json
import os
import pickle
import sys

# --- Debugging setup (prints to stderr, visible in spark-submit output) ---
print("--- Starting Spark Streaming Pipeline ---", file=sys.stderr)
print(f"Python Version: {sys.version}", file=sys.stderr)
# If you want to see cloudpickle version, uncomment the line below.
# import cloudpickle
# print(f"Cloudpickle Version: {cloudpickle.__version__}", file=sys.stderr)
print(f"Current Working Directory: {os.getcwd()}", file=sys.stderr)

# --- Spark Session Initialization ---
spark = SparkSession.builder \
    .appName("SalonRevenuePrediction") \
    .master("spark://192.168.162.130:7077") \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

print("Spark Session created successfully.", file=sys.stderr)

# --- Kafka Stream Configuration ---
# Define schema for the incoming JSON data from Kafka
schema = StructType([
    StructField("Id_salon", StringType(), True),
    StructField("create_date", TimestampType(), True),
    StructField("total", DoubleType(), True)
])

# Kafka bootstrap servers configuration
# Use 'localhost:9092' for a single Kafka broker setup
# Use 'kafka1:9092,kafka2:9093,kafka3:9094' for a 3-broker Docker Compose setup (if configured correctly)
kafka_input_bootstrap_servers = "localhost:9092"
# kafka_input_bootstrap_servers = "kafka1:9092,kafka2:9093,kafka3:9094"

print(f"Connecting to Kafka input topic with bootstrap servers: {kafka_input_bootstrap_servers}", file=sys.stderr)

# Read stream from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_input_bootstrap_servers) \
    .option("subscribe", "salon-input") \
    .load()

# Parse JSON from Kafka value column
df = df.selectExpr("CAST(value AS STRING)") \
       .select(from_json(col("value"), schema).alias("data")) \
       .select("data.*")

print("JSON parsing complete for Kafka input stream.", file=sys.stderr)

# --- Model Loading and Prediction Pandas UDF ---

# Global variables to cache the model/fake_model_predict_func within each executor process.
# This prevents reloading the model for every micro-batch or every UDF call,
# ensuring efficient resource usage on executors.
# These variables are specific to the Python process on each executor.
_cached_model_instance = None
_cached_fake_model_predict_func = None

# Define the internal fake model prediction function.
# This function is defined globally (at the module level) to be properly serialized by cloudpickle.
# It uses 'random.uniform' for simplicity and to avoid potential pickling issues with numpy's random state.
def _fake_model_predict_internal(input_df: pd.DataFrame) -> np.ndarray:
    """
    Internal function to simulate ML model predictions when model.pkl is not found.
    This function is designed to be picklable and run on Spark executors.
    """
    # print("DEBUG: Inside _fake_model_predict_internal on executor.", file=sys.stderr)
    predictions = np.zeros((len(input_df), 2)) # [revenue_month, revenue_week]
    for i, row in input_df.iterrows():
        total_val = row["total"]
        predictions[i, 0] = total_val * random.uniform(20, 30) # Simulated monthly revenue
        predictions[i, 1] = total_val * random.uniform(5, 7)   # Simulated weekly revenue
    return predictions

# Define the Pandas UDF for revenue prediction.
# The core logic for loading/initializing the model (real or fake) is placed here,
# ensuring it runs once per executor process.
@pandas_udf("struct<Id_salon: string, date_predict: string, revenue_month: double, revenue_week: double>")
def predict_revenue(salon_id: pd.Series, create_date: pd.Series, total: pd.Series) -> pd.DataFrame:
    """
    Pandas UDF to predict revenue using a loaded model or a fake model.
    The model loading logic is self-contained within the UDF's execution context
    to ensure proper serialization and efficient lazy loading on executors.
    """
    global _cached_model_instance
    global _cached_fake_model_predict_func

    # Initialize _cached_fake_model_predict_func if it's the first time this UDF runs on an executor.
    if _cached_fake_model_predict_func is None:
        _cached_fake_model_predict_func = _fake_model_predict_internal
        # print("DEBUG: _fake_model_predict_internal cached on executor.", file=sys.stderr)

    # Load real model or assign fake model only once per executor process.
    # This block ensures that the model is loaded/initialized only when needed on each executor.
    if _cached_model_instance is None:
        model_path = "model/model.pkl" # This path must be accessible from all worker nodes/executors

        # print(f"DEBUG: Checking for model.pkl at {model_path} on executor.", file=sys.stderr)
        if os.path.exists(model_path):
            try:
                with open(model_path, "rb") as f:
                    _cached_model_instance = pickle.load(f)
                # print(f"DEBUG: Loaded model from {model_path} successfully on executor.", file=sys.stderr)
            except Exception as e:
                print(f"ERROR: Error loading model.pkl on executor: {e}. Using fake model.", file=sys.stderr)
                _cached_model_instance = _cached_fake_model_predict_func # Assign the function
        else:
            print("WARNING: model.pkl not found on executor. Using fake model.", file=sys.stderr)
            _cached_model_instance = _cached_fake_model_predict_func # Assign the function
    # else:
        # print("DEBUG: Model already cached on executor.", file=sys.stderr)

    # Prepare input data as a Pandas DataFrame for the model
    input_df = pd.DataFrame({
        "Id_salon": salon_id,
        "create_date": create_date,
        "total": total
    })

    # Call the appropriate prediction function/method based on what was loaded/assigned
    if callable(_cached_model_instance): # If _cached_model_instance is the fake model function
        # print("DEBUG: Using fake model for prediction on executor.", file=sys.stderr)
        predictions = _cached_model_instance(input_df)
    else: # If _cached_model_instance is a loaded model object with a .predict method
        # print("DEBUG: Using loaded model for prediction on executor.", file=sys.stderr)
        predictions = _cached_model_instance.predict(input_df)

    # Return the predictions as a Pandas DataFrame matching the UDF's return type
    return pd.DataFrame({
        "Id_salon": salon_id,
        "date_predict": create_date.dt.strftime("%Y-%m-%d"),
        "revenue_month": predictions[:, 0],
        "revenue_week": predictions[:, 1]
    })

print("Pandas UDF 'predict_revenue' defined.", file=sys.stderr)

# Apply the UDF to the streaming DataFrame
# This groups data by salon ID and creation date, sums the total,
# and then applies the predict_revenue UDF to get predictions.
result_df = df.groupBy("Id_salon", "create_date").agg({"total": "sum"}) \
              .withColumnRenamed("sum(total)", "total") \
              .select(predict_revenue(col("Id_salon"), col("create_date"), col("total")).alias("prediction")) \
              .select("prediction.*")

print("UDF 'predict_revenue' applied to DataFrame. Resulting schema:", file=sys.stderr)
result_df.printSchema() # Print the schema of the DataFrame after UDF application

# --- Write Stream to Kafka Output Topic ---
kafka_output_bootstrap_servers = "localhost:9092"
# kafka_output_bootstrap_servers = "kafka1:9092,kafka2:9093,kafka3:9094"

def send_to_kafka(df_batch: pd.DataFrame, epoch_id: int): # df_batch is a PySpark DataFrame here
    """
    Function to send each micro-batch of predicted data to a Kafka output topic.
    A new Kafka Producer is initialized for each batch on the executor to ensure serializability
    and proper resource management.
    """
    print(f"INFO: Processing batch {epoch_id} with {df_batch.count()} records for Kafka output.", file=sys.stderr)
    
    # Initialize Kafka Producer inside the foreachBatch function.
    # This ensures that the Producer object is created on the executor and is not serialized from the driver.
    producer_internal = Producer({'bootstrap.servers': kafka_output_bootstrap_servers})

    # Convert PySpark DataFrame to Pandas DataFrame for easier iteration.
    # WARNING: This can be memory intensive for large batches as the entire batch
    # is pulled into the memory of a single executor.
    pandas_df_batch = df_batch.toPandas()

    # Iterate over rows in the Pandas DataFrame batch
    for index, row in pandas_df_batch.iterrows():
        try:
            message = json.dumps({
                "Id_salon": row["Id_salon"],
                "date_predict": row["date_predict"],
                "revenue_month": row["revenue_month"],
                "revenue_week": row["revenue_week"]
            })
            producer_internal.produce("salon-output", value=message.encode('utf-8'))
            # print(f"DEBUG: Produced message for batch {epoch_id}: {message}", file=sys.stderr) # Uncomment for verbose debug
        except Exception as e:
            print(f"ERROR: Failed to produce message for row {row}: {e}", file=sys.stderr)
    
    # Flush the producer to ensure all messages are sent before the function exits
    try:
        producer_internal.flush()
        print(f"INFO: Successfully flushed {producer_internal.len()} messages for batch {epoch_id}.", file=sys.stderr)
    except Exception as e:
        print(f"ERROR: Failed to flush producer for batch {epoch_id}: {e}", file=sys.stderr)

print("send_to_kafka function defined for Kafka output.", file=sys.stderr)

# Define the output sink for the streaming query
query = result_df \
    .writeStream \
    .foreachBatch(send_to_kafka) \
    .outputMode("update") \
    .start()

print("Spark Streaming query started. Awaiting termination...", file=sys.stderr)

# Await termination of the streaming query
query.awaitTermination()

print("Spark Streaming query terminated.", file=sys.stderr)


