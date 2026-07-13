from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, datediff, dayofweek
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import sys

def main():
    print("Initializing Spark Session for ML...")
    spark = SparkSession.builder \
        .appName("DeliveryDelayPrediction") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .enableHiveSupport() \
        .getOrCreate()

    print("Loading Silver/Gold data from Spark Warehouse...")
    try:
        # Load orders from the Hive warehouse (created by dbt)
        orders_df = spark.table("dim_orders")
    except Exception as e:
        print(f"Error loading dim_orders: {e}")
        print("Falling back to reading parquet directly if needed, but table should exist via dbt.")
        sys.exit(1)

    # Feature Engineering
    print("Performing Feature Engineering...")
    # Target Variable: Is delayed? (1 = Yes, 0 = No)
    df = orders_df.withColumn(
        "is_delayed", 
        when(col("order_delivered_customer_date") > col("order_estimated_delivery_date"), 1).otherwise(0)
    )

    # Features: Delivery duration estimation, day of week, etc.
    df = df.withColumn("estimated_duration_days", datediff(col("order_estimated_delivery_date"), col("order_purchase_timestamp")))
    df = df.withColumn("purchase_day_of_week", dayofweek(col("order_purchase_timestamp")))
    
    # Drop nulls for training
    df_clean = df.na.drop(subset=["estimated_duration_days", "purchase_day_of_week", "is_delayed"])

    # Assemble features
    assembler = VectorAssembler(
        inputCols=["estimated_duration_days", "purchase_day_of_week"],
        outputCol="features"
    )
    
    df_assembled = assembler.transform(df_clean)
    
    # Train/Test Split
    train_data, test_data = df_assembled.randomSplit([0.8, 0.2], seed=42)

    print("Training Logistic Regression Model...")
    lr = LogisticRegression(featuresCol="features", labelCol="is_delayed", maxIter=10)
    model = lr.fit(train_data)

    print("Evaluating Model...")
    predictions = model.transform(test_data)
    evaluator = BinaryClassificationEvaluator(labelCol="is_delayed")
    auc = evaluator.evaluate(predictions)
    print(f"Model AUC: {auc}")

    print("Saving predictions to Gold layer...")
    # Select final columns to save
    final_predictions = predictions.select(
        "order_id", 
        "customer_id", 
        "is_delayed", 
        col("prediction").alias("predicted_delay"),
        col("probability")
    )
    
    # Save to Hive table
    final_predictions.write.mode("overwrite").saveAsTable("gold_delivery_predictions")
    print("Successfully saved gold_delivery_predictions!")
    
    spark.stop()

if __name__ == "__main__":
    main()
