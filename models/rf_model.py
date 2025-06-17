from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Initialize Spark Session
spark = SparkSession.builder.appName("MLlib_RandomForest").getOrCreate()

# Load Data from GCS Bucket
df = spark.read.csv("gs://toys555/results/hive_final.csv/000000_0", header=True, inferSchema=True)

# Print Schema to verify column names
df.printSchema()
df.show(5)

# Convert 'verified_purchase' (_c3) to binary label (1 = Yes, 0 = No)
df = df.withColumn("label", when(col("_c3") == "Yes", 1).otherwise(0))

# Safely cast feature columns (use backticks for problematic column names)
df = df.withColumn("`0.0`", col("`0.0`").cast("double"))
df = df.withColumn("`0`", col("`0`").cast("double"))

# Rename columns if preferred (optional but cleaner)
df = df.withColumnRenamed("0.0", "feature_0_0")
df = df.withColumnRenamed("0", "feature_0")

# Define features after renaming
feature_cols = ["feature_0_0", "feature_0"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df = assembler.transform(df)

# Initialize and train Random Forest model
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10)
model = rf.fit(df)

# Make predictions
predictions = model.transform(df)

# Evaluate accuracy
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)

print(f"Test Accuracy = {accuracy:.4f}")

# Stop Spark session
spark.stop()
