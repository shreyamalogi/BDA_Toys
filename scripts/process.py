from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, explode, unix_timestamp, concat_ws
from pyspark.ml.feature import Tokenizer
from pyspark.ml import Pipeline

# ✅ Initialize Spark session
spark = SparkSession.builder \
    .appName("Toy Review Analysis") \
    .getOrCreate()

# ✅ Load the JSONL file (adjust the path to your GCP bucket location)
data_path = "gs://toys555/raw_data/Toys_and_Games.jsonl"
df = spark.read.json(data_path)

# ✅ Sample a fraction of the data (reduce dataset size)
df = df.sample(fraction=0.1, seed=42)  # Adjust fraction if needed

# ✅ Convert timestamp from milliseconds to seconds and cast to timestamp
df = df.withColumn("timestamp", unix_timestamp((col("timestamp") / 1000).cast("string")).cast("timestamp"))

# ✅ Flatten the `images` column (CSV does not support nested arrays/structs)
df = df.withColumn("images", concat_ws(",", col("images.attachment_type"), col("images.large_image_url"), col("images.medium_image_url"), col("images.small_image_url")))

# ✅ Check total reviews and unique products before processing
print(f"Total reviews (sampled): {df.count()}")
print(f"Total unique products: {df.select('asin').distinct().count()}")

# ✅ Task 1: Analyze average rating of toys (LIMIT 10,000 rows)
avg_rating_df = df.groupBy("asin").agg(
    avg("rating").alias("avg_rating"),
    count("rating").alias("review_count")
).orderBy("review_count", ascending=False).limit(10000)

# ✅ Task 2: Identify common words in titles and reviews
tokenizer_title = Tokenizer(inputCol="title", outputCol="title_words")
tokenizer_text = Tokenizer(inputCol="text", outputCol="text_words")
pipeline = Pipeline(stages=[tokenizer_title, tokenizer_text])
df_transformed = pipeline.fit(df).transform(df)

title_words_df = df_transformed.select(explode(col("title_words")).alias("word")).groupBy("word").count().orderBy("count", ascending=False).limit(10000)
text_words_df = df_transformed.select(explode(col("text_words")).alias("word")).groupBy("word").count().orderBy("count", ascending=False).limit(10000)

# ✅ Task 3: Determine the most helpful reviews (LIMIT 10,000 rows)
helpful_reviews_df = df.filter(col("helpful_vote") > 0).orderBy(col("helpful_vote"), ascending=False).limit(10000)

# ✅ Task 4: Filter verified purchases (LIMIT 10,000 rows)
verified_df = df.filter(col("verified_purchase") == True).limit(10000)

# ✅ Save results to single CSV outputs
output_path = "gs://toys555/results/"

avg_rating_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}avg_ratings.csv")
title_words_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}title_word_counts.csv")
text_words_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}text_word_counts.csv")
helpful_reviews_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}helpful_reviews.csv")
verified_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}verified_reviews.csv")

# ✅ Stop Spark session
spark.stop()
