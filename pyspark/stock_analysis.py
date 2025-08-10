from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg, max, min
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Twitter Stock Analysis").getOrCreate()

# Load CSV
df = spark.read.csv("twitter_stock.csv", header=True, inferSchema=True)

# Filter & select relevant columns
df = df.select(col("Date"), col("Close")).orderBy("Date")

# Calculate daily change
window_spec = Window.orderBy("Date")
df = df.withColumn("PrevClose", lag("Close").over(window_spec))
df = df.withColumn("Change", col("Close") - col("PrevClose"))

# Calculate metrics
avg_change = df.select(avg("Change")).first()[0]
max_gain = df.orderBy(col("Change").desc()).first()
max_drop = df.orderBy(col("Change").asc()).first()

# Print results
print(f"Average Daily Change: {avg_change:.4f}")
print(f"Largest Gain: {max_gain['Date']} ({max_gain['Change']:.4f})")
print(f"Largest Drop: {max_drop['Date']} ({max_drop['Change']:.4f})")

spark.stop()