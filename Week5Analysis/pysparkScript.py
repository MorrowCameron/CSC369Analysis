from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, date_trunc, countDistinct, lit, first
from PIL import Image
import webcolors

# Initialize PySpark Session
spark = SparkSession.builder.appName("rplace_analysis").getOrCreate()

# Load data from Parquet
df = spark.read.parquet("data.parquet")

# Convert timestamp column to a proper timestamp type
df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))

# Define color mappings
additionalColors = color_mappings = {
    "#000000": "black",
    "#FF4500": "red",
    "#FFD635": "yellow",
    "#3690EA": "blue",
    "#2450A4": "dark blue",
    "#00A368": "dark green",
    "#FFA800": "orange",
    "#BE0039": "dark red",
    "#B44AC0": "purple",
    "#51E9F4": "light blue",
    "#7EED56": "light green",
    "#898D90": "gray",
    "#FF99AA": "light pink",
    "#D4D7D9": "light gray",
    "#9C6926": "brown",
    "#811E9F": "dark purple",
    "#FFFFFF": "white",
    "#493AC1": "indigo",
    "#6D482F": "dark brown",
    "#6A5CFF": "periwinkle",
    "#FF3881": "pink",
    "#00CC78": "green",
    "#00756F": "dark teal",
    "#009EAA": "teal",
    "#FFB470": "beige",
    "#515252": "dark gray",
    "#6D001A": "burgandy",
    "#DE107F": "magenta",
    "#FFF8B8": "pale yellow",
    "#E4ABFF": "pale purple",
    "#94B3FF": "lavender",
    "#00CCC0": "light teal"
}

# Function to get closest color name
def get_closest_color_name(hex_code):
    try:
        # Try to find an exact match
        return webcolors.hex_to_name(hex_code)
    except ValueError:
        # If no exact match, find the closest named color
        return additionalColors[hex_code]

# **Filter dataset for a specific timeframe**
df_filtered = df.filter(
    (col("timestamp") >= lit("2022-04-04 22:48")) & 
    (col("timestamp") <= lit("2022-04-04 23:18"))
)

df_latest_pixels = df_filtered.groupBy("x_coord", "y_coord").agg(
    spark_max("timestamp").alias("max_timestamp"),
    first('pixel_color').alias("pixel_color"))

df_latest_pixels.join(
    df_filtered.alias("original"),
    (df_latest_pixels["x_coord"] == col("original.x_coord")) & 
    (df_latest_pixels["y_coord"] == col("original.y_coord")) & 
    (df_latest_pixels["max_timestamp"] == col("original.timestamp"))
).select("original.x_coord", "original.y_coord", "original.pixel_color", "original.timestamp")

df_latest_pixels.printSchema()
df_latest_pixels_pd = df_latest_pixels.toPandas()

# **Function to Save Image**
def saveImage(df, name, background):
    width = df['x_coord'].max() + 1
    height = df['y_coord'].max() + 1
    # Create a blank image
    image = Image.new('RGB', (width, height), color=background)

    # Convert hex colors to RGB and populate image
    pixels = image.load()
    for _, row in df.iterrows():
        x, y = row['x_coord'], row['y_coord']
        color = tuple(int(row['pixel_color'][i:i+2], 16) for i in (1, 3, 5))  # Convert hex to RGB
        pixels[x, y] = color

    # Show and save image
    image.show()  # Display image
    image.save(name)

# **Save black-background image**
saveImage(df_latest_pixels_pd, "PostPurge30.png", background="black")

# **Find a minute where only white pixels were placed**
df_whiteout = df_filtered.withColumn("minute", date_trunc("minute", col("timestamp"))) \
    .groupBy("minute").agg(
        countDistinct("pixel_color").alias("color_count")
    ).filter((col("color_count") == 1)) \
    .orderBy("minute").limit(100)

# Show results
df_whiteout.show()

# Stop Spark session
spark.stop()