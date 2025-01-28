import polars as pl
import pandas as pd
import os
# Dictionary to map original user IDs to new UUIDs
user_id_map = {}
uuid_counter = 1

# Function to get or create a new UUID for a user ID
def get_new_uuid(original_id):
    global uuid_counter
    if original_id not in user_id_map:
        user_id_map[original_id] = uuid_counter
        uuid_counter += 1
    return user_id_map[original_id]

# Batch processing function
def process_batch(batch):
    print("test")
    # Map user IDs to new UUIDs
    batch = batch.with_columns(
        pl.col("user_id").map_elements(get_new_uuid, return_dtype= int).alias("new_user_id")
    )
    # Split coordinates into x and y columns
    batch = batch.with_columns(
        pl.col("coordinate")
        .str.replace_all(r"[()]", "")
        .str.split_exact(",", 1)
        .alias("split_coordinates")
    )
    batch = batch.with_columns(
        pl.col("split_coordinates").struct.field("field_0").alias("x_coord"),
        pl.col("split_coordinates").struct.field("field_1").alias("y_coord")
    )
    return batch.select(["timestamp", "new_user_id", "pixel_color", "x_coord", "y_coord"])

# Read and process the CSV in batches
batch_size = 10000  # Define the number of rows per batch
reader = pl.read_csv_batched("2022_place_canvas_history.csv", batch_size=10000)  # Lazily load the CSV for streaming

batches = reader.next_batches(100) 
while batches:
    df_current_batches = pl.concat(batches)

    processed_batch = process_batch(df_current_batches)
    processed_batch.to_pandas().to_csv("new_data.csv", mode='a', header=not os.path.exists("new_data.csv"), index=False)
    batches = reader.next_batches(100) 

pl.scan_csv("new_data.csv").sink_parquet("data.parquet")
print(f"Processed large CSV file 2022_place_canvas_history.csv and saved it to Parquet as data.parquet.")
