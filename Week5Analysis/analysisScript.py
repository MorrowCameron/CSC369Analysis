import duckdb
import pandas as pd
import webcolors
from PIL import Image


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

def get_closest_color_name(hex_code):
    try:
        # Try to find an exact match
        return webcolors.hex_to_name(hex_code)
    except ValueError:
        # If no exact match, find the closest named color
        return additionalColors[hex_code]
    

def saveImage(df, name):
    width = df['x_coord'].max() + 1
    height = df['y_coord'].max() + 1
    # Create a blank image
    image = Image.new('RGB', (width, height), color='white')

    # Convert hex colors to RGB and populate image
    pixels = image.load()
    for _, row in df.iterrows():
        x, y = row['x_coord'], row['y_coord']
        color = tuple(int(row['pixel_color'][i:i+2], 16) for i in (1, 3, 5))  # Convert hex to RGB
        pixels[x, y] = color

    # Show and save image
    image.show()  # Display image
    image.save(name)

def saveImageBlack(df, name):
    width = df['x_coord'].max() + 1
    height = df['y_coord'].max() + 1
    # Create a blank image
    image = Image.new('RGB', (width, height), color='black')

    # Convert hex colors to RGB and populate image
    pixels = image.load()
    for _, row in df.iterrows():
        x, y = row['x_coord'], row['y_coord']
        color = tuple(int(row['pixel_color'][i:i+2], 16) for i in (1, 3, 5))  # Convert hex to RGB
        pixels[x, y] = color

    # Show and save image
    image.show()  # Display image
    image.save(name)

con = duckdb.connect()
limitedTable = f"""
        CREATE TABLE rplacelimited AS
        SELECT timestamp, x_coord, y_coord, pixel_color
        FROM 'data.parquet'
        WHERE timestamp BETWEEN '2022-04-04 22:48' AND '2022-04-04 23:48';
        """
con.execute(limitedTable)

day1query = """
    SELECT r.x_coord, r.y_coord, r.pixel_color, r.timestamp
    FROM rplacelimited r
    JOIN (
        SELECT x_coord, y_coord, MAX(timestamp) AS max_timestamp
        FROM rplacelimited
        GROUP BY x_coord, y_coord
    ) latest_pixel
    ON r.x_coord = latest_pixel.x_coord
    AND r.y_coord = latest_pixel.y_coord
    AND r.timestamp = latest_pixel.max_timestamp
    ORDER BY r.x_coord, r.y_coord;
    """

day1Snapshot = con.execute(day1query).fetchdf()

saveImageBlack(day1Snapshot, 'PostPerge60.png')

# saveImage(day1Snapshot, 'BeforePurge.png')

# Found through manual binary search due to low RAM 
findWhiteOutTime = f"""
    SELECT DISTINCT DATE_TRUNC('minute', CAST(timestamp AS TIMESTAMP)) AS minute, pixel_color
    FROM rplacelimited
    GROUP BY minute, pixel_color
    HAVING COUNT(DISTINCT pixel_color) = 1
    ORDER BY minute 
    limit 100
    """

print(con.execute(findWhiteOutTime).fetch_df())

con.close()