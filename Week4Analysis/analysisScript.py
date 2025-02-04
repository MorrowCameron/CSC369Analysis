import duckdb
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


con = duckdb.connect()
limitedTable = f"""
        CREATE TABLE rplacelimited AS
        SELECT *
        FROM 'data.parquet';
        """
con.execute(limitedTable)

query = """SELECT x_coord, y_coord, COUNT(*) AS placement_count
               FROM rplacelimited
               GROUP BY x_coord, y_coord
               ORDER BY placement_count DESC
               LIMIT 3;"""
colorRanking = con.execute(query).fetch_df()

print(colorRanking)


popularColors = """
    SELECT x_coord, y_coord, pixel_color, COUNT(*) as color_count FROM rplacelimited
    WHERE x_coord = 0 AND y_coord = 0
    GROUP BY x_coord, y_coord, pixel_color
    ORDER BY color_count DESC
"""
zerozerodf = con.execute(popularColors).fetch_df()
zdf = zerozerodf['pixel_color'].apply(get_closest_color_name)
print(zdf)

print(zerozerodf)

popularColors2 = """
    SELECT x_coord, y_coord, pixel_color, COUNT(*) as color_count FROM rplacelimited
    WHERE x_coord = 359 AND y_coord = 564
    GROUP BY x_coord, y_coord, pixel_color
    ORDER BY color_count DESC
"""
threefiveninedf = con.execute(popularColors2).fetch_df()

print(threefiveninedf)

print(threefiveninedf['pixel_color'].apply(get_closest_color_name))

popularColors3 = """
    SELECT x_coord, y_coord, pixel_color, COUNT(*) as color_count FROM rplacelimited
    WHERE x_coord = 349 AND y_coord = 564
    GROUP BY x_coord, y_coord, pixel_color
    ORDER BY color_count DESC
"""
threefourninedf = con.execute(popularColors3).fetch_df()

print(threefourninedf)

print(threefourninedf['pixel_color'].apply(get_closest_color_name))

zeroSnipTable = f"""
        CREATE TABLE zeroTable AS
        SELECT *
        FROM 'data.parquet'
        WHERE x_coord < 250 and y_coord < 50;
        """
con.execute(zeroSnipTable)

day1query = """
    WITH latest_per_day AS (
            SELECT 
                x_coord, 
                y_coord, 
                pixel_color, 
                CAST(timestamp AS DATE) AS day,
                ROW_NUMBER() OVER (
                    PARTITION BY day, x_coord, y_coord 
                    ORDER BY timestamp DESC
                ) AS rank
            FROM zeroTable
            WHERE timestamp BETWEEN '2022-04-01' AND '2022-04-02'
        )
        SELECT x_coord, y_coord, pixel_color, day
        FROM latest_per_day
        WHERE rank = 1
        ORDER BY day, x_coord, y_coord;"""

day1Snapshot = con.execute(day1query).fetchdf()

print(day1Snapshot)
saveImage(day1Snapshot, '(0,0)_Day2.png')

threefivenineSnipTable = f"""
    CREATE TABLE threeFiveNineTable AS
    SELECT *
    FROM 'data.parquet'
    WHERE x_coord BETWEEN 309 AND 409
      AND y_coord BETWEEN 514 AND 614;
"""
con.execute(threefivenineSnipTable)

day1query = """
    WITH latest_per_day AS (
            SELECT 
                x_coord, 
                y_coord, 
                pixel_color, 
                CAST(timestamp AS DATE) AS day,
                ROW_NUMBER() OVER (
                    PARTITION BY day, x_coord, y_coord 
                    ORDER BY timestamp DESC
                ) AS rank
            FROM threeFiveNineTable
            WHERE timestamp BETWEEN '2022-04-04' AND '2022-04-05'
        )
        SELECT x_coord, y_coord, pixel_color, day
        FROM latest_per_day
        WHERE rank = 1
        ORDER BY day, x_coord, y_coord;"""

day1Snapshot = con.execute(day1query).fetchdf()

print(day1Snapshot)

saveImage(day1Snapshot, '(349,564)_Day4.png')