import duckdb
import pandas as pd
import webcolors
import sys
import time

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

def findColors(con):
    query = """SELECT pixel_color, 
                  COUNT(DISTINCT new_user_id) as unique_user_count
            FROM 
                  rplacelimited
            GROUP BY
                  pixel_color
            ORDER BY
                  unique_user_count DESC"""
    colorRanking = con.execute(query).fetch_df()
    colorRanking['new_colors'] = colorRanking['pixel_color'].apply(get_closest_color_name)
    print(colorRanking[['new_colors', 'unique_user_count']])

def findPercentiles(con):
    query = """WITH user_pixel_counts AS (
    SELECT
        new_user_id,
        COUNT(*) AS pixel_count
    FROM
        rplacelimited
    GROUP BY
        new_user_id
    )
    SELECT
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY pixel_count) AS percentile_50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pixel_count) AS percentile_75,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY pixel_count) AS percentile_90,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY pixel_count) AS percentile_99
    FROM
        user_pixel_counts;
    """

    print(con.execute(query).fetch_df())

def findFirstPlacement(con, startTime, endTime):
    start_time = startTime + ":00:00"
    end_time = endTime + ":00:00"
    con.execute(f"CREATE TABLE rplace AS SELECT DISTINCT new_user_id, timestamp FROM 'data.parquet' WHERE timestamp < '{end_time}'")
    con.execute(f"CREATE VIEW previously_placed_list AS SELECT DISTINCT(new_user_id), timestamp FROM rplace WHERE timestamp < '{start_time}'")
    con.execute(f"CREATE VIEW placed_list AS SELECT DISTINCT(new_user_id), timestamp FROM rplace WHERE timestamp BETWEEN '{start_time}' AND '{end_time}'")
    print(con.execute("SELECT COUNT(p1.new_user_id) from placed_list p1 LEFT JOIN previously_placed_list p2 ON p1.new_user_id = p2.new_user_id where p2.new_user_id IS NULL").fetch_df())

def findSessionLength(con):
    query = f"""
    WITH user_activity AS (
        SELECT
            new_user_id,
            CAST(timestamp AS DATETIME) AS timestamp,
            LAG(CAST(timestamp AS DATETIME)) OVER (PARTITION BY new_user_id ORDER BY CAST(timestamp AS DATETIME)) AS previous_timestamp
        FROM
            rplacelimited
        ),
    session_identification AS (
        SELECT
            new_user_id,
            timestamp,
            previous_timestamp,
            CASE
                WHEN previous_timestamp IS NULL OR (EXTRACT(EPOCH FROM timestamp) - EXTRACT(EPOCH FROM previous_timestamp)) > 900 THEN 1
                ELSE 0
            END AS is_new_session
        FROM
            user_activity
    ),
    session_durations AS (
        SELECT
            new_user_id,
            session_id,
            (EXTRACT(EPOCH FROM max(timestamp)) - EXTRACT(EPOCH FROM min(timestamp)))
 AS session_length
        FROM (
            SELECT
                new_user_id,
                timestamp,
                SUM(is_new_session) OVER (PARTITION BY new_user_id ORDER BY timestamp) AS session_id
            FROM
                session_identification
        ) session_grouped
        GROUP BY
            new_user_id,
            session_id
    ),
    filtered_sessions AS (
        SELECT
            new_user_id,
            AVG(session_length) AS average_session_length
        FROM
            session_durations
        GROUP BY
            new_user_id
        HAVING
            COUNT(*) > 1 -- Users must have more than one session
    )
    SELECT
        AVG(average_session_length) AS overall_average_session_length
    FROM
        filtered_sessions;
    """
    print(con.execute(query).fetch_df())


def parseFile(startTime, endTime):
    con = duckdb.connect()
    limitedTable = f"""
            CREATE TABLE rplacelimited AS
            SELECT *
            FROM 'data.parquet'
            WHERE timestamp BETWEEN '{startTime}' AND '{endTime}';
            """
    con.execute(limitedTable)
    starting = time.perf_counter_ns()
    findColors(con)
    findSessionLength(con)
    findPercentiles(con)
    findFirstPlacement(con, startTime=startTime, endTime=endTime)
    print("This took " + str((time.perf_counter_ns() - starting)/1000000000) + " seconds")
    con.close()
    
def main():
    args = sys.argv[1:]
    if(len(args) == 4):
        startTime = args[0] + " " + args[1]
        endTime = args[2] + " " + args[3]
        if(startTime < endTime):
            parseFile(startTime=startTime, endTime=endTime)
        else:
            print("end time is before start time")
    elif(len(args) < 4):
        print("Too few arguments, provide a start day, start hour, end day, and end hour.")
    elif(len(args) > 4):
        print("Too many arguments, provide only a start day, start hour, end day, and end hour.")
    return 0


if __name__ == "__main__":
    main()