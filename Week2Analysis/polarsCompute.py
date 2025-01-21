#!/usr/bin/env python
import sys
import time
import polars as pl;

def parseCSV(startTime, endTime):
    parquet_file = "data.parquet"
    
    TimerStart = time.perf_counter_ns()
    lazy_df = pl.scan_parquet(parquet_file)
    filtered_df = lazy_df.filter((pl.col("timestamp") >= startTime) & (pl.col("timestamp") <= endTime)
                            )
    pixel_df = filtered_df.group_by("pixel_color").agg(pl.count("pixel_color").alias("color_count"))
    coords_df = filtered_df.group_by("coordinate").agg(pl.count("coordinate").alias("coord_count"))
    color_df = pixel_df.collect()
    coords_df = coords_df.collect()
    color_count = color_df.filter(pl.col("color_count").max() == pl.col("color_count"))
    coords_count = coords_df.filter(pl.col("coord_count").max() == pl.col("coord_count"))
    most_used_color = color_count["pixel_color"][0]
    most_used_count = str(color_count["color_count"][0])
    most_used_coord = coords_count["coordinate"][0]
    coord_use_count = str(coords_count["coord_count"][0])
    TimerEnd = time.perf_counter_ns()
    timeTaken = TimerEnd-TimerStart
    print("The process took " + str(timeTaken) + " to complete")
    print("The most used color was " + most_used_color + " which occured " + most_used_count + " times.")
    print("The most used coordinate pair was " + most_used_coord + " which occured " + coord_use_count + " times.")

def main():
    args = sys.argv[1:]
    if(len(args) == 4):
        startTime = args[0] + " " + args[1]
        endTime = args[2] + " " + args[3]
        if(startTime < endTime):
            parseCSV(startTime=startTime, endTime=endTime)
        else:
            print("end time is before start time")
    elif(len(args) < 4):
        print("Too few arguments, provide a start day, start hour, end day, and end hour.")
    elif(len(args) > 4):
        print("Too many arguments, provide only a start day, start hour, end day, and end hour.")
    return 0


if __name__ == "__main__":
    main()