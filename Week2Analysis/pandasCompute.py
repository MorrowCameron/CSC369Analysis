#!/usr/bin/env python
import sys
import time
import pandas as pd;
import pyarrow.dataset as ds;


def parseCSV(startTime, endTime):
    parquet_file = "data.parquet"
    
    TimerStart = time.perf_counter_ns()
    dataset = ds.dataset(parquet_file, format="parquet")
    filtered_table = dataset.to_table(
        filter=(ds.field("timestamp") >= startTime) & 
            (ds.field("timestamp") <= endTime),
        columns=["timestamp", "pixel_color", "coordinate"]
    )
    filtered_df = filtered_table.to_pandas()
    color_counts = filtered_df['pixel_color'].value_counts()
    coords_count = filtered_df['coordinate'].value_counts()
    most_used_color = color_counts.idxmax()
    most_used_count = color_counts.max()
    most_used_coords = coords_count.idxmax()
    count_max_coords = coords_count.max()
    TimerEnd = time.perf_counter_ns()
    timeTaken = TimerEnd-TimerStart
    print("The process took " + str(timeTaken) + " to complete")
    print("The most used color was " + most_used_color + " which occured " + str(most_used_count) + " times.")
    print("The most used coordinate pair was " + most_used_coords + " which occured " + str(count_max_coords) + " times.")

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