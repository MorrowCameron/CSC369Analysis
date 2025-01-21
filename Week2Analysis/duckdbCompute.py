#!/usr/bin/env python
import sys
import time
import duckdb;


def parseCSV(startTime, endTime):
    parquet_file = "data.parquet"
    TimerStart = time.perf_counter_ns()
    con = duckdb.connect()
    color = con.execute(f"""SELECT pixel_color, COUNT(*) as color_count
                 FROM '{parquet_file}'
                 WHERE timestamp BETWEEN '{startTime}' and '{endTime}'
                 GROUP BY pixel_color
                 ORDER BY color_count DESC
                 LIMIT 1;""").fetch_df()
    coord = con.execute(f"""SELECT coordinate, COUNT(*) as coord_count
                 FROM '{parquet_file}'
                 WHERE timestamp BETWEEN '{startTime}' and '{endTime}'
                 GROUP BY coordinate
                 ORDER BY coord_count DESC
                 LIMIT 1;""").fetch_df()
    TimerEnd = time.perf_counter_ns()
    timeTaken = TimerEnd-TimerStart
    print("The process took " + str(timeTaken) + " to complete")
    print("The most common color was " + color['pixel_color'][0] + " which occured " + str(color['color_count'][0]) + " times.")
    print("The most common coordinate pair was " + coord['coordinate'][0] + " which occured " + str(coord['coord_count'][0]) + " times.")
    con.close()

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