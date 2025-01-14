#!/usr/bin/env python
import sys
import time
import datetime


def parseCSV(startTime, endTime):
    TimerStart = time.perf_counter_ns()
    maxColorAndOccurances = ("Empty", 0)
    colorAndOccurances = {}
    maxPixelAndOccurances = ("Empty", 0)
    pixelAndOccurances = {}
    with open('2022_place_canvas_history.csv', 'r') as file:
        lineNum = 0
        for line in file:
            if(lineNum > 0):
                entry = line.strip()
                # print(entry)
                columnArray = entry.split(",")
                timeCol = columnArray[0]
                timeCol = timeCol.split(" UTC")
                # print(timeCol[0])
                timestamp = 0
                try: 
                    timestamp = datetime.datetime.strptime(timeCol[0], '%Y-%m-%d %H:%M:%S.%f').timestamp()
                except:
                    timestamp = datetime.datetime.strptime(timeCol[0], '%Y-%m-%d %H:%M:%S').timestamp()
                # print("Time " + str(timestamp))
                # print("Start Time " + str(startTime))
                # print("End Time " + str(endTime))
                if(timestamp >= startTime and timestamp <= endTime):
                    print("HEREHREHERE")
                    color = columnArray[2]
                    print(color)
                    pixel = columnArray[3] + "," + columnArray[4]
                    print(pixel)
                    numColorOccurances = colorAndOccurances.get(color, 0) + 1
                    if(numColorOccurances > maxColorAndOccurances[1]):
                        maxColorAndOccurances = (color, numColorOccurances)
                    colorAndOccurances[color] = numColorOccurances
                    if(maxColorAndOccurances[1] == 0):
                        maxColorAndOccurances = (color, 1)
                    numPixelOccurances = pixelAndOccurances.get(pixel, 0) + 1
                    if(numPixelOccurances > maxPixelAndOccurances[1]):
                        maxPixelAndOccurances = (pixel, numPixelOccurances)
                    pixelAndOccurances[pixel] = numPixelOccurances
                    if(maxPixelAndOccurances[1] == 0):
                        maxPixelAndOccurances = (pixel, 1)
            lineNum += 1
            # if(lineNum == 2):
            #     break
        file.close()
    print("The most used color is " + maxColorAndOccurances[0] + " which was used " + str(maxColorAndOccurances[1]) + " times.")
    print("The most used pixel position is " + maxPixelAndOccurances[0] + " which was used " + str(maxPixelAndOccurances[1]) + " times.")
    TimerEnd = time.perf_counter_ns()
    timeTaken = TimerEnd-TimerStart
    print("The process took " + str(timeTaken) + " to complete")

def main():
    args = sys.argv[1:]
    if(len(args) == 4):
        startTime = args[0] + " " + args[1]
        endTime = args[2] + " " + args[3]
        print(startTime)
        print(endTime)
        startDateTime = datetime.datetime.strptime(startTime, '%Y-%m-%d %H').timestamp()
        print(startDateTime)
        endDateTime = datetime.datetime.strptime(endTime, '%Y-%m-%d %H').timestamp()
        print(endDateTime)
        if(startDateTime < endDateTime):
            print("Date is correct")
            parseCSV(startTime=startDateTime, endTime=endDateTime)
        else:
            print("end time is before start time")
    elif(args < 4):
        print("Too few arguments, provide a start day, start hour, end day, and end hour.")
    elif(args > 4):
        print("Too many arguments, provide only a start day, start hour, end day, and end hour.")

    return 0


if __name__ == "__main__":
    main()