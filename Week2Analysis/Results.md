## DuckDB Result
### 2024-04-01 12 to 2024-04-01 13
#### Execution Time:
The process took 507147625ns to complete
#### Most used Color:
The most common color was #FFFFFF which occured 4 times.
#### Most used Coordinate:
The most common coordinate pair was 5,29 which occured 4 times.

### 2024-04-01 12 to 2024-04-01 15
#### Execution Time:
The process took 442287250ns to complete
#### Most used Color:
The most common color was #000000 which occured 316225 times.
#### Most used Coordinate:
The most common coordinate pair was 0,0 which occured 1483 times.

### 2024-04-01 12 to 2024-04-01 18
#### Execution Time:
The process took 706750750 to complete
#### Most used Color:
The most common color was #000000 which occured 1068290 times.
#### Most used Coordinate:
The most common coordinate pair was 859,766 which occured 5614 times.

## Pandas Result
### 2024-04-01 12 to 2024-04-01 13
#### Execution Time:
The process took 63696000ns to complete
#### Most used Color:
The most common color was #FFFFFF which occured 4 times.
#### Most used Coordinate:
The most common coordinate pair was 5,29 which occured 4 times.

### 2024-04-01 12 to 2024-04-01 15
#### Execution Time:
The process took 626453000ns to complete
#### Most used Color:
The most common color was #000000 which occured 316225 times.
#### Most used Coordinate:
The most common coordinate pair was 0,0 which occured 1483 times.

### 2024-04-01 12 to 2024-04-01 18
#### Execution Time:
The process took 1704586875ns to complete
#### Most used Color:
The most common color was #000000 which occured 1068290 times.
#### Most used Coordinate:
The most common coordinate pair was 859,766 which occured 5614 times.

## Polars Result
### 2024-04-01 12 to 2024-04-01 13
#### Execution Time:
The process took 50715917ns to complete
#### Most used Color:
The most common color was #FFFFFF which occured 4 times.
#### Most used Coordinate:
The most common coordinate pair was 5,29 which occured 4 times.

### 2024-04-01 12 to 2024-04-01 15
#### Execution Time:
The process took 177043250ns to complete
#### Most used Color:
The most common color was #000000 which occured 316225 times.
#### Most used Coordinate:
The most common coordinate pair was 0,0 which occured 1483 times.

### 2024-04-01 12 to 2024-04-01 18
#### Execution Time:
The process took 405004791ns to complete
#### Most used Color:
The most common color was #000000 which occured 1068290 times.
#### Most used Coordinate:
The most common coordinate pair was 859,766 which occured 5614 times.

## Takeaways
I found duckdb to be easiest to use due to my experience with SQL allowing me to feel I had the best range of options to analyze the data. Additionally, the lack of memory issues was very nice, as both pandas and polar required me to use an external library (pyarrow for pandas) or lazy load the data in order to not crash the program. That being said, pandas and polars were both very nice for quick variations on data locally, with extremely nice built in formatting and useful methods. This was obviously much faster than my initial implementation, but I also converted my file to parquet, which had an additional impact on runtime.