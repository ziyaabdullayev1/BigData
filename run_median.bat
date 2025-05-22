@echo off
echo.
echo ===============================================
echo  Running Hadoop Streaming Job: MEDIAN Function
echo ===============================================
echo.

cd /d %~dp0

:: Try to remove output dir, continue no matter what
echo [*] Cleaning up previous HDFS output...
call hadoop fs -rm -r /kaggle_output_median
echo [*] Continuing even if above failed...

echo.
echo [*] About to run Hadoop Streaming job...
call hadoop jar "C:\hadoop-2.7.7\share\hadoop\tools\lib\hadoop-streaming-2.7.7.jar" -input /kaggleinput/kaggle_numbers.txt -output /kaggle_output_median -mapper "python mapper.py" -reducer "python stats_reducer.py median"

echo.
echo [*] Reading result from HDFS...
call hadoop fs -cat /kaggle_output_median/part-00000

echo.
pause
