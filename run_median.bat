@echo off
echo Checking Python environment...
python check_env.py

echo Starting Hadoop job...
set HADOOP_HOME=C:\hadoop
set PATH=%HADOOP_HOME%\bin;%PATH%
set PYTHON_PATH=C:\Users\abdul\anaconda3\python.exe

:: Delete output directory if it exists
%HADOOP_HOME%\bin\hadoop fs -rm -r /kaggle_output_median

:: Create input directory and upload data
%HADOOP_HOME%\bin\hadoop fs -mkdir -p /kaggleinput
%HADOOP_HOME%\bin\hadoop fs -put -f kaggleinput\ecom_purchase_amounts.txt /kaggleinput

:: Run MapReduce job
%HADOOP_HOME%\bin\hadoop jar %HADOOP_HOME%\share\hadoop\tools\lib\hadoop-streaming-2.7.7.jar ^
-file mapper.py ^
-mapper "%PYTHON_PATH% mapper.py" ^
-file stats_reducer.py ^
-reducer "%PYTHON_PATH% stats_reducer.py median" ^
-input /kaggleinput/ecom_purchase_amounts.txt ^
-output /kaggle_output_median

:: Display results
%HADOOP_HOME%\bin\hadoop fs -cat /kaggle_output_median/part-*

echo.
pause
