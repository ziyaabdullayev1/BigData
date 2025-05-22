@echo off
echo.
echo ========== 90th PERCENTILE ==========
cd /d %~dp0

call hadoop fs -rm -r /kaggle_output_percentile
echo [*] Continuing even if delete failed...

call hadoop jar "C:\hadoop-2.7.7\share\hadoop\tools\lib\hadoop-streaming-2.7.7.jar" -input /kaggleinput/kaggle_numbers.txt -output /kaggle_output_percentile -mapper "python mapper.py" -reducer "python stats_reducer.py percentile"

echo.
call hadoop fs -cat /kaggle_output_percentile/part-00000
pause
