@echo off
echo.
echo ========== MIN-MAX NORMALIZATION ==========
cd /d %~dp0

call hadoop fs -rm -r /kaggle_output_minmax
echo [*] Continuing even if delete failed...

call hadoop jar "C:\hadoop-2.7.7\share\hadoop\tools\lib\hadoop-streaming-2.7.7.jar" -input /kaggleinput/kaggle_numbers.txt -output /kaggle_output_minmax -mapper "python mapper.py" -reducer "python stats_reducer.py minmax"

echo.
call hadoop fs -cat /kaggle_output_minmax/part-00000
pause
