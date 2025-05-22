@echo off
title Big Data Statistical Functions Menu
color 0A

:menu
cls
echo ================================================
echo     Big Data Analysis - Hadoop Job Launcher
echo ================================================
echo.
echo  [1] Median
echo  [2] Standard Deviation
echo  [3] Min-Max Normalization
echo  [4] 90th Percentile
echo  [5] Skewness
echo  [6] Exit
echo.
set /p choice=Enter your choice [1-6]: 

if "%choice%"=="1" call run_median.bat
if "%choice%"=="2" call run_stddev.bat
if "%choice%"=="3" call run_minmax.bat
if "%choice%"=="4" call run_percentile.bat
if "%choice%"=="5" call run_skewness.bat
if "%choice%"=="6" exit

pause
goto menu
