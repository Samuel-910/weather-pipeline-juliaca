@echo off
set "HADOOP_HOME=%~dp0hadoop"
set "hadoop.home.dir=%~dp0hadoop"
set "PATH=%HADOOP_HOME%\bin;%PATH%"

echo Usando HADOOP_HOME=%HADOOP_HOME%
echo.

call .\venv\Scripts\activate.bat
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 s7_spark\streaming_job.py
