$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "C:\hadoop\bin;$env:PATH"
Write-Host "Starting stream_aggregations..." -ForegroundColor Green
python spark/jobs/stream_aggregations.py