$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "C:\hadoop\bin;$env:PATH"
Write-Host "Starting stream_ingest..." -ForegroundColor Green
python spark/jobs/stream_ingest.py