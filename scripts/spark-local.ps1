# Build và chạy các job Spark Java (package vn.edu.bigdata.revenue.spark) trên Windows, HDFS trong Docker.
#   .\scripts\spark-local.ps1 build [mvn args]            # mvn -pl bigdata -Pspark package (test Spark + MR) với JDK 17/21
#   .\scripts\spark-local.ps1 submit JOB [--name value]   # JOB: revenue | etl | metrics | features | labels | publish (xem SparkTool)
# Cấu hình máy: config\spark-local.env (JAVA_HOME, HADOOP_HOME có bin\winutils.exe, HDFS_URI, ...).
# Runtime Spark: spark-submit của PySpark trong .venv (cùng phiên bản với spark.version trong bigdata/pom.xml).
param([Parameter(Mandatory = $true)][ValidateSet("build", "submit")][string]$Command,
      [Parameter(ValueFromRemainingArguments = $true)][string[]]$Rest)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$envFile = Join-Path $root "config\spark-local.env"
if (-not (Test-Path $envFile)) { throw "Thiếu ${envFile} (sao chép từ config\spark-local.env.example)" }
# Giá trị trong file ghi đè môi trường (JAVA_HOME của máy có thể là JDK không hợp lệ), trừ các tham số
# Spark đã đặt sẵn trong phiên: scripts\spark-bench.ps1 dùng chúng để thay master/partitions khi đo.
$sessionKeys = @("SPARK_MASTER", "SPARK_DRIVER_MEMORY", "SPARK_SHUFFLE_PARTITIONS") |
  Where-Object { [Environment]::GetEnvironmentVariable($_) }
foreach ($line in Get-Content $envFile -Encoding UTF8) {
  $line = $line.Trim()
  if ($line -and -not $line.StartsWith("#") -and $line.Contains("=")) {
    $key, $value = $line.Split("=", 2)
    if ($sessionKeys -notcontains $key.Trim()) { Set-Item -Path "Env:$($key.Trim())" -Value $value.Trim() }
  }
}
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
if ($env:HADOOP_HOME) { $env:PATH = "$env:HADOOP_HOME\bin;$env:PATH" }
# spark-submit.cmd cần SPARK_HOME (nếu thiếu sẽ gọi python để tự dò); dùng PySpark trong .venv.
$env:SPARK_HOME = Join-Path $root ".venv\Lib\site-packages\pyspark"
$env:PATH = "$root\.venv\Scripts;$env:PATH"
$sha = (git rev-parse --short HEAD 2>$null)
$env:PTIT_GIT_SHA = if ($sha) { $sha } else { "nogit" }
if (-not $Rest) { $Rest = @() }
# PowerShell 5.1 coi stderr của lệnh native là lỗi khi Stop; từ đây dùng mã thoát của lệnh.
$ErrorActionPreference = "Continue"

if ($Command -eq "build") {
  $mvn = Join-Path $root ".tools\apache-maven-3.9.11\bin\mvn.cmd"
  if (-not (Test-Path $mvn)) { $mvn = "mvn" }
  & $mvn "-Dmaven.repo.local=$root\.build-cache\m2" -B -pl bigdata -Pspark package @Rest
  exit $LASTEXITCODE
}

$jar = Join-Path $root "bigdata\target\revenue-aggregation-spark.jar"
if (-not (Test-Path $jar)) { Write-Error "Chưa có ${jar}: chạy '.\scripts\spark-local.ps1 build' trước"; exit 2 }
$master = if ($env:SPARK_MASTER) { $env:SPARK_MASTER } else { "local[2]" }
$memory = if ($env:SPARK_DRIVER_MEMORY) { $env:SPARK_DRIVER_MEMORY } else { "1g" }
$partitions = if ($env:SPARK_SHUFFLE_PARTITIONS) { $env:SPARK_SHUFFLE_PARTITIONS } else { "8" }
& (Join-Path $root ".venv\Scripts\spark-submit.cmd") --master $master --driver-memory $memory `
  --conf "spark.sql.shuffle.partitions=$partitions" `
  --class vn.edu.bigdata.revenue.spark.SparkTool $jar @Rest
exit $LASTEXITCODE
