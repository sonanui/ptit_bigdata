# Tạo mẫu tất định từ một file raw trên HDFS native (DatasetTool sample), giống scripts/hdfs-sample.sh của bản Docker:
#   .\scripts\native\sample-native.ps1 /data/ecommerce/raw/2019-Oct.csv 0.01 21
# Lần đầu: preflight quét toàn bộ file nguồn, ghi manifest (fingerprint) vào <root>\runs\source-<tên>\; lần sau dùng lại.
param([Parameter(Mandatory = $true)][string]$SourcePath, [Parameter(Mandatory = $true)][string]$Rate, [Parameter(Mandatory = $true)][string]$Seed)
. "$PSScriptRoot\hadoop-env.ps1"
$hdfs = "hdfs://localhost:8020"
$jar = Join-Path $env:HADOOP_NATIVE_ROOT "lib\revenue-aggregation.jar"
$name = [IO.Path]::GetFileNameWithoutExtension($SourcePath)
$meta = Join-Path $env:HADOOP_NATIVE_ROOT "runs\source-$name"
$metaUri = "file:///" + $meta.Replace("\", "/")
$sample = "$hdfs/data/ecommerce/raw/sample/$name-r$Rate-s$Seed.csv"
New-Item -ItemType Directory -Force $meta | Out-Null
# Hadoop ghi log INFO ra stderr; với "Stop", PowerShell 5.1 coi đó là lỗi. Kiểm tra bằng mã thoát.
$ErrorActionPreference = "Continue"
function Tool([string]$tool) {
  & hadoop jar $jar "vn.edu.bigdata.revenue.cli.$tool" @args 2>&1 | ForEach-Object { "$_" }
  if ($LASTEXITCODE -ne 0) { throw "$tool thất bại (mã $LASTEXITCODE)" }
}
if (-not (Test-Path (Join-Path $meta "input.json"))) {
  $sw = [Diagnostics.Stopwatch]::StartNew()
  Tool DatasetTool preflight --input "$hdfs$SourcePath" --manifest "$metaUri/input.json" --report "$metaUri/preflight.json"
  "preflight_ms=$($sw.ElapsedMilliseconds)" | Tee-Object -Append (Join-Path $meta "timings.txt")
}
$sw = [Diagnostics.Stopwatch]::StartNew()
Tool DatasetTool sample --manifest "$metaUri/input.json" --output $sample --rate $Rate --seed $Seed
"sample_r${Rate}_s${Seed}_ms=$($sw.ElapsedMilliseconds)" | Tee-Object -Append (Join-Path $meta "timings.txt")
& hdfs dfs -ls /data/ecommerce/raw/sample 2>$null
