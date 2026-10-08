# Group By doanh thu theo category_id bằng Hadoop MapReduce trên HDFS native (Windows), giống scripts/hdfs-mr.sh của bản Docker:
#   .\scripts\native\mr-native.ps1 /data/ecommerce/raw/sample/2019-Oct-r0.01-s21.csv RUN_ID [v1 v2 v3 v4 v5]
# Chương trình gọi qua "hadoop jar" của bản Hadoop cài trên máy; cấu hình lấy từ HADOOP_CONF_DIR
# (fs.defaultFS = hdfs://localhost:8020, mapreduce.framework.name = local -> LocalJobRunner, không YARN).
# Output MR: /data/ecommerce/mr/<RUN_ID>/<variant>/ trên HDFS. Meta, run-manifest, revenue.csv: results\native\<RUN_ID>\.
param([Parameter(Mandatory = $true)][string]$InputPath, [Parameter(Mandatory = $true)][string]$RunId,
      [Parameter(ValueFromRemainingArguments = $true)][string[]]$Variants)
. "$PSScriptRoot\hadoop-env.ps1"
if (-not $Variants) { $Variants = @("v1", "v2", "v3", "v4", "v5") }
$hdfs = "hdfs://localhost:8020"
$jar = Join-Path $env:HADOOP_NATIVE_ROOT "lib\revenue-aggregation.jar"
# Thư mục meta cục bộ không dấu cách (đường dẫn truyền qua hadoop.cmd), sao về repo khi xong.
$meta = Join-Path $env:HADOOP_NATIVE_ROOT "runs\$RunId\meta"
$metaUri = "file:///" + $meta.Replace("\", "/")
$out = "$hdfs/data/ecommerce/mr/$RunId"
$repoOut = Join-Path $RepoRoot "results\native\$RunId"
if (Test-Path $meta) { throw "Chọn RUN_ID khác: $meta đã tồn tại" }
New-Item -ItemType Directory -Force $meta | Out-Null
# Hadoop ghi log INFO ra stderr; với "Stop", PowerShell 5.1 coi đó là lỗi. Từ đây kiểm tra bằng mã thoát.
$ErrorActionPreference = "Continue"
function Tool([string]$tool) {
  & hadoop jar $jar "vn.edu.bigdata.revenue.cli.$tool" @args 2>&1 | ForEach-Object { "$_" }
  if ($LASTEXITCODE -ne 0) { throw "$tool thất bại (mã $LASTEXITCODE)" }
}
function Step([string]$label, [scriptblock]$body) {
  $sw = [Diagnostics.Stopwatch]::StartNew()
  & $body
  "{0}`t{1}" -f $label, $sw.ElapsedMilliseconds | Add-Content (Join-Path $meta "timings.tsv")
}

Step preflight { Tool DatasetTool preflight --input "$hdfs$InputPath" --manifest "$metaUri/input.json" --report "$metaUri/preflight.json" }
$dictionary = @()
if ($Variants -contains "v4" -or $Variants -contains "v5") {
  Step profile { Tool DatasetTool profile --manifest "$metaUri/input.json" --output "$metaUri/profile.json" --dictionary "$metaUri/groups.json" }
  $dictionary = @("--dictionary", "$metaUri/groups.json")
}
foreach ($v in $Variants) {
  Step $v { Tool RevenueTool --variant $v --manifest "$metaUri/input.json" --preflight "$metaUri/preflight.json" --output "$out/$v" --reducers 2 @dictionary }
  & hdfs dfs -get "$out/$v/run-manifest.json" (Join-Path $meta "$v-run-manifest.json") 2>$null
}
foreach ($v in $Variants | Select-Object -Skip 1) { Tool DatasetTool compare --left "$out/$($Variants[0])" --right "$out/$v" }
Tool DatasetTool export --input "$out/$($Variants[0])" --output "$out/revenue.csv"
& hdfs dfs -ls -R "/data/ecommerce/mr/$RunId" 2>$null
$local = Join-Path $env:HADOOP_NATIVE_ROOT "runs\$RunId\revenue.csv"
& hdfs dfs -get "$out/revenue.csv" $local 2>$null
New-Item -ItemType Directory -Force $repoOut | Out-Null
Copy-Item -Recurse $meta $repoOut -Force
Copy-Item $local $repoOut -Force
Get-Content (Join-Path $meta "timings.tsv")
