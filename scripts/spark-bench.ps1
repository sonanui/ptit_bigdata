# Đo có lặp job Spark A1 (revenue) cho thực nghiệm E4/E5/E6: warmup + lặp, mỗi lần một run_id mới trên HDFS.
#   .\scripts\spark-bench.ps1 -Label e4-d2-rdd-raw -InputPath /data/ecommerce/raw/sample/2019-Oct-r0.1-s21.csv `
#       [-Mode rdd-raw|df-raw|df-curated] [-CuratedRunId ID] [-Master "local[2]"] [-Partitions 8] `
#       [-Warmups 1] [-Repeats 3] [-Reference results\mr\<run>\revenue.csv]
# Mỗi lần chạy được so khớp chính xác với -Reference (scripts/compare_revenue.py); lần nào khác đều bị đánh dấu lỗi.
# Kết quả: docs\evidence\bench\spark\<Label>\{runs.json, summary.csv}; log spark-submit: results\spark-bench\<Label>\.
param([Parameter(Mandatory = $true)][string]$Label,
      [string]$InputPath,
      [ValidateSet("rdd-raw", "df-raw", "df-curated")][string]$Mode = "rdd-raw",
      [string]$CuratedRunId,
      [string]$Master = "local[2]",
      [int]$Partitions = 8,
      [string]$DriverMemory = "1g",
      [int]$Warmups = 1,
      [int]$Repeats = 3,
      [string]$Reference)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
if ($Warmups -lt 1 -or $Repeats -lt 3) { throw "Cần ít nhất 1 warmup và 3 lần đo" }
if ($Mode -eq "df-curated") { if (-not $CuratedRunId) { throw "df-curated cần -CuratedRunId" } }
elseif (-not $InputPath) { throw "$Mode cần -InputPath" }
$evidence = Join-Path $root "docs\evidence\bench\spark\$Label"
$logs = Join-Path $root "results\spark-bench\$Label"
if (Test-Path $evidence) { throw "Đã có ${evidence}: chọn Label mới" }
New-Item -ItemType Directory -Force $evidence, $logs | Out-Null

$env:SPARK_MASTER = $Master
$env:SPARK_SHUFFLE_PARTITIONS = "$Partitions"
$env:SPARK_DRIVER_MEMORY = $DriverMemory
$jobArgs = @("--mode", $Mode)
if ($Mode -eq "df-curated") { $jobArgs += @("--curated-run-id", $CuratedRunId) } else { $jobArgs += @("--input", $InputPath) }

$runs = @()
for ($i = 0; $i -lt $Warmups + $Repeats; $i++) {
  $log = Join-Path $logs "$i.log"
  $sw = [Diagnostics.Stopwatch]::StartNew()
  $ErrorActionPreference = "Continue"
  & (Join-Path $PSScriptRoot "spark-local.ps1") submit revenue @jobArgs --tag "$Label-$i" *> $log
  $exit = $LASTEXITCODE
  $ErrorActionPreference = "Stop"
  $wall = $sw.ElapsedMilliseconds
  $run = [ordered]@{ round = $i; warmup = ($i -lt $Warmups); exitCode = $exit; wallMillis = $wall; success = $false }
  $line = Select-String -Path $log -Pattern " xong: (\S+)" | Select-Object -Last 1
  if ($exit -eq 0 -and $line) {
    $runId = $line.Matches[0].Groups[1].Value
    $record = Get-Content (Join-Path $root "results\spark\$runId\revenue-run.json") -Raw -Encoding UTF8 | ConvertFrom-Json
    $run.runId = $runId
    $run.endToEndMillis = $record.endToEndMillis
    $run.computeMillis = ($record.stages | Where-Object { $_.name -ne "write_parquet" } | Measure-Object elapsedMillis -Sum).Sum
    $run.writeMillis = ($record.stages | Where-Object { $_.name -eq "write_parquet" }).elapsedMillis
    $run.groups = $record.metrics.groups
    $run.validPurchases = $record.metrics.validPurchases
    $run.taskMetrics = $record.taskMetrics
    $run.matchesReference = $null
    if ($Reference) {
      $ErrorActionPreference = "Continue"
      $cmp = & py -3 scripts\compare_revenue.py $Reference (Join-Path $root "results\spark\$runId\revenue.csv") 2>&1
      $run.matchesReference = ($LASTEXITCODE -eq 0)
      $ErrorActionPreference = "Stop"
      $run.compare = "$($cmp | Select-Object -First 1)"
    }
    $run.success = ($run.matchesReference -ne $false)
  }
  $runs += [pscustomobject]$run
  "round $i exit=$exit wall=${wall}ms success=$($run.success) $($run.runId)"
}

$meta = [ordered]@{ label = $Label; mode = $Mode; input = $InputPath; curatedRunId = $CuratedRunId; master = $Master
  shufflePartitions = $Partitions; driverMemory = $DriverMemory; warmups = $Warmups; repeats = $Repeats; reference = $Reference
  gitSha = (git rev-parse --short HEAD); host = $env:COMPUTERNAME; startedRuns = $runs.Count }
[ordered]@{ meta = $meta; runs = $runs } | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 (Join-Path $evidence "runs.json")

$measured = @($runs | Where-Object { -not $_.warmup -and $_.success })
if ($measured.Count -lt $Repeats) { throw "Chỉ có $($measured.Count) lần đo hợp lệ, cần $Repeats; xem $evidence\runs.json" }
function Median([double[]]$values) {
  $s = $values | Sort-Object; $n = $s.Count
  if ($n % 2) { $s[[int][math]::Floor($n / 2)] } else { ($s[$n / 2 - 1] + $s[$n / 2]) / 2 }
}
$summary = [ordered]@{ label = $Label; mode = $Mode; master = $Master; shufflePartitions = $Partitions; runs = $measured.Count }
foreach ($field in "wallMillis", "endToEndMillis", "computeMillis") {
  $values = [double[]]($measured | ForEach-Object { $_.$field })
  $summary["${field}Median"] = Median $values
  $summary["${field}Min"] = ($values | Measure-Object -Minimum).Minimum
  $summary["${field}Max"] = ($values | Measure-Object -Maximum).Maximum
}
foreach ($metric in "inputBytesRead", "inputRecordsRead", "shuffleReadBytes", "shuffleWriteBytes", "tasks") {
  $summary["${metric}Median"] = Median ([double[]]($measured | ForEach-Object { $_.taskMetrics.$metric }))
}
$summary.groups = $measured[0].groups
$summary.validPurchases = $measured[0].validPurchases
$summary.allMatchReference = if ($Reference) { -not ($runs | Where-Object { $_.matchesReference -eq $false }) } else { $null }
[pscustomobject]$summary | ConvertTo-Csv -NoTypeInformation | Set-Content -Encoding UTF8 (Join-Path $evidence "summary.csv")
[pscustomobject]$summary | Format-List
