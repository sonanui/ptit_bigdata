# Chuỗi Spark Java trên một file raw trong HDFS: A1 (RDD) -> ETL curated -> A2..A5 -> A7 features -> A8 labels.
#   .\scripts\spark-pipeline.ps1 -InputPath /data/ecommerce/raw/sample/2019-Oct-r0.01-s21.csv -Tag d1 [-Duplicates]
# Log từng job: docs\evidence\spark-java\<tag>-<job>.log; run_id: docs\evidence\spark-java\<tag>-run-ids.tsv
param([Parameter(Mandatory = $true)][string]$InputPath,
      [Parameter(Mandatory = $true)][string]$Tag,
      [switch]$Duplicates,
      [int]$MinViews = 20,
      # A8: ảnh chụp train/test theo thời gian (đặc trưng 14 ngày trước t0, nhãn 7 ngày sau t0).
      [string]$TrainT0 = "2019-10-15",
      [string]$TestT0 = "2019-10-25")
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$logDir = Join-Path $root "docs\evidence\spark-java"
New-Item -ItemType Directory -Force $logDir | Out-Null

function Invoke-Job([string]$job, [string[]]$jobArgs) {
  $log = Join-Path $logDir "$Tag-$job.log"
  & (Join-Path $PSScriptRoot "spark-local.ps1") submit $job @jobArgs *> $log
  if ($LASTEXITCODE -ne 0) { Get-Content $log -Tail 30; throw "Job $job lỗi, xem $log" }
  $line = Select-String -Path $log -Pattern " xong: (\S+)" | Select-Object -Last 1
  $line.Matches[0].Groups[1].Value
}

$a1 = Invoke-Job "revenue" @("--input", $InputPath, "--tag", $Tag); "A1: $a1"
$etl = Invoke-Job "etl" @("--input", $InputPath, "--tag", $Tag, "--duplicates", $Duplicates.IsPresent.ToString().ToLower()); "ETL: $etl"
$metrics = Invoke-Job "metrics" @("--curated-run-id", $etl, "--revenue-run-id", $a1); "A2-A5: $metrics"
$features = Invoke-Job "features" @("--curated-run-id", $etl, "--min-views", "$MinViews"); "A7: $features"
$labels = Invoke-Job "labels" @("--curated-run-id", $etl, "--train-t0", $TrainT0, "--test-t0", $TestT0, "--min-views", "$MinViews"); "A8: $labels"
"revenue`t$a1`netl`t$etl`nmetrics`t$metrics`nfeatures`t$features`nlabels`t$labels" | Set-Content -Encoding UTF8 (Join-Path $logDir "$Tag-run-ids.tsv")
