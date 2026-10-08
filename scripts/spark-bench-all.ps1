# Ma trận thực nghiệm Spark giai đoạn 3 (E4, E5) — gọi scripts\spark-bench.ps1 cho từng ô, bỏ qua ô đã có evidence.
#   .\scripts\spark-bench-all.ps1 [-Only e4-d1]      # -Only: chỉ chạy các nhãn bắt đầu bằng chuỗi này
# Không chạy song song với việc nặng khác (MR, notebook, web) để số đo không bị nhiễu. Sau đó: py -3 scripts\bench_summary.py
param([string]$Only = "")
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$raw = @{ d1 = "/data/ecommerce/raw/sample/2019-Oct-r0.01-s21.csv"; d2 = "/data/ecommerce/raw/sample/2019-Oct-r0.1-s21.csv"; d3 = "/data/ecommerce/raw/2019-Oct.csv" }
# Kết quả tham chiếu: MR V1 (D1, D2) và baseline Python độc lập (D3, đã khớp MR V1 D3 qua revenue_parity.json).
$ref = @{ d1 = "results\mr\20261006-032500-88fd989-d1\revenue.csv"; d2 = "results\mr\20261006-034832-88fd989-d2\revenue.csv"; d3 = "results\baseline\2019-Oct-revenue.csv" }
# Curated Parquet (ETL) của từng bộ dữ liệu: docs\evidence\spark-java\<tag>-run-ids.tsv
$curated = @{}
foreach ($tag in "d1", "d2", "d3") {
  $line = Get-Content "docs\evidence\spark-java\$tag-run-ids.tsv" -Encoding UTF8 | Where-Object { $_ -match "^etl`t" }
  $curated[$tag] = ($line -split "`t")[1].Trim()
}
$cells = @()
foreach ($tag in "d1", "d2", "d3") {
  foreach ($mode in "rdd-raw", "df-raw", "df-curated") { $cells += @{ Label = "e4-$tag-$mode"; Tag = $tag; Mode = $mode } }
}
$cells += @{ Label = "e4-d2-rdd-raw-c1"; Tag = "d2"; Mode = "rdd-raw"; Master = "local[1]" }
$cells += @{ Label = "e4-d2-rdd-raw-c4"; Tag = "d2"; Mode = "rdd-raw"; Master = "local[4]" }
$cells += @{ Label = "e4-d2-df-raw-p64"; Tag = "d2"; Mode = "df-raw"; Partitions = 64 }
$cells += @{ Label = "e4-d2-df-raw-p200"; Tag = "d2"; Mode = "df-raw"; Partitions = 200 }
$cells += @{ Label = "e5-d3-rdd-raw-c1"; Tag = "d3"; Mode = "rdd-raw"; Master = "local[1]" }

foreach ($c in $cells) {
  if ($Only -and -not $c.Label.StartsWith($Only)) { continue }
  if (Test-Path "docs\evidence\bench\spark\$($c.Label)\summary.csv") { "skip $($c.Label) (đã có)"; continue }
  if (Test-Path "docs\evidence\bench\spark\$($c.Label)") { throw "docs\evidence\bench\spark\$($c.Label) dở dang: kiểm tra runs.json rồi xóa thủ công" }
  $a = @{ Label = $c.Label; Mode = $c.Mode; Reference = $ref[$c.Tag] }
  if ($c.Mode -eq "df-curated") { $a.CuratedRunId = $curated[$c.Tag] } else { $a.InputPath = $raw[$c.Tag] }
  if ($c.Master) { $a.Master = $c.Master }
  if ($c.Partitions) { $a.Partitions = $c.Partitions }
  "=== $($c.Label) $(Get-Date -Format HH:mm:ss)"
  & (Join-Path $PSScriptRoot "spark-bench.ps1") @a | Out-Host
}
"=== xong $(Get-Date -Format HH:mm:ss)"
