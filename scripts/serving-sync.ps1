# Đồng bộ một serving run từ HDFS về .\serving\<RunId>\ (volume chỉ đọc của backend), kiểm sha256 theo manifest.
#   .\scripts\serving-sync.ps1 -RunId 20261006-...-d3 [-SetLatest]
# HDFS phải đang chạy (docker compose up -d namenode datanode). Không ghi đè run đã có.
param([Parameter(Mandatory = $true)][ValidatePattern('^[0-9A-Za-z._-]+$')][string]$RunId,
      [switch]$SetLatest)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$target = Join-Path $root "serving\$RunId"
if (Test-Path $target) { throw "Đã có ${target}; serving run là bất biến" }
$staging = "results/serving-sync/$RunId"
if (Test-Path $staging) { Remove-Item -Recurse -Force $staging }
New-Item -ItemType Directory -Force (Split-Path $staging) | Out-Null
$env:MSYS_NO_PATHCONV = "1"
$ErrorActionPreference = "Continue"
docker compose run --rm bigdata scripts/hdfs-fs.sh -get "hdfs://namenode:8020/data/ecommerce/serving/$RunId" $staging
$exit = $LASTEXITCODE
$ErrorActionPreference = "Stop"
if ($exit -ne 0) { throw "hdfs -get lỗi ($exit)" }
$manifest = Get-Content (Join-Path $staging "manifest.json") -Raw -Encoding UTF8 | ConvertFrom-Json
foreach ($f in $manifest.files) {
  $path = Join-Path $staging $f.path
  if (-not (Test-Path $path)) { throw "Thiếu $($f.path)" }
  $sha = (Get-FileHash -Algorithm SHA256 $path).Hash.ToLower()
  if ($sha -ne $f.sha256) { throw "Sai sha256: $($f.path)" }
}
New-Item -ItemType Directory -Force (Split-Path $target) | Out-Null
Move-Item $staging $target
"Đã đồng bộ $($manifest.files.Count) file, sha256 khớp: $target"
if ($SetLatest) { Set-Content -NoNewline -Encoding ASCII (Join-Path $root "serving\_LATEST") $RunId; "_LATEST = $RunId" }
