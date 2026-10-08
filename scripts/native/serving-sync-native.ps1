# Tải một serving run từ HDFS native về thư mục serving riêng của bản cài local, kiểm sha256 theo manifest.json:
#   .\scripts\native\serving-sync-native.ps1 -RunId <run> [-Target E:\Library\hadoop-native\serving]
# Không đụng thư mục serving\ đã commit trong repo (run demo cả tháng). Run đã có thì không ghi đè.
param([Parameter(Mandatory = $true)][ValidatePattern('^[0-9A-Za-z._-]+$')][string]$RunId, [string]$Target)
. "$PSScriptRoot\hadoop-env.ps1"
if (-not $Target) { $Target = Join-Path $env:HADOOP_NATIVE_ROOT "serving" }
$dest = Join-Path $Target $RunId
if (Test-Path $dest) { throw "Đã có ${dest}; serving run là bất biến" }
New-Item -ItemType Directory -Force $Target | Out-Null
$ErrorActionPreference = "Continue"
& hdfs dfs -get "/data/ecommerce/serving/$RunId" $dest 2>$null
$manifest = Get-Content (Join-Path $dest "manifest.json") -Raw -Encoding UTF8 | ConvertFrom-Json
foreach ($f in $manifest.files) {
  $path = Join-Path $dest $f.path
  if (-not (Test-Path $path)) { throw "Thiếu $($f.path)" }
  if ((Get-FileHash -Algorithm SHA256 $path).Hash.ToLower() -ne $f.sha256) { throw "Sai sha256: $($f.path)" }
}
# hdfs -get kèm tệp .crc của Hadoop; backend chỉ đọc tệp có trong manifest.
Get-ChildItem $dest -Recurse -Force -Filter "*.crc" | Remove-Item -Force
Set-Content -NoNewline -Encoding ASCII (Join-Path $Target "_LATEST") $RunId
"Đã tải $($manifest.files.Count) file, sha256 khớp: $dest (_LATEST = $RunId)"
