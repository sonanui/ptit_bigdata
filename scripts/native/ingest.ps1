# Nạp CSV gốc từ data\raw vào vùng raw trên HDFS native (không bao giờ ghi đè):
#   .\scripts\native\ingest.ps1 2019-Oct.csv [2019-Nov.csv ...]
# Đã có cùng kích thước: bỏ qua (SKIP). Khác kích thước: dừng với lỗi. Sau khi nạp: fsck kiểm tra block.
param([Parameter(Mandatory = $true, ValueFromRemainingArguments = $true)][string[]]$Names)
. "$PSScriptRoot\hadoop-env.ps1"
$target = "/data/ecommerce/raw"
& hdfs dfs -mkdir -p $target
foreach ($name in $Names) {
  $local = Join-Path $RepoRoot "data\raw\$name"
  if (-not (Test-Path $local)) { throw "Không có $local" }
  $size = (Get-Item $local).Length
  $remote = "$target/$name"
  # Kích thước tệp trên HDFS đọc từ cột thứ 5 của "hdfs dfs -ls" (hdfs.cmd không trả mã thoát tin cậy, và cmd nuốt ký tự %).
  function RemoteSize { $line = & hdfs dfs -ls $remote 2>$null | Select-String ([regex]::Escape($remote) + '$'); if ($line) { [int64](($line.Line -split '\s+')[4]) } }
  $existing = RemoteSize
  if ($null -ne $existing) {
    if ($existing -eq $size) { Write-Host "SKIP $remote đã tồn tại ($size bytes)"; continue }
    throw "$remote đã tồn tại nhưng khác kích thước ($existing != $size): không ghi đè"
  }
  # đường dẫn tuyệt đối: hadoop.cmd chạy java ở thư mục làm việc khác nên đường dẫn tương đối không dùng được
  $t = Measure-Command { & hdfs dfs -put $local $remote }
  if ((RemoteSize) -ne $size) { throw "put thất bại: $name" }
  Write-Host ("PUT {0} -> {1}: {2} bytes, {3:N0} s" -f $local, $remote, $size, $t.TotalSeconds)
}
& hdfs dfs -ls $target
