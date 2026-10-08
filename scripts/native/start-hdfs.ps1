# Khởi động HDFS native: NameNode và DataNode, mỗi tiến trình một cửa sổ console riêng (đóng cửa sổ = dừng tiến trình).
#   .\scripts\native\start-hdfs.ps1            # khởi động
#   .\scripts\native\start-hdfs.ps1 -Format    # lần đầu: format NameNode (chỉ khi thư mục namenode còn trống)
# HDFS trong Docker dùng cùng cổng 8020/9870/9864/9866: tắt trước bằng  docker compose stop namenode datanode
param([switch]$Format)
. "$PSScriptRoot\hadoop-env.ps1"
$ErrorActionPreference = "Stop"
$root = $env:HADOOP_NATIVE_ROOT
$nameDir = Join-Path $root "data\namenode"
if ($Format) {
  if ((Test-Path $nameDir) -and (Get-ChildItem $nameDir -ErrorAction SilentlyContinue)) {
    throw "$nameDir đã có dữ liệu: không format lại (sẽ mất metadata HDFS)"
  }
  & hdfs namenode -format -nonInteractive
  if ($LASTEXITCODE -ne 0) { throw "format thất bại" }
}
foreach ($port in 8020, 9870) {
  if (Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue) { throw "Cổng $port đang bận (HDFS Docker còn chạy?)" }
}
# Mỗi daemon chạy từ một script nhỏ trong <root>\bin (đường dẫn không dấu cách), đặt biến môi trường tường minh:
# cửa sổ mới không kế thừa môi trường của phiên hiện tại.
New-Item -ItemType Directory -Force (Join-Path $root "bin") | Out-Null
foreach ($daemon in "namenode", "datanode") {
  $launcher = Join-Path $root "bin\$daemon.ps1"
  @(
    "mode con: cols=150 lines=40",
    "`$env:JAVA_HOME = '$env:JAVA_HOME'",
    "`$env:HADOOP_HOME = '$env:HADOOP_HOME'",
    "`$env:HADOOP_CONF_DIR = '$env:HADOOP_CONF_DIR'",
    "`$env:PATH = '$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;' + `$env:PATH",
    "Set-Location '$root'",
    "hdfs $daemon"
  ) | Set-Content -Path $launcher -Encoding UTF8
  # Cửa sổ mở thu nhỏ và không chiếm bàn phím (SW_SHOWMINNOACTIVE); mở lên từ taskbar để xem log.
  $startup = New-CimInstance -ClassName Win32_ProcessStartup -ClientOnly -Property @{ ShowWindow = [uint16]7 }
  Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{
    CommandLine = "conhost.exe powershell -NoExit -ExecutionPolicy Bypass -File $launcher"; ProcessStartupInformation = $startup } | Out-Null
  Start-Sleep -Seconds 8
}
& hdfs dfsadmin -safemode wait
& hdfs dfsadmin -report | Select-Object -First 24
