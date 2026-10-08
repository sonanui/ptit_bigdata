# Môi trường cho Hadoop native trên Windows. Dot-source trong PowerShell:  . .\scripts\native\hadoop-env.ps1
# - JAVA_HOME, HADOOP_HOME lấy từ config\spark-local.env (cùng JDK 21 và bản Hadoop 3.4.2 có winutils.exe).
# - HADOOP_CONF_DIR trỏ tới bản cấu hình đã sinh bởi setup-native.ps1. Thư mục native không được có dấu cách
#   (các script .cmd của Hadoop không xử lý được), mặc định E:\Library\hadoop-native, đổi bằng $env:HADOOP_NATIVE_ROOT.
$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$envFile = Join-Path $RepoRoot "config\spark-local.env"
if (-not (Test-Path $envFile)) { throw "Thiếu $envFile (sao chép từ config\spark-local.env.example)" }
foreach ($line in Get-Content $envFile -Encoding UTF8) {
  $line = $line.Trim()
  if ($line -and -not $line.StartsWith("#") -and $line.Contains("=")) {
    $key, $value = $line.Split("=", 2)
    if ($key.Trim() -in @("JAVA_HOME", "HADOOP_HOME")) { Set-Item -Path "Env:$($key.Trim())" -Value $value.Trim() }
  }
}
$NativeRoot = if ($env:HADOOP_NATIVE_ROOT) { $env:HADOOP_NATIVE_ROOT } else { "E:\Library\hadoop-native" }
if ($NativeRoot.Contains(" ")) { throw "HADOOP_NATIVE_ROOT không được có dấu cách: $NativeRoot" }
$env:HADOOP_NATIVE_ROOT = $NativeRoot
$env:HADOOP_CONF_DIR = Join-Path $NativeRoot "conf"
# Lệnh hdfs/hadoop chạy bằng user Windows; kiểm tra quyền đã tắt trong hdfs-site.xml.
Remove-Item Env:HADOOP_USER_NAME -ErrorAction SilentlyContinue
$env:PATH = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:HADOOP_HOME\sbin;$env:PATH"
