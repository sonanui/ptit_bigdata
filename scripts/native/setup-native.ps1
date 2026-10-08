# Chuẩn bị HDFS native trên Windows (chạy một lần, chạy lại an toàn):
#   .\scripts\native\setup-native.ps1
# Tạo <root>\conf (sinh từ config\hadoop-native\), <root>\data, <root>\logs, <root>\lib (jar MapReduce), <root>\runs.
# Không sửa thư mục cài Hadoop (HADOOP_HOME) và không động tới HDFS trong Docker.
. "$PSScriptRoot\hadoop-env.ps1"
$ErrorActionPreference = "Stop"
$root = $env:HADOOP_NATIVE_ROOT
foreach ($d in "conf", "data", "logs", "pid", "lib", "runs", "tmp") { New-Item -ItemType Directory -Force (Join-Path $root $d) | Out-Null }

$rootSlash = $root.Replace("\", "/")
foreach ($f in Get-ChildItem (Join-Path $RepoRoot "config\hadoop-native") -File) {
  $text = [IO.File]::ReadAllText($f.FullName).Replace("@NATIVE_ROOT_WIN@", $root).Replace("@NATIVE_ROOT@", $rootSlash)
  # tệp .cmd: cmd.exe cần xuống dòng CRLF
  if ($f.Extension -eq ".cmd") { $text = ($text -replace "`r?`n", "`r`n") }
  [IO.File]::WriteAllText((Join-Path $env:HADOOP_CONF_DIR $f.Name), $text)
}
# log4j.properties lấy từ bản cài Hadoop (HDFS ghi log ra <root>\logs qua HADOOP_LOG_DIR).
Copy-Item (Join-Path $env:HADOOP_HOME "etc\hadoop\log4j.properties") $env:HADOOP_CONF_DIR -Force

# Jar chứa các lớp MapReduce: bản build trên máy bằng JDK 21 (scripts\spark-local.ps1 build, profile spark, gồm cả lớp
# MapReduce lẫn Spark). Chép sang đường dẫn không dấu cách thành <root>\lib\revenue-aggregation.jar.
$jar = Join-Path $RepoRoot "bigdata\target\revenue-aggregation-spark.jar"
if (Test-Path $jar) { Copy-Item $jar (Join-Path $root "lib\revenue-aggregation.jar") -Force }
else { Write-Warning "Chưa có ${jar}: chạy .\scripts\spark-local.ps1 build trước" }

foreach ($tool in "winutils.exe", "hadoop.dll") {
  if (-not (Test-Path (Join-Path $env:HADOOP_HOME "bin\$tool"))) { throw "Thiếu $env:HADOOP_HOME\bin\$tool" }
}
Write-Host "HADOOP_HOME     = $env:HADOOP_HOME"
Write-Host "HADOOP_CONF_DIR = $env:HADOOP_CONF_DIR"
Write-Host "JAVA_HOME       = $env:JAVA_HOME"
Get-ChildItem $env:HADOOP_CONF_DIR | Select-Object Name, Length | Format-Table -AutoSize
