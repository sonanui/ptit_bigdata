# Dừng HDFS native (DataNode trước, NameNode sau) và đóng cửa sổ console của chúng:
#   .\scripts\native\stop-hdfs.ps1
foreach ($cls in "datanode.DataNode", "namenode.NameNode") {
  Get-CimInstance Win32_Process -Filter "Name='java.exe'" | Where-Object { $_.CommandLine -like "*org.apache.hadoop.hdfs.server.$cls*" } | ForEach-Object {
    Write-Host "Dừng $cls (pid $($_.ProcessId))"
    Stop-Process -Id $_.ProcessId -ErrorAction SilentlyContinue
  }
  Start-Sleep -Seconds 2
}
# Cửa sổ console chạy bin\namenode.ps1 / bin\datanode.ps1
Get-CimInstance Win32_Process -Filter "Name='powershell.exe'" | Where-Object { $_.CommandLine -match "hadoop-native\\bin\\(namenode|datanode)\.ps1" } |
  ForEach-Object { Stop-Process -Id $_.ProcessId -ErrorAction SilentlyContinue }
