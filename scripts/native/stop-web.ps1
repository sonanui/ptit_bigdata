# Dừng web chạy local (backend Spring Boot và Vite dev server) và đóng cửa sổ console của chúng:
#   .\scripts\native\stop-web.ps1
Get-CimInstance Win32_Process -Filter "Name='java.exe'" | Where-Object { $_.CommandLine -like "*revenue-webapp.jar*" } |
  ForEach-Object { Write-Host "Dừng backend (pid $($_.ProcessId))"; Stop-Process -Id $_.ProcessId -ErrorAction SilentlyContinue }
Get-CimInstance Win32_Process -Filter "Name='node.exe'" | Where-Object { $_.CommandLine -like "*vite*" } |
  ForEach-Object { Write-Host "Dừng frontend (pid $($_.ProcessId))"; Stop-Process -Id $_.ProcessId -ErrorAction SilentlyContinue }
Get-CimInstance Win32_Process -Filter "Name='powershell.exe'" | Where-Object { $_.CommandLine -match "hadoop-native\\bin\\web-(backend|frontend)\.ps1" } |
  ForEach-Object { Stop-Process -Id $_.ProcessId -ErrorAction SilentlyContinue }
