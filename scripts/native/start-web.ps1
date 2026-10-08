# Chạy web trên máy (không Docker): backend Spring Boot (cổng 8080) và frontend Vite dev server (cổng 5173),
# mỗi tiến trình một cửa sổ console. Build trước:
#   .tools\apache-maven-3.9.11\bin\mvn.cmd -B -f webapp\backend\pom.xml clean package   (JDK 21)
#   cd webapp\frontend; npm ci; npm test; npm run build
# Chạy:  .\scripts\native\start-web.ps1 [-ServingDir E:\Library\hadoop-native\serving]   -> mở http://localhost:5173
param([string]$ServingDir)
. "$PSScriptRoot\hadoop-env.ps1"
if (-not $ServingDir) { $ServingDir = Join-Path $env:HADOOP_NATIVE_ROOT "serving" }
$jar = Join-Path $RepoRoot "webapp\backend\target\revenue-webapp.jar"
if (-not (Test-Path $jar)) { throw "Chưa có ${jar}: build backend trước" }
foreach ($port in 8080, 5173) {
  if (Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue) { throw "Cổng $port đang bận (web trong Docker còn chạy? docker compose --profile web stop)" }
}
$bin = Join-Path $env:HADOOP_NATIVE_ROOT "bin"
New-Item -ItemType Directory -Force $bin | Out-Null
$frontend = Join-Path $RepoRoot "webapp\frontend"
$launchers = @{
  # chcp 65001 + stdout.encoding: log tiếng Việt của backend hiện đúng dấu trong console
  "web-backend" = @("mode con: cols=150 lines=40", "chcp 65001 | Out-Null", "`$env:JAVA_HOME = '$env:JAVA_HOME'",
                    "& '$env:JAVA_HOME\bin\java.exe' '-Dstdout.encoding=UTF-8' -jar '$jar' '--serving.dir=$ServingDir'")
  "web-frontend" = @("mode con: cols=150 lines=40", "Set-Location '$frontend'", "npm run dev")
}
foreach ($name in "web-backend", "web-frontend") {
  $launcher = Join-Path $bin "$name.ps1"
  $launchers[$name] | Set-Content -Path $launcher -Encoding UTF8
  # Cửa sổ mở thu nhỏ và không chiếm bàn phím (SW_SHOWMINNOACTIVE).
  $startup = New-CimInstance -ClassName Win32_ProcessStartup -ClientOnly -Property @{ ShowWindow = [uint16]7 }
  Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{
    CommandLine = "conhost.exe powershell -NoExit -ExecutionPolicy Bypass -File $launcher"; ProcessStartupInformation = $startup } | Out-Null
}
# Chờ backend sẵn sàng.
for ($i = 0; $i -lt 60; $i++) {
  try { $h = Invoke-RestMethod http://localhost:8080/api/health -TimeoutSec 2; break } catch { Start-Sleep -Seconds 2 }
}
$h | ConvertTo-Json -Compress
"Backend: http://localhost:8080/api/health   Frontend: http://localhost:5173"
