param(
  [Parameter(Mandatory = $true)]
  [ValidateScript({ Test-Path -LiteralPath $_ -PathType Leaf })]
  [string]$HostExePath,
  [ValidateSet('Chrome', 'Edge', 'Both')]
  [string]$Browser = 'Both'
)

$ErrorActionPreference = 'Stop'
$extensionId = 'clgiddogjgpjllmjgpffopnmighkmhae'
$hostName = 'com.fluxnet.browser'
$hostPath = (Resolve-Path -LiteralPath $HostExePath).Path
$manifestDirectory = Join-Path $env:LOCALAPPDATA 'FluxNet\browser-native-host'
New-Item -ItemType Directory -Force -Path $manifestDirectory | Out-Null
$manifestPath = Join-Path $manifestDirectory "$hostName.json"
$manifest = @{
  name = $hostName
  description = 'FluxNet browser media handoff'
  path = $hostPath
  type = 'stdio'
  allowed_origins = @("chrome-extension://$extensionId/")
} | ConvertTo-Json -Depth 4
[System.IO.File]::WriteAllText(
  $manifestPath,
  $manifest,
  [System.Text.UTF8Encoding]::new($false)
)

$registryPaths = @()
if ($Browser -in @('Chrome', 'Both')) {
  $registryPaths += 'HKCU:\Software\Google\Chrome\NativeMessagingHosts'
}
if ($Browser -in @('Edge', 'Both')) {
  $registryPaths += 'HKCU:\Software\Microsoft\Edge\NativeMessagingHosts'
}
foreach ($registryPath in $registryPaths) {
  $key = Join-Path $registryPath $hostName
  New-Item -Path $key -Force | Out-Null
  Set-Item -Path $key -Value $manifestPath
}
Write-Host "Registered FluxNet Native Messaging host for $Browser."
Write-Host "Manifest: $manifestPath"
Write-Host "Extension ID allowlisted: $extensionId"
