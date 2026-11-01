param(
  [ValidateSet('Chrome', 'Edge', 'Both')]
  [string]$Browser = 'Both'
)

$ErrorActionPreference = 'Stop'
$hostName = 'com.fluxnet.browser'
$manifestPath = Join-Path (Join-Path $env:LOCALAPPDATA 'FluxNet\browser-native-host') "$hostName.json"
$registryPaths = @()
if ($Browser -in @('Chrome', 'Both')) {
  $registryPaths += 'HKCU:\Software\Google\Chrome\NativeMessagingHosts'
}
if ($Browser -in @('Edge', 'Both')) {
  $registryPaths += 'HKCU:\Software\Microsoft\Edge\NativeMessagingHosts'
}

foreach ($registryPath in $registryPaths) {
  $key = Join-Path $registryPath $hostName
  if (-not (Test-Path -LiteralPath $key)) {
    continue
  }
  $registeredManifest = (Get-Item -LiteralPath $key).GetValue('')
  if ([string]::Equals([string]$registeredManifest, $manifestPath, [StringComparison]::OrdinalIgnoreCase)) {
    Remove-Item -LiteralPath $key -Force
  }
}

if (Test-Path -LiteralPath $manifestPath -PathType Leaf) {
  Remove-Item -LiteralPath $manifestPath -Force
}
$manifestDirectory = Split-Path -Parent $manifestPath
if ((Test-Path -LiteralPath $manifestDirectory -PathType Container) -and
    -not (Get-ChildItem -LiteralPath $manifestDirectory -Force | Select-Object -First 1)) {
  Remove-Item -LiteralPath $manifestDirectory -Force
}
Write-Host "Removed FluxNet Native Messaging host registrations for $Browser."
