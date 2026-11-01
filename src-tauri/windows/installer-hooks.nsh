!macro NSIS_HOOK_POSTINSTALL
  nsExec::ExecToLog '"$SYSDIR\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File "$INSTDIR\install-native-host.ps1" -HostExePath "$INSTDIR\fluxnet_native_host.exe"'
  Pop $0
  ${If} $0 != 0
    MessageBox MB_ICONEXCLAMATION|MB_OK "FluxNet installed, but Chrome/Edge Native Messaging registration failed. Browser integration may need repair."
  ${EndIf}
!macroend

!macro NSIS_HOOK_PREUNINSTALL
  nsExec::ExecToLog '"$SYSDIR\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File "$INSTDIR\uninstall-native-host.ps1"'
  Pop $0
!macroend
