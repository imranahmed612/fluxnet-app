use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use std::collections::VecDeque;
#[cfg(windows)]
use std::path::Path;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Instant;
#[cfg(windows)]
use tauri::Manager;

const BDIX_MAX_PROBES: usize = 10;
const BDIX_MIN_TIMEOUT_MS: u64 = 500;
const BDIX_MAX_TIMEOUT_MS: u64 = 5000;
const QUALITY_HISTORY_LIMIT: usize = 300;
const QUALITY_ROLLING_WINDOW: usize = 60;
const QUALITY_MAX_TIMEOUT_MS: u64 = 5000;
const QUALITY_INTERVALS: [u64; 3] = [1, 2, 5];
const QUALITY_EXCELLENT_MAX_LATENCY_MS: f64 = 50.0;
const QUALITY_EXCELLENT_MAX_PACKET_LOSS_PERCENT: f64 = 1.0;
const QUALITY_GOOD_MAX_LATENCY_MS: f64 = 100.0;
const QUALITY_GOOD_MAX_PACKET_LOSS_PERCENT: f64 = 2.0;
const QUALITY_FAIR_MAX_LATENCY_MS: f64 = 200.0;
const QUALITY_FAIR_MAX_PACKET_LOSS_PERCENT: f64 = 5.0;

pub struct QualityMonitorControl {
    running: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
    history: Arc<Mutex<VecDeque<QualityProbe>>>,
    settings: Arc<Mutex<QualityMonitorSettings>>,
    alert_settings: Arc<Mutex<QualityAlertSettings>>,
}

impl Default for QualityMonitorControl {
    fn default() -> Self {
        Self {
            running: Arc::default(),
            cancelled: Arc::default(),
            history: Arc::default(),
            settings: Arc::new(Mutex::new(QualityMonitorSettings {
                target: "1.1.1.1".into(),
                interval_seconds: 1,
                ping_timeout_ms: QUALITY_MAX_TIMEOUT_MS,
            })),
            alert_settings: Arc::new(Mutex::new(QualityAlertSettings::default())),
        }
    }
}

#[derive(Clone, Debug)]
pub struct QualityMonitorSettings {
    target: String,
    interval_seconds: u64,
    ping_timeout_ms: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QualityAlertSettings {
    pub enabled: bool,
    pub failure_threshold: u32,
    pub cooldown_seconds: u64,
}

impl Default for QualityAlertSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            failure_threshold: 3,
            cooldown_seconds: 300,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QualityProbe {
    pub target: String,
    pub timestamp_ms: u64,
    pub status: String,
    pub latency_ms: Option<f64>,
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QualityStats {
    pub samples: usize,
    pub packet_loss_percent: Option<f64>,
    pub min_latency_ms: Option<f64>,
    pub average_latency_ms: Option<f64>,
    pub max_latency_ms: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QualityRating {
    pub level: String,
    pub explanation: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QualityMonitorUpdate {
    pub probe: QualityProbe,
    pub rolling_stats: QualityStats,
    pub quality_rating: QualityRating,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QualityMonitorSnapshot {
    pub running: bool,
    pub target: String,
    pub interval_seconds: u64,
    pub history: Vec<QualityProbe>,
    pub rolling_stats: QualityStats,
    pub quality_rating: QualityRating,
    pub alert_settings: QualityAlertSettings,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QualityMonitorStatus {
    pub status: String,
    pub message: Option<String>,
}

struct QualityRunningGuard(Arc<AtomicBool>);

impl Drop for QualityRunningGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn append_quality_probe(history: &mut VecDeque<QualityProbe>, probe: QualityProbe) {
    if history.len() == QUALITY_HISTORY_LIMIT {
        history.pop_front();
    }
    history.push_back(probe);
}

#[derive(Default)]
struct QualityAlertTracker {
    consecutive_failures: u32,
    last_notification: Option<Instant>,
}

fn should_send_quality_alert(
    settings: &QualityAlertSettings,
    tracker: &mut QualityAlertTracker,
    probe_succeeded: bool,
    now: Instant,
) -> bool {
    if probe_succeeded || !settings.enabled {
        tracker.consecutive_failures = 0;
        return false;
    }
    tracker.consecutive_failures = tracker.consecutive_failures.saturating_add(1);
    if tracker.consecutive_failures < settings.failure_threshold {
        return false;
    }
    let cooldown = std::time::Duration::from_secs(settings.cooldown_seconds);
    if tracker
        .last_notification
        .is_some_and(|last| now.duration_since(last) < cooldown)
    {
        return false;
    }
    tracker.last_notification = Some(now);
    true
}

fn quality_stats(history: &VecDeque<QualityProbe>) -> QualityStats {
    let window = history
        .iter()
        .rev()
        .take(QUALITY_ROLLING_WINDOW)
        .collect::<Vec<_>>();
    if window.is_empty() {
        return QualityStats {
            samples: 0,
            packet_loss_percent: None,
            min_latency_ms: None,
            average_latency_ms: None,
            max_latency_ms: None,
        };
    }

    let successful = window
        .iter()
        .filter(|probe| probe.status == "success")
        .filter_map(|probe| probe.latency_ms)
        .collect::<Vec<_>>();
    let packet_loss_percent =
        Some((window.len() - successful.len()) as f64 / window.len() as f64 * 100.0);
    QualityStats {
        samples: window.len(),
        packet_loss_percent,
        min_latency_ms: successful.iter().copied().reduce(f64::min),
        average_latency_ms: (!successful.is_empty())
            .then(|| successful.iter().sum::<f64>() / successful.len() as f64),
        max_latency_ms: successful.iter().copied().reduce(f64::max),
    }
}

fn quality_rating(stats: &QualityStats) -> QualityRating {
    let level = match (
        stats.average_latency_ms,
        stats.packet_loss_percent,
        stats.samples,
    ) {
        (_, _, 0) => "noData",
        (None, _, _) | (_, None, _) => "disconnected",
        (Some(latency), Some(loss), _)
            if latency <= QUALITY_EXCELLENT_MAX_LATENCY_MS
                && loss <= QUALITY_EXCELLENT_MAX_PACKET_LOSS_PERCENT =>
        {
            "excellent"
        }
        (Some(latency), Some(loss), _)
            if latency <= QUALITY_GOOD_MAX_LATENCY_MS
                && loss <= QUALITY_GOOD_MAX_PACKET_LOSS_PERCENT =>
        {
            "good"
        }
        (Some(latency), Some(loss), _)
            if latency <= QUALITY_FAIR_MAX_LATENCY_MS
                && loss <= QUALITY_FAIR_MAX_PACKET_LOSS_PERCENT =>
        {
            "fair"
        }
        _ => "poor",
    };
    let explanation = match level {
        "noData" => "No probe outcomes are available yet.".to_owned(),
        "disconnected" => {
            "All recent probes failed; no positive quality rating is available.".to_owned()
        }
        _ => format!(
            "{}: average successful-probe latency is {:.1} ms and packet loss is {:.1}% ({} outcomes).",
            match level {
                "excellent" => "Excellent",
                "good" => "Good",
                "fair" => "Fair",
                _ => "Poor",
            },
            stats.average_latency_ms.unwrap_or_default(),
            stats.packet_loss_percent.unwrap_or_default(),
            stats.samples
        ),
    };
    QualityRating {
        level: level.into(),
        explanation,
    }
}

fn quality_history_csv(history: &VecDeque<QualityProbe>) -> Result<String, String> {
    let mut csv = String::from(
        "Timestamp (ISO 8601 UTC),Target hostname/IP,Probe result,Latency (ms),Error or failure reason\r\n",
    );
    for probe in history {
        let result = if probe.status == "success" {
            "success"
        } else {
            "failure"
        };
        let latency = probe
            .latency_ms
            .filter(|_| result == "success")
            .map(format_latency)
            .unwrap_or_default();
        let timestamp_ms = i64::try_from(probe.timestamp_ms)
            .map_err(|_| format!("Probe timestamp {} is out of range.", probe.timestamp_ms))?;
        let timestamp = DateTime::<Utc>::from_timestamp_millis(timestamp_ms)
            .ok_or_else(|| format!("Probe timestamp {} is out of range.", probe.timestamp_ms))?
            .to_rfc3339_opts(SecondsFormat::Millis, true);
        let reason = if result == "success" {
            String::new()
        } else {
            probe.detail.clone().unwrap_or_else(|| probe.status.clone())
        };
        csv.push_str(
            &[
                timestamp,
                probe.target.clone(),
                result.to_owned(),
                latency,
                reason,
            ]
            .iter()
            .map(|field| csv_field(field))
            .collect::<Vec<_>>()
            .join(","),
        );
        csv.push_str("\r\n");
    }
    Ok(csv)
}

fn format_latency(latency_ms: f64) -> String {
    let formatted = format!("{latency_ms:.2}");
    formatted
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_owned()
}

fn csv_field(value: &str) -> String {
    if value
        .chars()
        .any(|character| matches!(character, ',' | '"' | '\r' | '\n'))
    {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

#[derive(Default)]
pub struct BdixAnalysisControl {
    running: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BdixAnalysisResult {
    pub hostname: String,
    pub status: String,
    pub resolved_addresses: Vec<String>,
    pub probed_address: Option<String>,
    pub attempts: usize,
    pub successful_attempts: usize,
    pub packet_loss_percent: Option<f64>,
    pub min_latency_ms: Option<f64>,
    pub average_latency_ms: Option<f64>,
    pub max_latency_ms: Option<f64>,
    pub probes: Vec<ConnectivityProbe>,
    pub message: Option<String>,
    pub timestamp_ms: u64,
}

struct AnalysisRunningGuard(Arc<AtomicBool>);

impl Drop for AnalysisRunningGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkInterface {
    pub name: String,
    pub adapter_type: Option<String>,
    pub description: Option<String>,
    pub category: String,
    pub is_virtual: bool,
    pub operational_status: String,
    pub mac_address: Option<String>,
    pub ipv4_address: Option<String>,
    pub ipv4_addresses: Vec<String>,
    pub ipv4_is_link_local: bool,
    pub ipv6_addresses: Vec<String>,
    pub gateway_addresses: Vec<String>,
    pub default_gateway: Option<String>,
    pub dns_servers: Vec<String>,
    pub link_speed_mbps: Option<f64>,
    pub bytes_received: Option<String>,
    pub bytes_sent: Option<String>,
    pub wifi_ssid: Option<String>,
    pub wifi_signal_quality: Option<u32>,
    pub wifi_frequency_mhz: Option<u32>,
    pub wifi_receive_mbps: Option<f64>,
    pub wifi_transmit_mbps: Option<f64>,
    pub wifi_error: Option<String>,
    pub data_warning: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectivityDiagnostic {
    pub hostname: String,
    pub status: String,
    pub resolved_ipv4: Option<String>,
    pub attempts: usize,
    pub successful_attempts: usize,
    pub packet_loss_percent: Option<f64>,
    pub min_latency_ms: Option<f64>,
    pub average_latency_ms: Option<f64>,
    pub max_latency_ms: Option<f64>,
    pub probes: Vec<ConnectivityProbe>,
    pub message: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectivityProbe {
    pub attempt: usize,
    pub status: String,
    pub latency_ms: Option<f64>,
    pub detail: Option<String>,
}

#[cfg(windows)]
mod windows {
    use super::{
        BDIX_MAX_PROBES, BDIX_MAX_TIMEOUT_MS, BDIX_MIN_TIMEOUT_MS, BdixAnalysisResult,
        ConnectivityDiagnostic, ConnectivityProbe, NetworkInterface, QUALITY_INTERVALS,
        QualityAlertSettings, QualityAlertTracker, QualityMonitorSettings, QualityMonitorStatus,
        QualityMonitorUpdate, QualityProbe, append_quality_probe, quality_rating, quality_stats,
        should_send_quality_alert,
    };
    use std::{
        collections::VecDeque,
        ffi::CStr,
        mem::{MaybeUninit, size_of},
        net::{IpAddr, Ipv4Addr, Ipv6Addr, ToSocketAddrs},
        ptr::{self, null, null_mut},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        thread,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };
    use tauri::{AppHandle, Emitter};
    use tauri_plugin_notification::NotificationExt;
    use windows_sys::Win32::{
        Foundation::{
            ERROR_ACCESS_DENIED, ERROR_BUFFER_OVERFLOW, ERROR_NO_DATA, ERROR_SUCCESS, GetLastError,
            HANDLE, INVALID_HANDLE_VALUE,
        },
        NetworkManagement::WiFi::{
            DOT11_SSID, WLAN_API_VERSION, WLAN_BSS_LIST, WLAN_CONNECTION_ATTRIBUTES,
            WLAN_INTERFACE_INFO_LIST, WlanCloseHandle, WlanEnumInterfaces, WlanFreeMemory,
            WlanGetNetworkBssList, WlanOpenHandle, WlanQueryInterface,
            dot11_BSS_type_infrastructure, wlan_interface_state_connected,
            wlan_intf_opcode_current_connection,
        },
        NetworkManagement::{
            IpHelper::{
                ConvertInterfaceLuidToGuid, FreeMibTable, GAA_FLAG_INCLUDE_GATEWAYS,
                GetAdaptersAddresses, GetIfTable2, ICMP_ECHO_REPLY, ICMPV6_ECHO_REPLY_LH,
                IF_TYPE_ETHERNET_CSMACD, IF_TYPE_IEEE80211, IF_TYPE_SOFTWARE_LOOPBACK,
                IF_TYPE_TUNNEL, IP_ADAPTER_ADDRESSES_LH, IP_ADAPTER_DNS_SERVER_ADDRESS_XP,
                IP_ADAPTER_GATEWAY_ADDRESS_LH, IP_ADAPTER_UNICAST_ADDRESS_LH,
                IP_DEST_HOST_UNREACHABLE, IP_DEST_NET_UNREACHABLE, IP_REQ_TIMED_OUT, IP_SUCCESS,
                Icmp6SendEcho2, IcmpCloseHandle, IcmpCreateFile, IcmpSendEcho, MIB_IF_TABLE2,
            },
            Ndis::{
                NET_IF_OPER_STATUS_DORMANT, NET_IF_OPER_STATUS_DOWN,
                NET_IF_OPER_STATUS_LOWER_LAYER_DOWN, NET_IF_OPER_STATUS_NOT_PRESENT,
                NET_IF_OPER_STATUS_TESTING, NET_IF_OPER_STATUS_UNKNOWN, NET_IF_OPER_STATUS_UP,
            },
        },
        Networking::WinSock::{
            AF_INET, AF_INET6, AF_UNSPEC, IN6_ADDR, SOCKADDR_IN, SOCKADDR_IN6, SOCKET_ADDRESS,
        },
    };

    const MAX_BUFFER_ATTEMPTS: usize = 3;
    const CONNECTIVITY_PROBES: usize = 4;
    const PROBE_TIMEOUT: Duration = Duration::from_millis(1200);
    const MAX_HOSTNAME_LENGTH: usize = 253;
    const DNS_TIMEOUT: Duration = Duration::from_secs(8);
    static DNS_LOOKUP_ACTIVE: AtomicBool = AtomicBool::new(false);

    struct DnsWorkerGuard;

    impl Drop for DnsWorkerGuard {
        fn drop(&mut self) {
            DNS_LOOKUP_ACTIVE.store(false, Ordering::Release);
        }
    }

    pub fn get_network_interfaces() -> Result<Vec<NetworkInterface>, String> {
        let mut buffer_size = 0u32;
        let first_result = unsafe {
            GetAdaptersAddresses(
                AF_UNSPEC as u32,
                GAA_FLAG_INCLUDE_GATEWAYS,
                ptr::null(),
                ptr::null_mut(),
                &mut buffer_size,
            )
        };

        if first_result == ERROR_NO_DATA {
            return Ok(Vec::new());
        }
        if first_result != ERROR_BUFFER_OVERFLOW && first_result != ERROR_SUCCESS {
            return Err(api_error(first_result));
        }
        if first_result == ERROR_SUCCESS {
            return Ok(Vec::new());
        }

        for _ in 0..MAX_BUFFER_ATTEMPTS {
            if buffer_size == 0 {
                return Ok(Vec::new());
            }

            let entry_count = (buffer_size as usize).div_ceil(size_of::<IP_ADAPTER_ADDRESSES_LH>());
            let mut buffer: Vec<MaybeUninit<IP_ADAPTER_ADDRESSES_LH>> =
                vec![MaybeUninit::uninit(); entry_count];
            let mut available_size = (buffer.len() * size_of::<IP_ADAPTER_ADDRESSES_LH>()) as u32;
            let result = unsafe {
                GetAdaptersAddresses(
                    AF_UNSPEC as u32,
                    GAA_FLAG_INCLUDE_GATEWAYS,
                    ptr::null(),
                    buffer.as_mut_ptr().cast(),
                    &mut available_size,
                )
            };

            if result == ERROR_BUFFER_OVERFLOW {
                buffer_size = available_size;
                continue;
            }
            if result == ERROR_NO_DATA {
                return Ok(Vec::new());
            }
            if result != ERROR_SUCCESS {
                return Err(api_error(result));
            }

            let (counters, counter_error) = match interface_counters() {
                Ok(counters) => (counters, None),
                Err(error) => (std::collections::HashMap::new(), Some(error)),
            };
            let (wifi_details, wifi_error) = wifi_interface_details();
            return unsafe {
                collect_interfaces(
                    buffer.as_ptr().cast(),
                    &counters,
                    &wifi_details,
                    wifi_error,
                    counter_error,
                )
            };
        }

        Err(
            "Windows network interface data changed repeatedly during detection. Retry the scan."
                .into(),
        )
    }

    fn api_error(code: u32) -> String {
        if code == ERROR_ACCESS_DENIED {
            format!(
                "Windows denied access while reading network interfaces (error {code}). Check Windows networking permissions and retry."
            )
        } else {
            format!(
                "Windows could not read network interfaces (error {code}). Check that Windows networking services are available and retry."
            )
        }
    }

    #[derive(Clone, Default)]
    struct InterfaceCounters {
        bytes_received: u64,
        bytes_sent: u64,
        receive_link_speed: u64,
        transmit_link_speed: u64,
    }

    #[derive(Clone, Default)]
    struct WifiDetails {
        ssid: Option<String>,
        signal_quality: Option<u32>,
        frequency_mhz: Option<u32>,
        receive_mbps: Option<f64>,
        transmit_mbps: Option<f64>,
    }

    struct WlanHandleGuard(HANDLE);

    impl Drop for WlanHandleGuard {
        fn drop(&mut self) {
            unsafe {
                WlanCloseHandle(self.0, ptr::null());
            }
        }
    }

    struct MibTableGuard(*mut MIB_IF_TABLE2);

    impl Drop for MibTableGuard {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe { FreeMibTable(self.0.cast()) };
            }
        }
    }

    struct WlanMemoryGuard(*mut std::ffi::c_void);

    impl Drop for WlanMemoryGuard {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe { WlanFreeMemory(self.0) };
            }
        }
    }

    fn interface_counters() -> Result<std::collections::HashMap<u64, InterfaceCounters>, String> {
        let mut table: *mut MIB_IF_TABLE2 = ptr::null_mut();
        let result = unsafe { GetIfTable2(&mut table) };
        let _table_guard = MibTableGuard(table);
        if result != ERROR_SUCCESS {
            return Err(api_error(result));
        }
        if table.is_null() {
            return Ok(std::collections::HashMap::new());
        }

        let counters = unsafe {
            let table_ref = &*table;
            std::slice::from_raw_parts(table_ref.Table.as_ptr(), table_ref.NumEntries as usize)
                .iter()
                .map(|row| {
                    (
                        row.InterfaceLuid.Value,
                        InterfaceCounters {
                            bytes_received: row.InOctets,
                            bytes_sent: row.OutOctets,
                            receive_link_speed: row.ReceiveLinkSpeed,
                            transmit_link_speed: row.TransmitLinkSpeed,
                        },
                    )
                })
                .collect()
        };
        Ok(counters)
    }

    fn wifi_interface_details() -> (
        std::collections::HashMap<String, WifiDetails>,
        Option<String>,
    ) {
        let mut negotiated_version = 0;
        let mut handle: HANDLE = ptr::null_mut();
        let result = unsafe {
            WlanOpenHandle(
                WLAN_API_VERSION,
                ptr::null(),
                &mut negotiated_version,
                &mut handle,
            )
        };
        if result != ERROR_SUCCESS {
            return (
                std::collections::HashMap::new(),
                Some(format!(
                    "Windows WLAN API is unavailable (error {result}); Wi-Fi details are unavailable."
                )),
            );
        }
        let _handle_guard = WlanHandleGuard(handle);

        let mut interface_list: *mut WLAN_INTERFACE_INFO_LIST = ptr::null_mut();
        let result = unsafe { WlanEnumInterfaces(handle, ptr::null(), &mut interface_list) };
        if result != ERROR_SUCCESS {
            return (
                std::collections::HashMap::new(),
                Some(format!(
                    "Windows could not enumerate Wi-Fi interfaces (error {result})."
                )),
            );
        }
        let _list_guard = WlanMemoryGuard(interface_list.cast());
        if interface_list.is_null() {
            return (std::collections::HashMap::new(), None);
        }

        let list = unsafe { &*interface_list };
        let entries = unsafe {
            std::slice::from_raw_parts(
                ptr::addr_of!(list.InterfaceInfo)
                    .cast::<windows_sys::Win32::NetworkManagement::WiFi::WLAN_INTERFACE_INFO>(),
                list.dwNumberOfItems as usize,
            )
        };
        let mut result = std::collections::HashMap::new();
        let mut query_error = None;
        for interface in entries {
            if interface.isState != wlan_interface_state_connected {
                continue;
            }
            let mut data_size = 0u32;
            let mut data: *mut std::ffi::c_void = ptr::null_mut();
            let query_result = unsafe {
                WlanQueryInterface(
                    handle,
                    &interface.InterfaceGuid,
                    wlan_intf_opcode_current_connection,
                    ptr::null(),
                    &mut data_size,
                    &mut data,
                    ptr::null_mut(),
                )
            };
            if query_result != ERROR_SUCCESS || data.is_null() {
                query_error = Some(format!(
                    "Windows could not read a connected Wi-Fi interface (error {query_result})."
                ));
                continue;
            }
            let _data_guard = WlanMemoryGuard(data);
            if (data_size as usize) < size_of::<WLAN_CONNECTION_ATTRIBUTES>() {
                continue;
            }
            let connection = unsafe { *data.cast::<WLAN_CONNECTION_ATTRIBUTES>() };
            if connection.isState != wlan_interface_state_connected {
                continue;
            }
            let ssid = dot11_ssid_string(connection.wlanAssociationAttributes.dot11Ssid);
            let frequency_mhz = query_wifi_frequency(
                handle,
                &interface.InterfaceGuid,
                connection.wlanAssociationAttributes.dot11Ssid,
                connection.wlanAssociationAttributes.dot11Bssid,
            );
            result.insert(
                guid_key(&interface.InterfaceGuid),
                WifiDetails {
                    ssid,
                    signal_quality: Some(
                        connection
                            .wlanAssociationAttributes
                            .wlanSignalQuality
                            .min(100),
                    ),
                    frequency_mhz,
                    receive_mbps: Some(
                        connection.wlanAssociationAttributes.ulRxRate as f64 / 1000.0,
                    ),
                    transmit_mbps: Some(
                        connection.wlanAssociationAttributes.ulTxRate as f64 / 1000.0,
                    ),
                },
            );
        }
        (result, query_error)
    }

    fn query_wifi_frequency(
        handle: HANDLE,
        interface_guid: &windows_sys::core::GUID,
        ssid: DOT11_SSID,
        connected_bssid: [u8; 6],
    ) -> Option<u32> {
        let mut bss_list: *mut WLAN_BSS_LIST = ptr::null_mut();
        let result = unsafe {
            WlanGetNetworkBssList(
                handle,
                interface_guid,
                &ssid,
                dot11_BSS_type_infrastructure,
                0,
                ptr::null(),
                &mut bss_list,
            )
        };
        if result != ERROR_SUCCESS || bss_list.is_null() {
            return None;
        }
        let _list_guard = WlanMemoryGuard(bss_list.cast());
        unsafe {
            let list = &*bss_list;
            let entries_start = ptr::addr_of!(list.wlanBssEntries)
                .cast::<windows_sys::Win32::NetworkManagement::WiFi::WLAN_BSS_ENTRY>(
            );
            let entries_size = (list.dwNumberOfItems as usize).checked_mul(size_of::<
                windows_sys::Win32::NetworkManagement::WiFi::WLAN_BSS_ENTRY,
            >())?;
            if size_of::<u32>() * 2 + entries_size > list.dwTotalSize as usize {
                return None;
            }
            std::slice::from_raw_parts(entries_start, list.dwNumberOfItems as usize)
                .iter()
                .find(|entry| entry.dot11Bssid == connected_bssid)
                .map(|entry| entry.ulChCenterFrequency / 1000)
                .filter(|frequency| *frequency > 0)
        }
    }

    fn dot11_ssid_string(ssid: DOT11_SSID) -> Option<String> {
        let length = (ssid.uSSIDLength as usize).min(ssid.ucSSID.len());
        let value = String::from_utf8_lossy(&ssid.ucSSID[..length])
            .trim_matches(char::from(0))
            .trim()
            .to_owned();
        (!value.is_empty()).then_some(value)
    }

    fn guid_key(guid: &windows_sys::core::GUID) -> String {
        format!(
            "{:08x}-{:04x}-{:04x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            guid.data1,
            guid.data2,
            guid.data3,
            guid.data4[0],
            guid.data4[1],
            guid.data4[2],
            guid.data4[3],
            guid.data4[4],
            guid.data4[5],
            guid.data4[6],
            guid.data4[7],
        )
    }

    unsafe fn collect_interfaces(
        mut adapter: *const IP_ADAPTER_ADDRESSES_LH,
        counters: &std::collections::HashMap<u64, InterfaceCounters>,
        wifi_details: &std::collections::HashMap<String, WifiDetails>,
        wifi_error: Option<String>,
        counter_error: Option<String>,
    ) -> Result<Vec<NetworkInterface>, String> {
        let mut interfaces = Vec::new();
        while !adapter.is_null() {
            let current = unsafe { &*adapter };
            let name = unsafe { read_wide_string(current.FriendlyName) }
                .or_else(|| unsafe { read_wide_string(current.Description) })
                .or_else(|| unsafe { read_ansi_string(current.AdapterName) })
                .unwrap_or_else(|| "Unknown interface".into());

            let ipv4_addresses = unsafe { collect_addresses(current.FirstUnicastAddress, AF_INET) };
            let ipv6_addresses =
                unsafe { collect_addresses(current.FirstUnicastAddress, AF_INET6) };
            let ipv4_address = ipv4_addresses.first().cloned();
            let ipv4_is_link_local = ipv4_address
                .as_deref()
                .and_then(|address| address.parse::<Ipv4Addr>().ok())
                .is_some_and(is_link_local_ipv4);
            let description = unsafe { read_wide_string(current.Description) };
            let interface_luid = unsafe { current.Luid.Value };
            let counter = counters.get(&interface_luid);
            let raw_kind = adapter_type(current.IfType);
            let is_virtual = is_virtual_adapter(
                current.IfType,
                &name,
                description.as_deref().unwrap_or_default(),
            );
            let category = adapter_category(current.IfType, is_virtual);
            let adapter_type = raw_kind.map(|kind| {
                if is_virtual {
                    format!("{kind} (Virtual)")
                } else {
                    kind
                }
            });
            let gateway_addresses =
                unsafe { collect_socket_addresses(current.FirstGatewayAddress) };
            let dns_servers = unsafe { collect_dns_servers(current.FirstDnsServerAddress) };
            let mut wifi = WifiDetails::default();
            let mut resolved_wifi_error = None;
            if current.IfType == IF_TYPE_IEEE80211 {
                let mut interface_guid = windows_sys::core::GUID::default();
                let guid_result =
                    unsafe { ConvertInterfaceLuidToGuid(&current.Luid, &mut interface_guid) };
                if guid_result == ERROR_SUCCESS {
                    wifi = wifi_details
                        .get(&guid_key(&interface_guid))
                        .cloned()
                        .unwrap_or_default();
                    resolved_wifi_error = wifi_error.clone();
                } else {
                    resolved_wifi_error = Some(format!(
                        "Could not identify the Wi-Fi interface (error {guid_result})."
                    ));
                }
            }

            interfaces.push(NetworkInterface {
                name,
                adapter_type,
                description,
                category,
                is_virtual,
                operational_status: operational_status(current.OperStatus),
                mac_address: physical_address(
                    current.PhysicalAddress,
                    current.PhysicalAddressLength,
                ),
                ipv4_address,
                ipv4_addresses,
                ipv4_is_link_local,
                ipv6_addresses,
                default_gateway: gateway_addresses.first().cloned(),
                gateway_addresses,
                dns_servers,
                link_speed_mbps: counter
                    .map(|item| item.receive_link_speed.max(item.transmit_link_speed))
                    .filter(|speed| *speed > 0)
                    .map(|speed| speed as f64 / 1_000_000.0)
                    .or_else(|| {
                        let speed = current.ReceiveLinkSpeed.max(current.TransmitLinkSpeed);
                        (speed > 0).then_some(speed as f64 / 1_000_000.0)
                    }),
                bytes_received: counter.map(|item| item.bytes_received.to_string()),
                bytes_sent: counter.map(|item| item.bytes_sent.to_string()),
                wifi_ssid: wifi.ssid,
                wifi_signal_quality: wifi.signal_quality,
                wifi_frequency_mhz: wifi.frequency_mhz,
                wifi_receive_mbps: wifi.receive_mbps,
                wifi_transmit_mbps: wifi.transmit_mbps,
                wifi_error: resolved_wifi_error,
                data_warning: counter_error.clone(),
            });
            adapter = current.Next;
        }
        Ok(interfaces)
    }

    unsafe fn read_wide_string(value: *const u16) -> Option<String> {
        if value.is_null() {
            return None;
        }
        let mut length = 0;
        while unsafe { *value.add(length) } != 0 {
            length += 1;
        }
        let slice = unsafe { std::slice::from_raw_parts(value, length) };
        let string = String::from_utf16_lossy(slice);
        (!string.is_empty()).then_some(string)
    }

    unsafe fn read_ansi_string(value: *const u8) -> Option<String> {
        if value.is_null() {
            return None;
        }
        let string = unsafe { CStr::from_ptr(value.cast()) }
            .to_string_lossy()
            .into_owned();
        (!string.is_empty()).then_some(string)
    }

    unsafe fn collect_addresses(
        mut address: *const IP_ADAPTER_UNICAST_ADDRESS_LH,
        family: u16,
    ) -> Vec<String> {
        let mut addresses = Vec::new();
        while !address.is_null() {
            let current = unsafe { &*address };
            if let Some(ip) = unsafe { socket_address_to_ip(current.Address, family) } {
                addresses.push(ip.to_string());
            }
            address = current.Next;
        }
        addresses
    }

    unsafe fn collect_socket_addresses(
        mut gateway: *const IP_ADAPTER_GATEWAY_ADDRESS_LH,
    ) -> Vec<String> {
        let mut addresses = Vec::new();
        while !gateway.is_null() {
            let current = unsafe { &*gateway };
            if let Some(ip) = unsafe { socket_address_to_ip(current.Address, AF_INET) } {
                addresses.push(ip.to_string());
            } else if let Some(ip) = unsafe { socket_address_to_ip(current.Address, AF_INET6) } {
                addresses.push(ip.to_string());
            }
            gateway = current.Next;
        }
        addresses
    }

    unsafe fn collect_dns_servers(
        mut server: *const IP_ADAPTER_DNS_SERVER_ADDRESS_XP,
    ) -> Vec<String> {
        let mut addresses = Vec::new();
        while !server.is_null() {
            let current = unsafe { &*server };
            if let Some(ip) = unsafe { socket_address_to_ip(current.Address, AF_INET) } {
                addresses.push(ip.to_string());
            } else if let Some(ip) = unsafe { socket_address_to_ip(current.Address, AF_INET6) } {
                addresses.push(ip.to_string());
            }
            server = current.Next;
        }
        addresses
    }

    unsafe fn socket_address_to_ip(address: SOCKET_ADDRESS, family: u16) -> Option<IpAddr> {
        if address.lpSockaddr.is_null() {
            return None;
        }
        if family == AF_INET && address.iSockaddrLength >= size_of::<SOCKADDR_IN>() as i32 {
            let sockaddr = unsafe { &*address.lpSockaddr.cast::<SOCKADDR_IN>() };
            if sockaddr.sin_family == AF_INET {
                let address = unsafe { sockaddr.sin_addr.S_un.S_addr };
                return Some(IpAddr::V4(Ipv4Addr::from(u32::from_be(address))));
            }
        }
        if family == AF_INET6 && address.iSockaddrLength >= size_of::<SOCKADDR_IN6>() as i32 {
            let sockaddr = unsafe { &*address.lpSockaddr.cast::<SOCKADDR_IN6>() };
            if sockaddr.sin6_family == AF_INET6 {
                let bytes = unsafe { sockaddr.sin6_addr.u.Byte };
                return Some(IpAddr::V6(Ipv6Addr::from(bytes)));
            }
        }
        None
    }

    #[cfg(test)]
    unsafe fn socket_address_to_ipv4(address: SOCKET_ADDRESS) -> Option<Ipv4Addr> {
        match unsafe { socket_address_to_ip(address, AF_INET) } {
            Some(IpAddr::V4(address)) => Some(address),
            _ => None,
        }
    }

    fn physical_address(bytes: [u8; 8], length: u32) -> Option<String> {
        let length = (length as usize).min(bytes.len());
        (length > 0).then(|| {
            bytes[..length]
                .iter()
                .map(|byte| format!("{byte:02X}"))
                .collect::<Vec<_>>()
                .join(":")
        })
    }

    fn is_virtual_adapter(if_type: u32, name: &str, description: &str) -> bool {
        if if_type == IF_TYPE_TUNNEL || if_type == IF_TYPE_SOFTWARE_LOOPBACK {
            return true;
        }
        let searchable = format!("{name} {description}").to_ascii_lowercase();
        [
            "virtual",
            "vmware",
            "virtualbox",
            "hyper-v",
            "hyperv",
            "vpn",
            "tap-windows",
            "wintun",
            "wireguard",
            "container",
            "veth",
        ]
        .iter()
        .any(|token| searchable.contains(token))
    }

    fn adapter_category(if_type: u32, is_virtual: bool) -> String {
        if is_virtual {
            "Virtual".into()
        } else {
            match if_type {
                IF_TYPE_ETHERNET_CSMACD => "Ethernet",
                IF_TYPE_IEEE80211 => "Wi-Fi",
                IF_TYPE_SOFTWARE_LOOPBACK => "Loopback",
                23 => "PPP",
                IF_TYPE_TUNNEL => "Tunnel",
                _ => "Other",
            }
            .into()
        }
    }

    fn adapter_type(if_type: u32) -> Option<String> {
        let kind = match if_type {
            IF_TYPE_ETHERNET_CSMACD => "Ethernet",
            IF_TYPE_IEEE80211 => "Wi-Fi",
            IF_TYPE_SOFTWARE_LOOPBACK => "Loopback",
            23 => "PPP",
            IF_TYPE_TUNNEL => "Tunnel",
            _ => return Some(format!("Other (type {if_type})")),
        };
        Some(kind.into())
    }

    fn operational_status(status: i32) -> String {
        let label = match status {
            NET_IF_OPER_STATUS_UP => "Up".to_owned(),
            NET_IF_OPER_STATUS_DOWN => "Down".to_owned(),
            NET_IF_OPER_STATUS_TESTING => "Testing".to_owned(),
            NET_IF_OPER_STATUS_UNKNOWN => "Unknown".to_owned(),
            NET_IF_OPER_STATUS_DORMANT => "Dormant".to_owned(),
            NET_IF_OPER_STATUS_NOT_PRESENT => "Not present".to_owned(),
            NET_IF_OPER_STATUS_LOWER_LAYER_DOWN => "Lower layer down".to_owned(),
            _ => format!("Unknown ({status})"),
        };
        label
    }

    fn is_link_local_ipv4(address: Ipv4Addr) -> bool {
        let octets = address.octets();
        octets[0] == 169 && octets[1] == 254
    }

    pub fn test_connectivity(hostname: String) -> Result<ConnectivityDiagnostic, String> {
        let hostname = hostname.trim().to_owned();
        validate_hostname(&hostname)?;

        let resolved = match resolve_ipv4(&hostname) {
            Ok(address) => address,
            Err(ResolutionFailure::Dns(message)) => {
                return Ok(ConnectivityDiagnostic {
                    hostname,
                    status: "dnsResolutionFailed".into(),
                    resolved_ipv4: None,
                    attempts: 0,
                    successful_attempts: 0,
                    packet_loss_percent: None,
                    min_latency_ms: None,
                    average_latency_ms: None,
                    max_latency_ms: None,
                    probes: Vec::new(),
                    message: Some(message),
                });
            }
            Err(ResolutionFailure::DnsTimeout(message)) => {
                return Ok(ConnectivityDiagnostic {
                    hostname,
                    status: "timeout".into(),
                    resolved_ipv4: None,
                    attempts: 0,
                    successful_attempts: 0,
                    packet_loss_percent: None,
                    min_latency_ms: None,
                    average_latency_ms: None,
                    max_latency_ms: None,
                    probes: Vec::new(),
                    message: Some(message),
                });
            }
            Err(ResolutionFailure::NoIpv4) => {
                return Ok(ConnectivityDiagnostic {
                    hostname: hostname.clone(),
                    status: "noIpv4Address".into(),
                    resolved_ipv4: None,
                    attempts: 0,
                    successful_attempts: 0,
                    packet_loss_percent: None,
                    min_latency_ms: None,
                    average_latency_ms: None,
                    max_latency_ms: None,
                    probes: Vec::new(),
                    message: Some(
                        "The hostname resolved, but Windows returned no IPv4 address to test."
                            .into(),
                    ),
                });
            }
            Err(ResolutionFailure::Cancelled) => {
                return Err("Connectivity diagnostic was cancelled.".into());
            }
        };

        let handle = IcmpHandle::create()?;
        let mut probes = Vec::with_capacity(CONNECTIVITY_PROBES);
        for attempt in 1..=CONNECTIVITY_PROBES {
            probes.push(send_probe(
                handle.0,
                resolved,
                attempt,
                PROBE_TIMEOUT.as_millis() as u32,
            )?);
        }

        let latencies = probes
            .iter()
            .filter(|probe| probe.status == "success")
            .filter_map(|probe| probe.latency_ms)
            .collect::<Vec<_>>();
        let successful_attempts = latencies.len();
        let statistics = summarize_latencies(&latencies, CONNECTIVITY_PROBES);

        Ok(ConnectivityDiagnostic {
            hostname,
            status: if successful_attempts > 0 {
                "success"
            } else if probes.iter().all(|probe| probe.status == "timeout") {
                "timeout"
            } else {
                "unreachable"
            }
            .into(),
            resolved_ipv4: Some(resolved.to_string()),
            attempts: CONNECTIVITY_PROBES,
            successful_attempts,
            packet_loss_percent: statistics.packet_loss_percent,
            min_latency_ms: statistics.min_latency_ms,
            average_latency_ms: statistics.average_latency_ms,
            max_latency_ms: statistics.max_latency_ms,
            probes,
            message: None,
        })
    }

    pub fn analyze_target(
        hostname: String,
        probe_count: usize,
        timeout_ms: u64,
        cancelled: Arc<AtomicBool>,
    ) -> Result<BdixAnalysisResult, String> {
        let hostname = hostname.trim().to_owned();
        validate_hostname(&hostname)?;
        validate_analysis_settings(probe_count, timeout_ms)?;
        check_analysis_cancelled(&cancelled)?;

        let resolved = match resolve_addresses(&hostname, &cancelled) {
            Ok(addresses) if !addresses.is_empty() => addresses,
            Ok(_) | Err(ResolutionFailure::NoIpv4) => {
                return Ok(empty_analysis(
                    hostname,
                    "noAddress",
                    Vec::new(),
                    Some("The target did not resolve to an IP address.".into()),
                ));
            }
            Err(ResolutionFailure::Dns(message)) => {
                return Ok(empty_analysis(
                    hostname,
                    "dnsResolutionFailed",
                    Vec::new(),
                    Some(message),
                ));
            }
            Err(ResolutionFailure::DnsTimeout(message)) => {
                return Ok(empty_analysis(
                    hostname,
                    "timeout",
                    Vec::new(),
                    Some(message),
                ));
            }
            Err(ResolutionFailure::Cancelled) => {
                return Err("BDIX analysis stopped by user.".into());
            }
        };
        check_analysis_cancelled(&cancelled)?;

        let addresses = resolved.iter().map(ToString::to_string).collect::<Vec<_>>();
        let destination = resolved[0];
        let handle = IcmpHandle::create()?;
        let mut probes = Vec::with_capacity(probe_count);
        for attempt in 1..=probe_count {
            check_analysis_cancelled(&cancelled)?;
            probes.push(send_analysis_probe(
                handle.0,
                destination,
                attempt,
                timeout_ms as u32,
            )?);
        }
        check_analysis_cancelled(&cancelled)?;

        let latencies = probes
            .iter()
            .filter(|probe| probe.status == "success")
            .filter_map(|probe| probe.latency_ms)
            .collect::<Vec<_>>();
        let successful_attempts = latencies.len();
        let statistics = summarize_latencies(&latencies, probe_count);
        let status = if successful_attempts > 0 {
            if successful_attempts < probe_count {
                "partial"
            } else {
                "success"
            }
        } else if probes.iter().all(|probe| probe.status == "timeout") {
            "timeout"
        } else {
            "unreachable"
        };

        Ok(BdixAnalysisResult {
            hostname,
            status: status.into(),
            resolved_addresses: addresses,
            probed_address: Some(destination.to_string()),
            attempts: probe_count,
            successful_attempts,
            packet_loss_percent: statistics.packet_loss_percent,
            min_latency_ms: statistics.min_latency_ms,
            average_latency_ms: statistics.average_latency_ms,
            max_latency_ms: statistics.max_latency_ms,
            probes,
            message: None,
            timestamp_ms: current_timestamp_ms(),
        })
    }

    pub fn validate_quality_settings(hostname: &str, interval_seconds: u64) -> Result<(), String> {
        validate_hostname(hostname.trim())?;
        if !QUALITY_INTERVALS.contains(&interval_seconds) {
            return Err("Choose a probe interval of 1, 2, or 5 seconds.".into());
        }
        Ok(())
    }

    pub fn run_quality_monitor(
        app: AppHandle,
        hostname: String,
        settings: Arc<Mutex<QualityMonitorSettings>>,
        cancelled: Arc<AtomicBool>,
        history: Arc<Mutex<VecDeque<QualityProbe>>>,
        alert_settings: Arc<Mutex<QualityAlertSettings>>,
    ) {
        let result = run_quality_monitor_loop(
            &app,
            hostname,
            &settings,
            &cancelled,
            &history,
            &alert_settings,
        );
        let status = match result {
            Ok(()) => QualityMonitorStatus {
                status: "stopped".into(),
                message: None,
            },
            Err(message) => QualityMonitorStatus {
                status: if cancelled.load(Ordering::Acquire) {
                    "stopped".into()
                } else {
                    "error".into()
                },
                message: Some(message),
            },
        };
        let _ = app.emit("quality-monitor-status", status);
    }

    fn run_quality_monitor_loop(
        app: &AppHandle,
        hostname: String,
        settings: &Mutex<QualityMonitorSettings>,
        cancelled: &AtomicBool,
        history: &Mutex<VecDeque<QualityProbe>>,
        alert_settings: &Mutex<QualityAlertSettings>,
    ) -> Result<(), String> {
        let resolved = match resolve_addresses(&hostname, cancelled) {
            Ok(addresses) => addresses,
            Err(ResolutionFailure::Cancelled) => return Ok(()),
            Err(ResolutionFailure::Dns(message) | ResolutionFailure::DnsTimeout(message)) => {
                return Err(message);
            }
            Err(ResolutionFailure::NoIpv4) => {
                return Err("The target resolved without any IP addresses.".into());
            }
        };
        let destination = resolved[0];
        let handle = IcmpHandle::create()?;
        let mut alert_tracker = QualityAlertTracker::default();
        while !cancelled.load(Ordering::Acquire) {
            let current_settings = settings
                .lock()
                .map_err(|_| "Network quality monitor settings lock was poisoned.".to_owned())?
                .clone();
            validate_quality_settings(&hostname, current_settings.interval_seconds)?;
            if !(500..=5000).contains(&current_settings.ping_timeout_ms) {
                return Err("Choose a ping timeout from 500 to 5000 milliseconds.".into());
            }
            let mut fatal_error = None;
            let probe = match send_analysis_probe(
                handle.0,
                destination,
                0,
                current_settings.ping_timeout_ms as u32,
            ) {
                Ok(probe) => QualityProbe {
                    target: hostname.clone(),
                    timestamp_ms: current_timestamp_ms(),
                    status: probe.status,
                    latency_ms: probe.latency_ms,
                    detail: probe.detail,
                },
                Err(message) => {
                    if cancelled.load(Ordering::Acquire) {
                        break;
                    }
                    fatal_error = Some(message);
                    QualityProbe {
                        target: hostname.clone(),
                        timestamp_ms: current_timestamp_ms(),
                        status: "error".into(),
                        latency_ms: None,
                        detail: fatal_error.clone(),
                    }
                }
            };
            let probe_succeeded = probe.status == "success";
            publish_quality_probe(app, history, probe)?;
            let settings = alert_settings
                .lock()
                .map_err(|_| "Network quality alert settings lock was poisoned.".to_owned())?
                .clone();
            notify_if_quality_alert_is_due(
                app,
                &hostname,
                &settings,
                &mut alert_tracker,
                probe_succeeded,
            );
            if let Some(message) = fatal_error {
                return Err(message);
            }

            let wait_started = Instant::now();
            let interval = Duration::from_secs(current_settings.interval_seconds);
            while wait_started.elapsed() < interval {
                if cancelled.load(Ordering::Acquire) {
                    return Ok(());
                }
                thread::sleep(Duration::from_millis(100).min(interval - wait_started.elapsed()));
            }
        }
        Ok(())
    }

    fn notify_if_quality_alert_is_due(
        app: &AppHandle,
        hostname: &str,
        settings: &QualityAlertSettings,
        tracker: &mut QualityAlertTracker,
        probe_succeeded: bool,
    ) {
        if should_send_quality_alert(settings, tracker, probe_succeeded, Instant::now()) {
            if let Err(error) = app
                .notification()
                .builder()
                .title("FluxNet: connection problem")
                .body(format!(
                    "{hostname} has failed {} consecutive ICMP probes.",
                    tracker.consecutive_failures
                ))
                .show()
            {
                eprintln!("Could not show network quality notification: {error}");
            }
        }
    }

    fn publish_quality_probe(
        app: &AppHandle,
        history: &Mutex<VecDeque<QualityProbe>>,
        probe: QualityProbe,
    ) -> Result<(), String> {
        let rolling_stats = {
            let mut history = history
                .lock()
                .map_err(|_| "Network quality history lock was poisoned.".to_owned())?;
            append_quality_probe(&mut history, probe.clone());
            quality_stats(&history)
        };
        app.emit(
            "quality-monitor-probe",
            QualityMonitorUpdate {
                probe,
                quality_rating: quality_rating(&rolling_stats),
                rolling_stats,
            },
        )
        .map_err(|error| format!("Could not publish network quality sample: {error}"))
    }

    fn validate_analysis_settings(probe_count: usize, timeout_ms: u64) -> Result<(), String> {
        if !(1..=BDIX_MAX_PROBES).contains(&probe_count) {
            return Err(format!("Choose between 1 and {BDIX_MAX_PROBES} probes."));
        }
        if !(BDIX_MIN_TIMEOUT_MS..=BDIX_MAX_TIMEOUT_MS).contains(&timeout_ms) {
            return Err(format!(
                "Choose a per-probe timeout between {BDIX_MIN_TIMEOUT_MS} and {BDIX_MAX_TIMEOUT_MS} milliseconds."
            ));
        }
        Ok(())
    }

    fn validate_hostname(hostname: &str) -> Result<(), String> {
        if hostname.parse::<IpAddr>().is_ok() {
            return Ok(());
        }
        let domain = hostname.strip_suffix('.').unwrap_or(hostname);
        if hostname.len() > MAX_HOSTNAME_LENGTH
            || domain.is_empty()
            || domain.split('.').any(|label| {
                label.is_empty()
                    || label.len() > 63
                    || label.starts_with('-')
                    || label.ends_with('-')
                    || !label
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            })
        {
            return Err("Enter a valid hostname or IP address (up to 253 characters).".into());
        }
        Ok(())
    }

    #[derive(Debug)]
    enum ResolutionFailure {
        Dns(String),
        DnsTimeout(String),
        NoIpv4,
        Cancelled,
    }

    fn resolve_addresses(
        hostname: &str,
        cancelled: &AtomicBool,
    ) -> Result<Vec<IpAddr>, ResolutionFailure> {
        if let Ok(address) = hostname.parse::<IpAddr>() {
            return Ok(vec![address]);
        }
        if DNS_LOOKUP_ACTIVE
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(ResolutionFailure::Dns(
                "A previous DNS lookup is still pending in Windows. Wait briefly and retry.".into(),
            ));
        }

        let hostname = hostname.to_owned();
        let lookup_hostname = hostname.clone();
        let (sender, receiver) = mpsc::channel();
        let spawn_result = thread::Builder::new()
            .name("fluxnet-bdix-dns".into())
            .spawn(move || {
                let _worker_guard = DnsWorkerGuard;
                let result = (lookup_hostname.as_str(), 0)
                    .to_socket_addrs()
                    .map(|addresses| addresses.map(|address| address.ip()).collect::<Vec<_>>());
                drop(_worker_guard);
                let _ = sender.send(result);
            });
        if let Err(error) = spawn_result {
            DNS_LOOKUP_ACTIVE.store(false, Ordering::Release);
            return Err(ResolutionFailure::Dns(format!(
                "Could not start DNS lookup: {error}"
            )));
        }

        let started = Instant::now();
        loop {
            if cancelled.load(Ordering::Acquire) {
                return Err(ResolutionFailure::Cancelled);
            }
            if started.elapsed() >= DNS_TIMEOUT {
                return Err(ResolutionFailure::DnsTimeout(format!(
                    "DNS lookup for {hostname} exceeded the 8 second timeout."
                )));
            }
            match receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(Ok(addresses)) => {
                    let mut unique_addresses = Vec::new();
                    for address in addresses {
                        if !unique_addresses.contains(&address) {
                            unique_addresses.push(address);
                        }
                    }
                    if unique_addresses.is_empty() {
                        return Err(ResolutionFailure::NoIpv4);
                    }
                    return Ok(unique_addresses);
                }
                Ok(Err(error)) => return Err(map_resolution_error(&hostname, error)),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(ResolutionFailure::Dns(
                        "DNS lookup worker exited without a result.".into(),
                    ));
                }
            }
        }
    }

    fn empty_analysis(
        hostname: String,
        status: &str,
        resolved_addresses: Vec<String>,
        message: Option<String>,
    ) -> BdixAnalysisResult {
        BdixAnalysisResult {
            hostname,
            status: status.into(),
            resolved_addresses,
            probed_address: None,
            attempts: 0,
            successful_attempts: 0,
            packet_loss_percent: None,
            min_latency_ms: None,
            average_latency_ms: None,
            max_latency_ms: None,
            probes: Vec::new(),
            message,
            timestamp_ms: current_timestamp_ms(),
        }
    }

    fn current_timestamp_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64
    }

    fn check_analysis_cancelled(cancelled: &AtomicBool) -> Result<(), String> {
        if cancelled.load(Ordering::Acquire) {
            Err("BDIX analysis stopped by user.".into())
        } else {
            Ok(())
        }
    }

    fn resolve_ipv4(hostname: &str) -> Result<Ipv4Addr, ResolutionFailure> {
        if let Ok(address) = hostname.parse::<Ipv4Addr>() {
            return Ok(address);
        }

        let addresses = (hostname, 0)
            .to_socket_addrs()
            .map_err(|error| map_resolution_error(hostname, error))?;
        addresses
            .filter_map(|address| match address.ip() {
                std::net::IpAddr::V4(ipv4) => Some(ipv4),
                std::net::IpAddr::V6(_) => None,
            })
            .next()
            .ok_or(ResolutionFailure::NoIpv4)
    }

    fn map_resolution_error(hostname: &str, error: std::io::Error) -> ResolutionFailure {
        if error.kind() == std::io::ErrorKind::TimedOut {
            ResolutionFailure::DnsTimeout(format!(
                "DNS lookup for {hostname} timed out before an IP address was returned."
            ))
        } else {
            ResolutionFailure::Dns(format!("Could not resolve {hostname}: {error}"))
        }
    }

    struct IcmpHandle(HANDLE);

    impl IcmpHandle {
        fn create() -> Result<Self, String> {
            let handle = unsafe { IcmpCreateFile() };
            if handle.is_null() || handle == INVALID_HANDLE_VALUE {
                let error = unsafe { GetLastError() };
                return Err(format!(
                    "Windows could not create an ICMP diagnostic handle (error {error}). Check system networking permissions and retry."
                ));
            }
            Ok(Self(handle))
        }
    }

    impl Drop for IcmpHandle {
        fn drop(&mut self) {
            unsafe {
                IcmpCloseHandle(self.0);
            }
        }
    }

    fn send_probe(
        handle: HANDLE,
        destination: Ipv4Addr,
        attempt: usize,
        timeout_ms: u32,
    ) -> Result<ConnectivityProbe, String> {
        let mut reply: ICMP_ECHO_REPLY = unsafe { std::mem::zeroed() };
        let reply_size = size_of::<ICMP_ECHO_REPLY>() as u32;
        let start = Instant::now();
        let reply_count = unsafe {
            IcmpSendEcho(
                handle,
                u32::from_ne_bytes(destination.octets()),
                null(),
                0,
                null(),
                (&mut reply as *mut ICMP_ECHO_REPLY).cast(),
                reply_size,
                timeout_ms,
            )
        };
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

        if reply_count == 0 {
            let error = unsafe { GetLastError() };
            if error == IP_REQ_TIMED_OUT {
                return Ok(timeout_probe(
                    attempt,
                    "No ICMP echo reply before the timeout.",
                ));
            }

            if error == IP_DEST_HOST_UNREACHABLE || error == IP_DEST_NET_UNREACHABLE {
                return Ok(failed_probe(
                    attempt,
                    "unreachable",
                    &format!("Destination unreachable (Windows error {error})."),
                ));
            }
            return Err(format!(
                "Windows ICMP probe failed for {destination} (error {error}). A firewall or system policy may block ICMP."
            ));
        }

        if reply.Status == IP_SUCCESS {
            return Ok(ConnectivityProbe {
                attempt,
                status: "success".into(),
                latency_ms: Some(elapsed_ms),
                detail: None,
            });
        }

        if reply.Status == IP_REQ_TIMED_OUT {
            return Ok(timeout_probe(
                attempt,
                &format!(
                    "No echo reply from destination (ICMP status {}).",
                    reply.Status
                ),
            ));
        }

        Ok(failed_probe(
            attempt,
            "unreachable",
            &format!("No successful echo reply (ICMP status {}).", reply.Status),
        ))
    }

    fn send_analysis_probe(
        handle: HANDLE,
        destination: IpAddr,
        attempt: usize,
        timeout_ms: u32,
    ) -> Result<ConnectivityProbe, String> {
        match destination {
            IpAddr::V4(address) => send_probe(handle, address, attempt, timeout_ms),
            IpAddr::V6(address) => send_ipv6_probe(handle, address, attempt, timeout_ms),
        }
    }

    fn send_ipv6_probe(
        handle: HANDLE,
        destination: Ipv6Addr,
        attempt: usize,
        timeout_ms: u32,
    ) -> Result<ConnectivityProbe, String> {
        let destination_address = SOCKADDR_IN6 {
            sin6_family: AF_INET6,
            sin6_addr: IN6_ADDR {
                u: windows_sys::Win32::Networking::WinSock::IN6_ADDR_0 {
                    Byte: destination.octets(),
                },
            },
            ..Default::default()
        };
        let mut reply: ICMPV6_ECHO_REPLY_LH = unsafe { std::mem::zeroed() };
        let reply_count = unsafe {
            Icmp6SendEcho2(
                handle,
                null_mut(),
                None,
                null(),
                null(),
                &destination_address,
                null(),
                0,
                null(),
                (&mut reply as *mut ICMPV6_ECHO_REPLY_LH).cast(),
                size_of::<ICMPV6_ECHO_REPLY_LH>() as u32,
                timeout_ms,
            )
        };
        if reply_count == 0 {
            let error = unsafe { GetLastError() };
            if error == IP_REQ_TIMED_OUT {
                return Ok(timeout_probe(
                    attempt,
                    "No ICMPv6 echo reply before the timeout.",
                ));
            }
            if error == IP_DEST_HOST_UNREACHABLE || error == IP_DEST_NET_UNREACHABLE {
                return Ok(failed_probe(
                    attempt,
                    "unreachable",
                    &format!("IPv6 destination unreachable (Windows error {error})."),
                ));
            }
            return Err(format!(
                "Windows ICMPv6 probe failed for {destination} (error {error}). A firewall or system policy may block ICMPv6."
            ));
        }

        if reply.Status == IP_SUCCESS {
            return Ok(ConnectivityProbe {
                attempt,
                status: "success".into(),
                latency_ms: Some(reply.RoundTripTime as f64),
                detail: None,
            });
        }
        if reply.Status == IP_REQ_TIMED_OUT {
            return Ok(timeout_probe(
                attempt,
                "No ICMPv6 echo reply before the timeout.",
            ));
        }
        Ok(failed_probe(
            attempt,
            "unreachable",
            &format!("No successful ICMPv6 echo reply (status {}).", reply.Status),
        ))
    }

    fn timeout_probe(attempt: usize, detail: &str) -> ConnectivityProbe {
        failed_probe(attempt, "timeout", detail)
    }

    fn failed_probe(attempt: usize, status: &str, detail: &str) -> ConnectivityProbe {
        ConnectivityProbe {
            attempt,
            status: status.into(),
            latency_ms: None,
            detail: Some(detail.into()),
        }
    }

    #[derive(Debug, PartialEq)]
    struct LatencySummary {
        packet_loss_percent: Option<f64>,
        min_latency_ms: Option<f64>,
        average_latency_ms: Option<f64>,
        max_latency_ms: Option<f64>,
    }

    fn summarize_latencies(latencies_ms: &[f64], attempts: usize) -> LatencySummary {
        if attempts == 0 {
            return LatencySummary {
                packet_loss_percent: None,
                min_latency_ms: None,
                average_latency_ms: None,
                max_latency_ms: None,
            };
        }
        if latencies_ms.is_empty() {
            return LatencySummary {
                packet_loss_percent: Some(100.0),
                min_latency_ms: None,
                average_latency_ms: None,
                max_latency_ms: None,
            };
        }

        let min_latency_ms = latencies_ms.iter().copied().reduce(f64::min);
        let max_latency_ms = latencies_ms.iter().copied().reduce(f64::max);
        let average_latency_ms = Some(latencies_ms.iter().sum::<f64>() / latencies_ms.len() as f64);
        let packet_loss_percent =
            Some((attempts - latencies_ms.len()) as f64 / attempts as f64 * 100.0);

        LatencySummary {
            packet_loss_percent,
            min_latency_ms,
            average_latency_ms,
            max_latency_ms,
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn maps_common_and_unrecognized_adapter_type_identifiers() {
            assert_eq!(
                adapter_type(IF_TYPE_ETHERNET_CSMACD).as_deref(),
                Some("Ethernet")
            );
            assert_eq!(adapter_type(IF_TYPE_IEEE80211).as_deref(), Some("Wi-Fi"));
            assert_eq!(adapter_type(9999).as_deref(), Some("Other (type 9999)"));
        }

        #[test]
        fn classifies_virtual_and_physical_adapters_without_changing_system_type() {
            assert_eq!(adapter_category(IF_TYPE_ETHERNET_CSMACD, false), "Ethernet");
            assert_eq!(adapter_category(IF_TYPE_IEEE80211, false), "Wi-Fi");
            assert!(is_virtual_adapter(
                IF_TYPE_ETHERNET_CSMACD,
                "vEthernet (Default Switch)",
                "Hyper-V Virtual Ethernet Adapter"
            ));
            assert!(is_virtual_adapter(
                IF_TYPE_TUNNEL,
                "VPN",
                "Tunnel interface"
            ));
            assert!(!is_virtual_adapter(
                IF_TYPE_ETHERNET_CSMACD,
                "Ethernet",
                "Intel(R) Ethernet Controller"
            ));
        }

        #[test]
        fn formats_physical_addresses_from_windows_adapter_data() {
            assert_eq!(
                physical_address([0, 1, 0xA, 0xFF, 0, 0, 0, 0], 4).as_deref(),
                Some("00:01:0A:FF")
            );
            assert_eq!(physical_address([0; 8], 0), None);
        }

        #[test]
        fn maps_operational_statuses_including_unrecognized_values() {
            assert_eq!(operational_status(NET_IF_OPER_STATUS_UP), "Up");
            assert_eq!(operational_status(NET_IF_OPER_STATUS_DOWN), "Down");
            assert_eq!(operational_status(9999), "Unknown (9999)");
        }

        #[test]
        fn recognizes_only_the_ipv4_link_local_range() {
            assert!(is_link_local_ipv4(Ipv4Addr::new(169, 254, 0, 1)));
            assert!(is_link_local_ipv4(Ipv4Addr::new(169, 254, 255, 254)));
            assert!(!is_link_local_ipv4(Ipv4Addr::new(169, 253, 1, 1)));
            assert!(!is_link_local_ipv4(Ipv4Addr::new(192, 168, 1, 1)));
        }

        #[test]
        fn calculates_actual_probe_packet_loss_and_latency_statistics() {
            let summary = summarize_latencies(&[10.0, 20.0, 30.0], 4);
            assert_eq!(summary.packet_loss_percent, Some(25.0));
            assert_eq!(summary.min_latency_ms, Some(10.0));
            assert_eq!(summary.average_latency_ms, Some(20.0));
            assert_eq!(summary.max_latency_ms, Some(30.0));

            let all_lost = summarize_latencies(&[], 4);
            assert_eq!(all_lost.packet_loss_percent, Some(100.0));
            assert_eq!(all_lost.average_latency_ms, None);
        }

        #[test]
        fn validates_user_supplied_hosts_before_dns_lookup() {
            assert!(validate_hostname("").is_err());
            assert!(validate_hostname("example.com").is_ok());
            assert!(validate_hostname("example.com.").is_ok());
            assert!(validate_hostname("1.1.1.1").is_ok());
            assert!(validate_hostname("2606:4700:4700::1111").is_ok());
            assert!(validate_hostname("host name").is_err());
            assert!(validate_hostname("example.com:443").is_err());
            assert!(validate_hostname("example.com/anything").is_err());
            assert!(validate_hostname("example.com;ping").is_err());
            assert!(validate_hostname("-invalid.example").is_err());
        }

        #[test]
        fn validates_configurable_probe_count_and_timeout_bounds() {
            assert!(validate_analysis_settings(4, 1200).is_ok());
            assert!(validate_analysis_settings(1, BDIX_MIN_TIMEOUT_MS).is_ok());
            assert!(validate_analysis_settings(BDIX_MAX_PROBES, BDIX_MAX_TIMEOUT_MS).is_ok());
            assert!(validate_analysis_settings(0, 1200).is_err());
            assert!(validate_analysis_settings(BDIX_MAX_PROBES + 1, 1200).is_err());
            assert!(validate_analysis_settings(4, BDIX_MIN_TIMEOUT_MS - 1).is_err());
            assert!(validate_analysis_settings(4, BDIX_MAX_TIMEOUT_MS + 1).is_err());
        }

        #[test]
        fn validates_network_quality_monitor_targets_and_intervals() {
            assert!(validate_quality_settings("1.1.1.1", 1).is_ok());
            assert!(validate_quality_settings("example.com", 2).is_ok());
            assert!(validate_quality_settings("2606:4700:4700::1111", 5).is_ok());
            assert!(validate_quality_settings("example.com", 3).is_err());
            assert!(validate_quality_settings("example.com;ping", 1).is_err());
        }

        #[test]
        fn cancelled_analysis_returns_without_starting_probes() {
            let cancelled = Arc::new(AtomicBool::new(true));
            let result = analyze_target("1.1.1.1".into(), 4, 1200, cancelled);
            assert_eq!(result.unwrap_err(), "BDIX analysis stopped by user.");
        }

        #[test]
        fn uses_ipv4_literals_without_dns_resolution() {
            assert_eq!(
                resolve_ipv4("1.1.1.1").expect("literal IPv4 address should parse"),
                Ipv4Addr::new(1, 1, 1, 1)
            );
        }

        #[test]
        fn resolves_ipv6_literals_without_dns_resolution() {
            let addresses = resolve_addresses("::1", &AtomicBool::new(false))
                .expect("literal IPv6 address should parse");
            assert_eq!(addresses, vec![IpAddr::V6(Ipv6Addr::LOCALHOST)]);
        }

        #[test]
        fn distinguishes_dns_lookup_timeouts_from_resolution_failures() {
            assert!(matches!(
                map_resolution_error(
                    "example.com",
                    std::io::Error::from(std::io::ErrorKind::TimedOut)
                ),
                ResolutionFailure::DnsTimeout(_)
            ));
            assert!(matches!(
                map_resolution_error(
                    "missing.invalid",
                    std::io::Error::from(std::io::ErrorKind::NotFound)
                ),
                ResolutionFailure::Dns(_)
            ));
        }

        #[test]
        fn transforms_ipv4_sockaddr_in_network_byte_order() {
            let mut sockaddr = SOCKADDR_IN {
                sin_family: AF_INET,
                ..Default::default()
            };
            sockaddr.sin_addr.S_un.S_addr = u32::from_ne_bytes([192, 168, 1, 25]);
            let socket_address = SOCKET_ADDRESS {
                lpSockaddr: (&mut sockaddr as *mut SOCKADDR_IN).cast(),
                iSockaddrLength: size_of::<SOCKADDR_IN>() as i32,
            };

            let result = unsafe { socket_address_to_ipv4(socket_address) };
            assert_eq!(result, Some(Ipv4Addr::new(192, 168, 1, 25)));
        }

        #[test]
        fn enumerates_interfaces_using_the_windows_api() {
            let interfaces = get_network_interfaces().expect("Windows interface query failed");
            assert!(
                interfaces
                    .iter()
                    .all(|interface| !interface.name.trim().is_empty())
            );
        }
    }
}

#[cfg(windows)]
#[tauri::command]
pub async fn get_network_interfaces() -> Result<Vec<NetworkInterface>, String> {
    tauri::async_runtime::spawn_blocking(windows::get_network_interfaces)
        .await
        .map_err(|error| format!("The network interface scan task failed: {error}"))?
}

#[cfg(windows)]
#[tauri::command]
pub async fn test_connectivity(hostname: String) -> Result<ConnectivityDiagnostic, String> {
    tauri::async_runtime::spawn_blocking(move || windows::test_connectivity(hostname))
        .await
        .map_err(|error| format!("The connectivity diagnostic task failed: {error}"))?
}

#[cfg(windows)]
#[tauri::command]
pub async fn analyze_bdix_target(
    control: tauri::State<'_, BdixAnalysisControl>,
    hostname: String,
    probe_count: usize,
    timeout_ms: u64,
) -> Result<BdixAnalysisResult, String> {
    if control
        .running
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err("A BDIX analysis is already running.".into());
    }
    control.cancelled.store(false, Ordering::Release);

    let running = Arc::clone(&control.running);
    let cancelled = Arc::clone(&control.cancelled);
    tauri::async_runtime::spawn_blocking(move || {
        let _running_guard = AnalysisRunningGuard(running);
        windows::analyze_target(hostname, probe_count, timeout_ms, cancelled)
    })
    .await
    .map_err(|error| format!("The BDIX analysis task failed: {error}"))?
}

#[cfg(windows)]
#[tauri::command]
pub fn cancel_bdix_analysis(control: tauri::State<'_, BdixAnalysisControl>) -> Result<(), String> {
    if !control.running.load(Ordering::Acquire) {
        return Err("No BDIX analysis is currently running.".into());
    }
    control.cancelled.store(true, Ordering::Release);
    Ok(())
}

#[cfg(windows)]
#[tauri::command]
pub fn start_quality_monitor(
    app: tauri::AppHandle,
    control: tauri::State<'_, QualityMonitorControl>,
    hostname: String,
    interval_seconds: u64,
) -> Result<(), String> {
    let hostname = hostname.trim().to_owned();
    start_quality_monitor_inner(app, &control, hostname, interval_seconds)
}

#[cfg(windows)]
fn start_quality_monitor_inner(
    app: tauri::AppHandle,
    control: &QualityMonitorControl,
    hostname: String,
    interval_seconds: u64,
) -> Result<(), String> {
    windows::validate_quality_settings(&hostname, interval_seconds)?;
    if control
        .running
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err("Network quality monitoring is already running.".into());
    }
    control.cancelled.store(false, Ordering::Release);

    match control.settings.lock() {
        Ok(mut settings) => {
            settings.target = hostname.clone();
            settings.interval_seconds = interval_seconds;
        }
        Err(_) => {
            control.running.store(false, Ordering::Release);
            return Err("Network quality monitor settings lock was poisoned.".into());
        }
    }

    match control.history.lock() {
        Ok(mut history) => history.clear(),
        Err(_) => {
            control.running.store(false, Ordering::Release);
            return Err("Network quality history lock was poisoned.".into());
        }
    }

    let running = Arc::clone(&control.running);
    let cancelled = Arc::clone(&control.cancelled);
    let history = Arc::clone(&control.history);
    let settings = Arc::clone(&control.settings);
    let alert_settings = Arc::clone(&control.alert_settings);
    tauri::async_runtime::spawn_blocking(move || {
        let _running_guard = QualityRunningGuard(running);
        windows::run_quality_monitor(app, hostname, settings, cancelled, history, alert_settings);
    });
    Ok(())
}

#[cfg(windows)]
pub fn start_saved_quality_monitor(app: tauri::AppHandle) -> Result<(), String> {
    let control = app.state::<QualityMonitorControl>();
    let settings = control
        .settings
        .lock()
        .map_err(|_| "Network quality monitor settings lock was poisoned.".to_owned())?
        .clone();
    start_quality_monitor_inner(
        app.clone(),
        &control,
        settings.target,
        settings.interval_seconds,
    )
}

#[cfg(windows)]
#[tauri::command]
pub fn stop_quality_monitor(
    control: tauri::State<'_, QualityMonitorControl>,
) -> Result<(), String> {
    stop_quality_monitor_inner(&control)
}

fn stop_quality_monitor_inner(control: &QualityMonitorControl) -> Result<(), String> {
    if !control.running.load(Ordering::Acquire) {
        return Err("Network quality monitoring is not running.".into());
    }
    control.cancelled.store(true, Ordering::Release);
    Ok(())
}

pub fn stop_saved_quality_monitor(app: &tauri::AppHandle) -> Result<(), String> {
    stop_quality_monitor_inner(&app.state::<QualityMonitorControl>())
}

#[tauri::command]
pub fn configure_quality_monitor_options(
    control: tauri::State<'_, QualityMonitorControl>,
    hostname: String,
    interval_seconds: u64,
) -> Result<(), String> {
    if control.running.load(Ordering::Acquire) {
        return Err(
            "Stop Network Quality monitoring before changing its target or interval.".into(),
        );
    }
    if ![1, 2, 5].contains(&interval_seconds) {
        return Err("Choose a probe interval of 1, 2, or 5 seconds.".into());
    }
    let mut settings = control
        .settings
        .lock()
        .map_err(|_| "Network quality monitor settings lock was poisoned.".to_owned())?;
    settings.target = hostname.trim().to_owned();
    settings.interval_seconds = interval_seconds;
    Ok(())
}

pub fn apply_saved_quality_preferences(
    control: &QualityMonitorControl,
    interval_seconds: u64,
    ping_timeout_ms: u64,
    alert_settings: QualityAlertSettings,
) -> Result<(), String> {
    if control.running.load(Ordering::Acquire) {
        return Err("Stop Network Quality monitoring before changing its preferences.".into());
    }
    if !QUALITY_INTERVALS.contains(&interval_seconds) {
        return Err("Choose a probe interval of 1, 2, or 5 seconds.".into());
    }
    if !(500..=5000).contains(&ping_timeout_ms) {
        return Err("Choose a ping timeout from 500 to 5000 milliseconds.".into());
    }
    if !(2..=10).contains(&alert_settings.failure_threshold)
        || ![60, 300, 900].contains(&alert_settings.cooldown_seconds)
    {
        return Err("The saved network alert preferences are invalid.".into());
    }
    let mut settings = control
        .settings
        .lock()
        .map_err(|_| "Network quality monitor settings lock was poisoned.".to_owned())?;
    settings.interval_seconds = interval_seconds;
    settings.ping_timeout_ms = ping_timeout_ms;
    *control
        .alert_settings
        .lock()
        .map_err(|_| "Network quality alert settings lock was poisoned.".to_owned())? =
        alert_settings;
    Ok(())
}

#[tauri::command]
pub fn configure_quality_monitor_preferences(
    control: tauri::State<'_, QualityMonitorControl>,
    interval_seconds: u64,
    ping_timeout_ms: u64,
) -> Result<(), String> {
    update_quality_monitor_preferences(&control, interval_seconds, ping_timeout_ms)
}

fn update_quality_monitor_preferences(
    control: &QualityMonitorControl,
    interval_seconds: u64,
    ping_timeout_ms: u64,
) -> Result<(), String> {
    if !QUALITY_INTERVALS.contains(&interval_seconds) {
        return Err("Choose a probe interval of 1, 2, or 5 seconds.".into());
    }
    if !(500..=5000).contains(&ping_timeout_ms) {
        return Err("Choose a ping timeout from 500 to 5000 milliseconds.".into());
    }
    let mut settings = control
        .settings
        .lock()
        .map_err(|_| "Network quality monitor settings lock was poisoned.".to_owned())?;
    settings.interval_seconds = interval_seconds;
    settings.ping_timeout_ms = ping_timeout_ms;
    Ok(())
}

pub fn request_quality_monitor_stop(control: &QualityMonitorControl) -> Arc<AtomicBool> {
    control.cancelled.store(true, Ordering::Release);
    Arc::clone(&control.running)
}

#[tauri::command]
pub fn configure_quality_alerts(
    control: tauri::State<'_, QualityMonitorControl>,
    enabled: bool,
    failure_threshold: u32,
    cooldown_seconds: u64,
) -> Result<(), String> {
    if !(2..=10).contains(&failure_threshold) {
        return Err("Choose an alert threshold between 2 and 10 failed probes.".into());
    }
    if ![60, 300, 900].contains(&cooldown_seconds) {
        return Err("Choose an alert cooldown of 1, 5, or 15 minutes.".into());
    }
    let mut settings = control
        .alert_settings
        .lock()
        .map_err(|_| "Network quality alert settings lock was poisoned.".to_owned())?;
    *settings = QualityAlertSettings {
        enabled,
        failure_threshold,
        cooldown_seconds,
    };
    Ok(())
}

#[tauri::command]
pub fn get_quality_monitor_snapshot(
    control: tauri::State<'_, QualityMonitorControl>,
) -> Result<QualityMonitorSnapshot, String> {
    let settings = control
        .settings
        .lock()
        .map_err(|_| "Network quality monitor settings lock was poisoned.".to_owned())?
        .clone();
    let history = control
        .history
        .lock()
        .map_err(|_| "Network quality history lock was poisoned.".to_owned())?
        .clone();
    let alert_settings = control
        .alert_settings
        .lock()
        .map_err(|_| "Network quality alert settings lock was poisoned.".to_owned())?
        .clone();
    let rolling_stats = quality_stats(&history);
    Ok(QualityMonitorSnapshot {
        running: control.running.load(Ordering::Acquire),
        target: settings.target,
        interval_seconds: settings.interval_seconds,
        history: history.into_iter().collect(),
        quality_rating: quality_rating(&rolling_stats),
        rolling_stats,
        alert_settings,
    })
}

#[cfg(windows)]
#[tauri::command]
pub fn clear_quality_monitor_history(
    control: tauri::State<'_, QualityMonitorControl>,
) -> Result<(), String> {
    control
        .history
        .lock()
        .map_err(|_| "Network quality history lock was poisoned.".to_owned())?
        .clear();
    Ok(())
}

#[cfg(windows)]
#[tauri::command]
pub fn export_quality_monitor_history(
    control: tauri::State<'_, QualityMonitorControl>,
    path: String,
) -> Result<usize, String> {
    let path = Path::new(&path);
    if !path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("csv"))
    {
        return Err("Choose a destination with a .csv file extension.".into());
    }
    let history = control
        .history
        .lock()
        .map_err(|_| "Network quality history lock was poisoned.".to_owned())?
        .clone();
    if history.is_empty() {
        return Err("There are no recorded probes to export.".into());
    }
    let count = history.len();
    let csv = quality_history_csv(&history)?;
    std::fs::write(path, csv).map_err(|error| format!("Could not write the CSV file: {error}"))?;
    Ok(count)
}

#[cfg(not(windows))]
#[tauri::command]
pub fn get_network_interfaces() -> Result<Vec<NetworkInterface>, String> {
    Err("Network interface detection is only available on Windows.".into())
}

#[cfg(not(windows))]
#[tauri::command]
pub async fn test_connectivity(_hostname: String) -> Result<ConnectivityDiagnostic, String> {
    Err("Connectivity diagnostics are only available on Windows.".into())
}

#[cfg(not(windows))]
#[tauri::command]
pub async fn analyze_bdix_target(
    _control: tauri::State<'_, BdixAnalysisControl>,
    _hostname: String,
    _probe_count: usize,
    _timeout_ms: u64,
) -> Result<BdixAnalysisResult, String> {
    Err("BDIX analysis is only available on Windows.".into())
}

#[cfg(not(windows))]
#[tauri::command]
pub fn cancel_bdix_analysis(_control: tauri::State<'_, BdixAnalysisControl>) -> Result<(), String> {
    Err("BDIX analysis is only available on Windows.".into())
}

#[cfg(not(windows))]
#[tauri::command]
pub fn start_quality_monitor(
    _app: tauri::AppHandle,
    _control: tauri::State<'_, QualityMonitorControl>,
    _hostname: String,
    _interval_seconds: u64,
) -> Result<(), String> {
    Err("Network quality monitoring is only available on Windows.".into())
}

#[cfg(not(windows))]
#[tauri::command]
pub fn stop_quality_monitor(
    _control: tauri::State<'_, QualityMonitorControl>,
) -> Result<(), String> {
    Err("Network quality monitoring is only available on Windows.".into())
}

#[cfg(not(windows))]
#[tauri::command]
pub fn clear_quality_monitor_history(
    _control: tauri::State<'_, QualityMonitorControl>,
) -> Result<(), String> {
    Err("Network quality monitoring is only available on Windows.".into())
}

#[cfg(not(windows))]
#[tauri::command]
pub fn export_quality_monitor_history(
    _control: tauri::State<'_, QualityMonitorControl>,
    _path: String,
) -> Result<usize, String> {
    Err("Network quality monitoring is only available on Windows.".into())
}

#[cfg(test)]
mod quality_monitor_tests {
    use super::*;

    #[test]
    fn quality_preferences_can_be_updated_while_monitoring_is_active() {
        let control = QualityMonitorControl::default();
        control.running.store(true, Ordering::Release);

        update_quality_monitor_preferences(&control, 5, 1_500)
            .expect("valid preferences should apply during an active monitor");

        let settings = control
            .settings
            .lock()
            .expect("settings lock should be available");
        assert_eq!(settings.interval_seconds, 5);
        assert_eq!(settings.ping_timeout_ms, 1_500);
        assert!(update_quality_monitor_preferences(&control, 3, 1_500).is_err());
    }

    fn probe(status: &str, latency_ms: Option<f64>, timestamp_ms: u64) -> QualityProbe {
        QualityProbe {
            target: "test.example".into(),
            timestamp_ms,
            status: status.into(),
            latency_ms,
            detail: (status != "success").then(|| format!("Probe {status}")),
        }
    }

    #[test]
    fn quality_statistics_use_successful_probe_latencies_and_failures_for_loss() {
        let history = VecDeque::from([
            probe("success", Some(10.0), 1),
            probe("timeout", None, 2),
            probe("success", Some(20.0), 3),
            probe("unreachable", None, 4),
        ]);
        let stats = quality_stats(&history);

        assert_eq!(stats.samples, 4);
        assert_eq!(stats.packet_loss_percent, Some(50.0));
        assert_eq!(stats.min_latency_ms, Some(10.0));
        assert_eq!(stats.average_latency_ms, Some(15.0));
        assert_eq!(stats.max_latency_ms, Some(20.0));
    }

    #[test]
    fn failed_samples_count_toward_loss_but_have_no_latency_statistics() {
        let history = VecDeque::from([
            probe("timeout", None, 1),
            probe("dnsResolutionFailed", None, 2),
            probe("unreachable", None, 3),
        ]);
        let stats = quality_stats(&history);

        assert_eq!(stats.samples, 3);
        assert_eq!(stats.packet_loss_percent, Some(100.0));
        assert_eq!(stats.min_latency_ms, None);
        assert_eq!(stats.average_latency_ms, None);
        assert_eq!(stats.max_latency_ms, None);
    }

    #[test]
    fn rolling_statistics_only_include_the_latest_sixty_samples() {
        let history = (0..QUALITY_ROLLING_WINDOW + 1)
            .map(|index| {
                if index == 0 {
                    probe("timeout", None, index as u64)
                } else {
                    probe("success", Some(index as f64), index as u64)
                }
            })
            .collect::<VecDeque<_>>();
        let stats = quality_stats(&history);

        assert_eq!(stats.samples, QUALITY_ROLLING_WINDOW);
        assert_eq!(stats.packet_loss_percent, Some(0.0));
        assert_eq!(stats.min_latency_ms, Some(1.0));
        assert_eq!(stats.max_latency_ms, Some(60.0));
    }

    #[test]
    fn retained_probe_history_is_bounded_to_three_hundred_entries() {
        let mut history = VecDeque::new();
        for timestamp_ms in 0..QUALITY_HISTORY_LIMIT + 25 {
            append_quality_probe(
                &mut history,
                probe("success", Some(timestamp_ms as f64), timestamp_ms as u64),
            );
        }

        assert_eq!(history.len(), QUALITY_HISTORY_LIMIT);
        assert_eq!(history.front().unwrap().timestamp_ms, 25);
        assert_eq!(
            history.back().unwrap().timestamp_ms,
            (QUALITY_HISTORY_LIMIT + 24) as u64
        );
    }

    #[test]
    fn quality_rating_applies_latency_and_loss_thresholds_inclusively() {
        let cases = [
            (50.0, 1.0, "excellent"),
            (100.0, 2.0, "good"),
            (200.0, 5.0, "fair"),
            (200.1, 0.0, "poor"),
            (50.0, 1.1, "good"),
            (100.0, 2.1, "fair"),
            (200.0, 5.1, "poor"),
        ];
        for (latency, loss, expected) in cases {
            let rating = quality_rating(&QualityStats {
                samples: 10,
                packet_loss_percent: Some(loss),
                min_latency_ms: Some(latency),
                average_latency_ms: Some(latency),
                max_latency_ms: Some(latency),
            });
            assert_eq!(rating.level, expected, "latency={latency}, loss={loss}");
        }
    }

    #[test]
    fn no_data_and_all_failed_probes_never_receive_positive_ratings() {
        assert_eq!(
            quality_rating(&quality_stats(&VecDeque::new())).level,
            "noData"
        );
        let failures = VecDeque::from([probe("timeout", None, 1), probe("unreachable", None, 2)]);
        assert_eq!(
            quality_rating(&quality_stats(&failures)).level,
            "disconnected"
        );
    }

    #[test]
    fn rating_uses_packet_loss_from_failed_outcomes() {
        let history = (0..QUALITY_ROLLING_WINDOW)
            .map(|index| {
                if index == 0 {
                    probe("timeout", None, index as u64)
                } else {
                    probe("success", Some(40.0), index as u64)
                }
            })
            .collect::<VecDeque<_>>();
        let stats = quality_stats(&history);

        assert_eq!(stats.packet_loss_percent, Some(100.0 / 60.0));
        assert_eq!(quality_rating(&stats).level, "good");
    }

    #[test]
    fn alerts_trigger_only_after_configured_consecutive_failures() {
        let settings = QualityAlertSettings {
            enabled: true,
            failure_threshold: 3,
            cooldown_seconds: 300,
        };
        let now = Instant::now();
        let mut tracker = QualityAlertTracker::default();

        assert!(!should_send_quality_alert(
            &settings,
            &mut tracker,
            false,
            now
        ));
        assert!(!should_send_quality_alert(
            &settings,
            &mut tracker,
            false,
            now + std::time::Duration::from_secs(1)
        ));
        assert!(should_send_quality_alert(
            &settings,
            &mut tracker,
            false,
            now + std::time::Duration::from_secs(2)
        ));
    }

    #[test]
    fn alerts_respect_cooldown_and_reset_failure_streak_on_success() {
        let settings = QualityAlertSettings {
            enabled: true,
            failure_threshold: 2,
            cooldown_seconds: 60,
        };
        let now = Instant::now();
        let mut tracker = QualityAlertTracker::default();

        assert!(!should_send_quality_alert(
            &settings,
            &mut tracker,
            false,
            now
        ));
        assert!(should_send_quality_alert(
            &settings,
            &mut tracker,
            false,
            now + std::time::Duration::from_secs(1)
        ));
        assert!(!should_send_quality_alert(
            &settings,
            &mut tracker,
            false,
            now + std::time::Duration::from_secs(2)
        ));
        assert!(!should_send_quality_alert(
            &settings,
            &mut tracker,
            true,
            now + std::time::Duration::from_secs(3)
        ));
        assert!(!should_send_quality_alert(
            &settings,
            &mut tracker,
            false,
            now + std::time::Duration::from_secs(4)
        ));
        assert!(!should_send_quality_alert(
            &settings,
            &mut tracker,
            false,
            now + std::time::Duration::from_secs(59)
        ));
        assert!(should_send_quality_alert(
            &settings,
            &mut tracker,
            false,
            now + std::time::Duration::from_secs(61)
        ));
    }

    #[test]
    fn disabled_alerts_do_not_notify_and_do_not_accumulate_failures() {
        let settings = QualityAlertSettings::default();
        let mut tracker = QualityAlertTracker::default();
        let now = Instant::now();

        for elapsed in 0..5 {
            assert!(!should_send_quality_alert(
                &settings,
                &mut tracker,
                false,
                now + std::time::Duration::from_secs(elapsed)
            ));
        }
        assert_eq!(tracker.consecutive_failures, 0);
    }

    #[test]
    fn csv_escapes_commas_quotes_and_newlines() {
        let history = VecDeque::from([QualityProbe {
            target: "host, \"one\"\nbackup".into(),
            timestamp_ms: 123,
            status: "unreachable".into(),
            latency_ms: None,
            detail: Some("route, \"blocked\"\ncheck network".into()),
        }]);

        let csv = quality_history_csv(&history).unwrap();
        assert!(csv.contains(
            "1970-01-01T00:00:00.123Z,\"host, \"\"one\"\"\nbackup\",failure,,\"route, \"\"blocked\"\"\ncheck network\"\r\n"
        ));
        let records = parse_csv_records(&csv);
        assert_eq!(records.len(), 2);
        assert!(records.iter().all(|record| record.len() == 5));
        assert_eq!(records[1][4], "route, \"blocked\"\ncheck network");
    }

    #[test]
    fn csv_timestamps_are_iso_8601_utc_and_latency_is_limited_to_two_decimals() {
        let history = VecDeque::from([
            probe("success", Some(52.088000000000006), 1_767_687_300_123),
            probe("success", Some(20.0), 1_767_687_301_456),
            probe("timeout", None, 1_767_687_302_789),
        ]);
        let csv = quality_history_csv(&history).unwrap();
        let records = parse_csv_records(&csv);

        assert_eq!(records[0][0], "Timestamp (ISO 8601 UTC)");
        assert_eq!(records[1][0], "2026-01-06T08:15:00.123Z");
        assert_eq!(records[1][3], "52.09");
        assert_eq!(records[2][3], "20");
        assert_eq!(records[3][3], "");
        assert_eq!(records[3][4], "Probe timeout");
    }

    #[test]
    fn csv_export_preserves_chronological_order_and_empty_history_has_header() {
        let history = VecDeque::from([
            probe("success", Some(1.0), 100),
            probe("timeout", None, 200),
        ]);
        let csv = quality_history_csv(&history).unwrap();
        assert!(
            csv.find("\r\n1970-01-01T00:00:00.100Z,").unwrap()
                < csv.find("\r\n1970-01-01T00:00:00.200Z,").unwrap()
        );
        assert_eq!(
            quality_history_csv(&VecDeque::new()).unwrap(),
            "Timestamp (ISO 8601 UTC),Target hostname/IP,Probe result,Latency (ms),Error or failure reason\r\n"
        );
    }

    fn parse_csv_records(csv: &str) -> Vec<Vec<String>> {
        let mut records = Vec::new();
        let mut record = Vec::new();
        let mut field = String::new();
        let mut chars = csv.chars().peekable();
        let mut quoted = false;

        while let Some(character) = chars.next() {
            match character {
                '"' if quoted && chars.peek() == Some(&'"') => {
                    field.push('"');
                    chars.next();
                }
                '"' => quoted = !quoted,
                ',' if !quoted => record.push(std::mem::take(&mut field)),
                '\r' if !quoted && chars.peek() == Some(&'\n') => {
                    chars.next();
                    record.push(std::mem::take(&mut field));
                    records.push(std::mem::take(&mut record));
                }
                _ => field.push(character),
            }
        }
        if !field.is_empty() || !record.is_empty() {
            record.push(field);
            records.push(record);
        }
        records
    }
}
