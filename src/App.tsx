import { useCallback, useEffect, useRef, useState, type FormEvent, type ReactNode } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { open as openDialog, save } from "@tauri-apps/plugin-dialog";
import { createBrowserHandoffRunner } from "./browserHandoff.mjs";
import fluxnetSymbol from "./assets/fluxnet-symbol-128.png";
import "./App.css";

type IconName =
  | "dashboard"
  | "interfaces"
  | "speed"
  | "analysis"
  | "monitor"
  | "settings"
  | "globe"
  | "download"
  | "video"
  | "upload"
  | "latency"
  | "packet"
  | "arrow"
  | "signal"
  | "chevron"
  | "refresh";

type NetworkInterface = {
  name: string;
  adapterType: string | null;
  description: string | null;
  category: string;
  isVirtual: boolean;
  operationalStatus: string;
  macAddress: string | null;
  ipv4Address: string | null;
  ipv4Addresses: string[];
  ipv4IsLinkLocal: boolean;
  ipv6Addresses: string[];
  gatewayAddresses: string[];
  defaultGateway: string | null;
  dnsServers: string[];
  linkSpeedMbps: number | null;
  bytesReceived: string | null;
  bytesSent: string | null;
  downloadMbps: number | null;
  uploadMbps: number | null;
  wifiSsid: string | null;
  wifiSignalQuality: number | null;
  wifiFrequencyMhz: number | null;
  wifiReceiveMbps: number | null;
  wifiTransmitMbps: number | null;
  wifiError: string | null;
  dataWarning: string | null;
};

type ConnectivityProbe = {
  attempt: number;
  status: string;
  latencyMs: number | null;
  detail: string | null;
};

type ConnectivityDiagnostic = {
  hostname: string;
  status: "success" | "timeout" | "dnsResolutionFailed" | "noIpv4Address" | "unreachable";
  resolvedIpv4: string | null;
  attempts: number;
  successfulAttempts: number;
  packetLossPercent: number | null;
  minLatencyMs: number | null;
  averageLatencyMs: number | null;
  maxLatencyMs: number | null;
  probes: ConnectivityProbe[];
  message: string | null;
};

type QualityProbe = {
  target: string;
  timestampMs: number;
  status: string;
  latencyMs: number | null;
  detail: string | null;
};

type QualityStats = {
  samples: number;
  packetLossPercent: number | null;
  minLatencyMs: number | null;
  averageLatencyMs: number | null;
  maxLatencyMs: number | null;
};

type QualityMonitorUpdate = {
  probe: QualityProbe;
  rollingStats: QualityStats;
  qualityRating: {
    level: "excellent" | "good" | "fair" | "poor" | "disconnected" | "noData";
    explanation: string;
  };
  interfaces: NetworkInterface[];
  downloadMbps: number;
  uploadMbps: number;
  interfaceError: string | null;
};

type QualityMonitorStatus = {
  status: "stopped" | "error";
  message: string | null;
};

type QualityAlertSettings = {
  enabled: boolean;
  failureThreshold: number;
  cooldownSeconds: number;
};

type QualityMonitorSnapshot = {
  running: boolean;
  target: string;
  intervalSeconds: number;
  history: QualityProbe[];
  rollingStats: QualityStats;
  qualityRating: QualityMonitorUpdate["qualityRating"];
  alertSettings: QualityAlertSettings;
};

type BdixAnalysisResult = {
  hostname: string;
  status: "success" | "partial" | "timeout" | "dnsResolutionFailed" | "noAddress" | "unreachable";
  resolvedAddresses: string[];
  probedAddress: string | null;
  attempts: number;
  successfulAttempts: number;
  packetLossPercent: number | null;
  minLatencyMs: number | null;
  averageLatencyMs: number | null;
  maxLatencyMs: number | null;
  probes: ConnectivityProbe[];
  message: string | null;
  timestampMs: number;
};

type SavedBdixServer = {
  id: string;
  name: string;
  target: string;
  verificationSource: string;
  verificationNotes: string;
  verified: boolean;
};

type SpeedTestProgress = {
  phase: "download" | "upload";
  bytesTransferred: number;
  totalBytes: number;
  percent: number;
  elapsedMs: number;
};

type SpeedTestResult = {
  endpoint: string;
  downloadMbps: number;
  uploadMbps: number;
  downloadElapsedMs: number;
  uploadElapsedMs: number;
  elapsedMs: number;
  downloadBytes: number;
  uploadBytes: number;
};

type DownloadEntry = {
  id: string;
  url: string;
  destination: string;
  filename: string;
  status: "queued" | "downloading" | "paused" | "completed" | "failed" | "cancelled";
  totalBytes: number | null;
  downloadedBytes: number;
  speedBytesPerSecond: number;
  elapsedMs: number;
  remainingSeconds: number | null;
  error: string | null;
  createdAtMs: number;
  updatedAtMs: number;
  attempts: number;
};

type DownloadSettings = {
  maxConcurrent: number;
  bandwidthLimitMbps: number;
  notifyOnComplete: boolean;
  notifyOnFailure: boolean;
};

type AppSettings = {
  launchOnStartup: boolean;
  minimizeToTray: boolean;
  startMinimized: boolean;
  theme: "dark" | "light" | "system";
  defaultDownloadDirectory: string | null;
  networkIntervalSeconds: number;
  pingTimeoutMs: number;
  networkAlertsEnabled: boolean;
  networkAlertFailureThreshold: number;
  networkAlertCooldownSeconds: number;
};

const DEFAULT_APP_SETTINGS: AppSettings = {
  launchOnStartup: false,
  minimizeToTray: true,
  startMinimized: false,
  theme: "dark",
  defaultDownloadDirectory: null,
  networkIntervalSeconds: 1,
  pingTimeoutMs: 5000,
  networkAlertsEnabled: false,
  networkAlertFailureThreshold: 3,
  networkAlertCooldownSeconds: 300,
};

type IconProps = {
  name: IconName;
  size?: number;
};

const navigation: { label: string; icon: IconName }[] = [
  { label: "Dashboard", icon: "dashboard" },
  { label: "Network Interfaces", icon: "interfaces" },
  { label: "Speed Test", icon: "speed" },
  { label: "BDIX Analysis", icon: "analysis" },
  { label: "Network Quality", icon: "monitor" },
  { label: "Download Manager", icon: "download" },
  { label: "Video Detection", icon: "video" },
  { label: "Settings", icon: "settings" },
];

function Icon({ name, size = 20 }: IconProps) {
  const paths: Record<IconName, ReactNode> = {
    dashboard: (
      <>
        <rect x="3" y="3" width="7" height="7" rx="1.5" />
        <rect x="14" y="3" width="7" height="7" rx="1.5" />
        <rect x="3" y="14" width="7" height="7" rx="1.5" />
        <rect x="14" y="14" width="7" height="7" rx="1.5" />
      </>
    ),
    interfaces: (
      <>
        <rect x="3" y="4" width="18" height="13" rx="2" />
        <path d="M8 21h8M12 17v4M7 9h3M7 13h7M17 9h.01" />
      </>
    ),
    speed: (
      <>
        <path d="M4.93 19a9 9 0 1 1 14.14 0" />
        <path d="m12 13 4-4M7 19h10" />
        <circle cx="12" cy="13" r="1" />
      </>
    ),
    analysis: (
      <>
        <path d="M3 3v18h18M7 14l4-4 4 3 6-7" />
        <path d="M17 6h4v4" />
      </>
    ),
    monitor: (
      <>
        <path d="M3 12h4l2.2-6 4.2 12 2.2-6H21" />
        <path d="M4 20h16" />
      </>
    ),
    settings: (
      <>
        <path d="M12 8.5a3.5 3.5 0 1 0 0 7 3.5 3.5 0 0 0 0-7Z" />
        <path d="m19.4 15 .1.1a1.8 1.8 0 1 1-2.5 2.5l-.1-.1a1.8 1.8 0 0 0-3 .9v.2a1.8 1.8 0 1 1-3.6 0v-.2a1.8 1.8 0 0 0-3-.9l-.1.1a1.8 1.8 0 1 1-2.5-2.5l.1-.1a1.8 1.8 0 0 0-.9-3H3.7a1.8 1.8 0 1 1 0-3.6h.2a1.8 1.8 0 0 0 .9-3l-.1-.1a1.8 1.8 0 1 1 2.5-2.5l.1.1a1.8 1.8 0 0 0 3-.9v-.2a1.8 1.8 0 1 1 3.6 0v.2a1.8 1.8 0 0 0 3 .9l.1-.1a1.8 1.8 0 1 1 2.5 2.5l-.1.1a1.8 1.8 0 0 0 .9 3h.2a1.8 1.8 0 1 1 0 3.6h-.2a1.8 1.8 0 0 0-.9 3Z" />
      </>
    ),
    globe: (
      <>
        <circle cx="12" cy="12" r="9" />
        <path d="M3 12h18M12 3a15 15 0 0 1 0 18M12 3a15 15 0 0 0 0 18" />
      </>
    ),
    download: (
      <>
        <path d="M12 3v12m0 0 4-4m-4 4-4-4" />
        <path d="M5 17v3h14v-3" />
      </>
    ),
    video: (
      <>
        <rect x="3" y="5" width="13" height="14" rx="2" />
        <path d="m16 10 5-3v10l-5-3" />
      </>
    ),
    upload: (
      <>
        <path d="M12 15V3m0 0L8 7m4-4 4 4" />
        <path d="M5 17v3h14v-3" />
      </>
    ),
    latency: (
      <>
        <path d="M3 12h4l3-8 4 16 3-8h4" />
      </>
    ),
    packet: (
      <>
        <path d="M12 22s8-4 8-11V5l-8-3-8 3v6c0 7 8 11 8 11Z" />
        <path d="M9 12h6" />
      </>
    ),
    arrow: <path d="M5 12h14m-6-6 6 6-6 6" />,
    signal: (
      <>
        <path d="M3 20h18M6 16v-3M10 16V9M14 16V5M18 16V2" />
      </>
    ),
    chevron: <path d="m9 18 6-6-6-6" />,
    refresh: (
      <>
        <path d="M20 7v5h-5M4 17v-5h5" />
        <path d="M5.5 9a7 7 0 0 1 11.6-2L20 12M4 12l2.9 5a7 7 0 0 0 11.6-2" />
      </>
    ),
  };

  return (
    <svg
      aria-hidden="true"
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.7"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      {paths[name]}
    </svg>
  );
}

function BrandMark() {
  return <img className="brand-mark" src={fluxnetSymbol} alt="" aria-hidden="true" />;
}

function Sidebar({
  activePage,
  onNavigate,
  interfaceSummary,
  navigationDisabled,
}: {
  activePage: string;
  onNavigate: (page: string) => void;
  interfaceSummary: string;
  navigationDisabled: boolean;
}) {
  return (
    <aside className="sidebar">
      <a
        className="brand"
        href="#"
        aria-disabled={navigationDisabled}
        onClick={(event) => {
          event.preventDefault();
          if (!navigationDisabled) onNavigate("Dashboard");
        }}
      >
        <BrandMark />
        <span className="brand-name">flux<span>net</span></span>
        <span className="brand-version">BETA</span>
      </a>

      <div className="sidebar-label">WORKSPACE</div>
      <nav className="navigation" aria-label="Main navigation">
        {navigation.map((item) => (
          <button
            className={`nav-item${activePage === item.label ? " active" : ""}`}
            key={item.label}
            onClick={() => onNavigate(item.label)}
            disabled={navigationDisabled}
            type="button"
          >
            <Icon name={item.icon} size={19} />
            <span>{item.label}</span>
            {activePage === item.label && <span className="active-indicator" />}
          </button>
        ))}
      </nav>

      <div className="sidebar-bottom">
        <div className="sidebar-status">
          <span className="status-orb" />
          <div>
            <strong>Network interfaces</strong>
            <span>{interfaceSummary}</span>
          </div>
        </div>
        <div className="sidebar-footer">
          <span>FLUXNET DESKTOP</span>
          <span>v0.1.0</span>
          <span className="sidebar-credit">Developed by Imran</span>
        </div>
      </div>
    </aside>
  );
}

function MetricCard({
  title,
  icon,
  detail,
  value,
}: {
  title: string;
  icon: IconName;
  detail: string;
  value: number | null;
}) {
  const unit = title === "Latency" ? "ms" : title === "Packet loss" ? "%" : "Mbps";
  return (
    <article className="metric-card">
      <div className="metric-top">
        <span className="metric-icon"><Icon name={icon} size={18} /></span>
        <span className="metric-title">{title}</span>
        <span className={value === null ? "unavailable-dot" : "available-dot"} title={value === null ? "Waiting for telemetry" : "Live Windows telemetry"} />
      </div>
      <div className="metric-value">{value === null ? "—" : value.toFixed(2)}<span className="metric-unit"> {unit}</span></div>
      <div className="metric-bottom">
        <span>{detail}</span>
        <span className={value === null ? "metric-unavailable" : "metric-available"}>{value === null ? "Waiting" : "Live"}</span>
      </div>
    </article>
  );
}

function InterfacePanel({
  interfaces,
  isLoading,
  error,
  onRefresh,
}: {
  interfaces: NetworkInterface[] | null;
  isLoading: boolean;
  error: string | null;
  onRefresh: () => void;
}) {
  return (
    <section className="panel interface-panel">
      <div className="panel-heading">
        <div>
          <div className="eyebrow">YOUR CONNECTION</div>
          <h2>Network interfaces</h2>
        </div>
        <button
          className="refresh-button"
          type="button"
          onClick={onRefresh}
          disabled={isLoading}
          aria-label="Refresh network interfaces"
        >
          <Icon name="refresh" size={14} />
          {isLoading ? "Scanning" : "Refresh"}
        </button>
      </div>
      {error && (
        <div className="interface-error" role="alert">
          <strong>Unable to read network interfaces</strong>
          <span>{error}</span>
        </div>
      )}
      {isLoading && interfaces === null && (
        <div className="interface-message">Reading Windows network adapters…</div>
      )}
      {!isLoading && !error && interfaces?.length === 0 && (
        <div className="interface-message">
          <strong>No network interfaces found</strong>
          <span>Windows did not return any network adapters for this device.</span>
        </div>
      )}
      {interfaces && interfaces.length > 0 && (
        <div className="interface-list">
          {interfaces.map((networkInterface) => (
            <article className="network-interface" key={networkInterface.name}>
              <div className="interface-name">
                <span className="empty-icon"><Icon name="interfaces" size={20} /></span>
                <div>
                  <strong>{networkInterface.name}</strong>
                  <span>{networkInterface.adapterType ?? "Adapter type unavailable"}</span>
                </div>
                <span
                  className={`interface-status${networkInterface.operationalStatus === "Up" ? " is-up" : ""}`}
                >
                  <i />
                  {networkInterface.operationalStatus}
                </span>
              </div>
              <div className="interface-details">
                <InterfaceDetail
                  label="Local IPv4"
                  value={networkInterface.ipv4Address}
                  isLinkLocal={networkInterface.ipv4IsLinkLocal}
                />
                <InterfaceDetail label="Default gateway" value={networkInterface.defaultGateway} />
              </div>
            </article>
          ))}
        </div>
      )}
    </section>
  );
}

function InterfaceDetail({
  label,
  value,
  isLinkLocal = false,
}: {
  label: string;
  value: string | null;
  isLinkLocal?: boolean;
}) {
  return (
    <div className="interface-detail">
      <span>{label}</span>
      {value ? (
        <>
          <strong>{value}</strong>
          {isLinkLocal && (
            <small className="link-local-warning">
              Link-local address. DHCP may not have assigned an address; internet access may be unavailable.
            </small>
          )}
        </>
      ) : <em>Unavailable</em>}
    </div>
  );
}

function DiagnosticPanel({
  hostname,
  onHostnameChange,
  diagnostic,
  isLoading,
  error,
  onTest,
}: {
  hostname: string;
  onHostnameChange: (hostname: string) => void;
  diagnostic: ConnectivityDiagnostic | null;
  isLoading: boolean;
  error: string | null;
  onTest: () => void;
}) {
  const statusLabel = diagnostic?.status === "success"
    ? "Reachable"
    : diagnostic?.status === "timeout"
      ? "No echo replies"
      : diagnostic?.status === "dnsResolutionFailed"
        ? "DNS resolution failed"
        : diagnostic?.status === "noIpv4Address"
          ? "No IPv4 address"
          : diagnostic?.status === "unreachable"
            ? "Destination unreachable"
            : null;

  return (
    <section className="panel diagnostic-panel" aria-labelledby="diagnostic-heading">
      <div className="panel-heading">
        <div>
          <div className="eyebrow">ACTIVE REACHABILITY TEST</div>
          <h2 id="diagnostic-heading">Internet connectivity</h2>
        </div>
        <span className="diagnostic-probe-note">4 ICMP echo attempts · IPv4</span>
      </div>
      <p className="diagnostic-description">
        Sends real ICMP echo requests and measures replies. Interface status alone does not confirm internet access.
      </p>
      <form
        className="diagnostic-form"
        onSubmit={(event) => {
          event.preventDefault();
          onTest();
        }}
      >
        <label className="diagnostic-input-label" htmlFor="diagnostic-host">
          Hostname or IPv4 address
        </label>
        <div className="diagnostic-controls">
          <input
            id="diagnostic-host"
            autoComplete="url"
            maxLength={253}
            onChange={(event) => onHostnameChange(event.currentTarget.value)}
            placeholder="1.1.1.1 or example.com"
            value={hostname}
          />
          <button className="test-connection-button" type="submit" disabled={isLoading || !hostname.trim()}>
            <Icon name="latency" size={16} />
            {isLoading ? "Testing…" : "Test Connection"}
          </button>
        </div>
      </form>
      {isLoading && (
        <div className="diagnostic-message is-pending" role="status">
          Resolving the host and sending ICMP probes. This can take a few seconds.
        </div>
      )}
      {!isLoading && error && (
        <div className="diagnostic-message is-error" role="alert">
          <strong>Diagnostic could not run</strong>
          <span>{error}</span>
        </div>
      )}
      {!isLoading && diagnostic && (
        <div className={`diagnostic-result is-${diagnostic.status}`}>
          <div className="diagnostic-result-header">
            <div>
              <span className="diagnostic-result-label">RESULT FOR {diagnostic.hostname}</span>
              <strong>{statusLabel ?? "Diagnostic unavailable"}</strong>
            </div>
            {diagnostic.resolvedIpv4 && (
              <span className="resolved-address">IPv4 · {diagnostic.resolvedIpv4}</span>
            )}
          </div>
          {diagnostic.message && <p className="diagnostic-result-message">{diagnostic.message}</p>}
          {diagnostic.attempts > 0 && (
            <>
              <div className="diagnostic-stats">
                <DiagnosticStat
                  label="Packet loss"
                  value={diagnostic.packetLossPercent === null
                    ? "Unavailable"
                    : `${diagnostic.packetLossPercent.toFixed(0)}%`}
                />
                <DiagnosticStat
                  label="Average latency"
                  value={diagnostic.averageLatencyMs === null
                    ? "—"
                    : `${diagnostic.averageLatencyMs.toFixed(1)} ms`}
                />
                <DiagnosticStat
                  label="Successful probes"
                  value={`${diagnostic.successfulAttempts} / ${diagnostic.attempts}`}
                />
              </div>
              <div className="probe-results" aria-label="Probe results">
                {diagnostic.probes.map((probe) => (
                  <span
                    className={`probe-result is-${probe.status}`}
                    key={probe.attempt}
                    title={probe.detail ?? `Attempt ${probe.attempt}: ${probe.status}`}
                  >
                    {probe.status === "success" && probe.latencyMs !== null
                      ? `${probe.latencyMs.toFixed(1)} ms`
                      : probe.status === "timeout" ? "Timeout" : "Failed"}
                  </span>
                ))}
              </div>
            </>
          )}
        </div>
      )}
    </section>
  );
}

function DiagnosticStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="diagnostic-stat">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function QuickActions({ onNavigate }: { onNavigate: (page: string) => void }) {
  return (
    <section className="panel quick-panel">
      <div className="eyebrow">TOOLS</div>
      <h2>Quick access</h2>
      <button className="quick-action" onClick={() => onNavigate("Speed Test")} type="button">
        <span className="quick-action-icon"><Icon name="speed" size={18} /></span>
        <span><strong>Run a speed test</strong><small>Measure your connection performance</small></span>
        <Icon name="chevron" size={17} />
      </button>
      <button className="quick-action" onClick={() => onNavigate("BDIX Analysis")} type="button">
        <span className="quick-action-icon"><Icon name="analysis" size={18} /></span>
        <span><strong>Analyze server reachability</strong><small>ICMP does not verify BDIX status</small></span>
        <Icon name="chevron" size={17} />
      </button>
    </section>
  );
}

function Dashboard({
  onNavigate,
  interfaces,
  isLoading,
  error,
  onRefresh,
  diagnosticHostname,
  onDiagnosticHostnameChange,
  diagnostic,
  isDiagnosticLoading,
  diagnosticError,
  onTestConnection,
  telemetry,
}: {
  onNavigate: (page: string) => void;
  interfaces: NetworkInterface[] | null;
  isLoading: boolean;
  error: string | null;
  onRefresh: () => void;
  diagnosticHostname: string;
  onDiagnosticHostnameChange: (hostname: string) => void;
  diagnostic: ConnectivityDiagnostic | null;
  isDiagnosticLoading: boolean;
  diagnosticError: string | null;
  onTestConnection: () => void;
  telemetry: QualityMonitorUpdate | null;
}) {
  const operationalCount = interfaces?.filter(
    (networkInterface) => networkInterface.operationalStatus === "Up",
  ).length ?? 0;
  const connectionHeading = isLoading
    ? "Reading network interfaces…"
    : error
      ? "Interface detection unavailable"
      : interfaces?.length
        ? `${interfaces.length} network interface${interfaces.length === 1 ? "" : "s"} detected`
        : "No network interfaces found";
  const connectionDescription = isLoading
    ? "Querying Windows for available network adapters."
    : error
      ? "Windows adapters could not be read. Interface state does not indicate internet reachability."
      : interfaces?.length
        ? `${operationalCount} operational adapter${operationalCount === 1 ? "" : "s"}. Adapter state does not confirm internet access.`
        : "Windows did not return any network adapters for this device.";
  const connectionState = isLoading
    ? "SCANNING"
    : error
      ? "SCAN FAILED"
      : interfaces?.length
        ? "DETECTED"
        : "NO INTERFACES";

  return (
    <>
      <header className="page-header">
        <div>
          <div className="eyebrow">OVERVIEW</div>
          <h1>Network dashboard</h1>
          <p className="page-subtitle">A clear view of your network, all in one place.</p>
        </div>
        <div className="header-meta">
          <div className="live-indicator"><span /> INTERFACE SCAN</div>
          <div className="updated-label">
            {isLoading ? "Scanning Windows interfaces…" : error ? "Last scan failed" : "Read-only detection"}
          </div>
        </div>
      </header>

      <section className="connection-banner" aria-label="Network adapter status">
        <div className="connection-symbol"><Icon name="interfaces" size={23} /></div>
        <div className="connection-copy">
          <span className="connection-label">ADAPTER INVENTORY</span>
          <strong>{connectionHeading}</strong>
          <span>{connectionDescription}</span>
        </div>
        <div className={`connection-state${!isLoading && !error && interfaces?.length ? " is-detected" : ""}`}>
          <span /> {connectionState}
        </div>
        <div className="banner-decoration" aria-hidden="true">
          <span /><span /><span /><span /><span /><span /><span />
        </div>
      </section>

      <DiagnosticPanel
        hostname={diagnosticHostname}
        onHostnameChange={onDiagnosticHostnameChange}
        diagnostic={diagnostic}
        isLoading={isDiagnosticLoading}
        error={diagnosticError}
        onTest={onTestConnection}
      />

      <div className="section-title-row">
        <div>
          <div className="eyebrow">PERFORMANCE</div>
          <h2>Connection metrics</h2>
        </div>
        <span className="data-note"><span /> Shared Windows network telemetry</span>
      </div>

      <div className="metrics-grid">
        <MetricCard title="Latency" icon="latency" detail="ICMP round-trip time" value={telemetry?.probe.status === "success" ? telemetry.probe.latencyMs : null} />
        <MetricCard title="Packet loss" icon="packet" detail="Rolling ICMP packet loss" value={telemetry?.rollingStats.packetLossPercent ?? null} />
        <MetricCard title="Download" icon="download" detail="Physical Ethernet and Wi-Fi" value={telemetry?.downloadMbps ?? null} />
        <MetricCard title="Upload" icon="upload" detail="Physical Ethernet and Wi-Fi" value={telemetry?.uploadMbps ?? null} />
      </div>

      <div className="content-grid">
        <InterfacePanel
          interfaces={interfaces}
          isLoading={isLoading}
          error={error}
          onRefresh={onRefresh}
        />
        <QuickActions onNavigate={onNavigate} />
      </div>
      <footer className="dashboard-footer">
        <span><Icon name="signal" size={14} /> Interface state does not confirm internet reachability</span>
        <span>FluxNet dashboard <i /> Desktop utility</span>
      </footer>
    </>
  );
}

function ComingSoon({ page }: { page: string }) {
  const active = navigation.find((item) => item.label === page);
  return (
    <section className="coming-soon">
      <div className="coming-icon"><Icon name={active?.icon ?? "dashboard"} size={25} /></div>
      <div className="eyebrow">FLUXNET WORKSPACE</div>
      <h1>{page}</h1>
      <p>This feature is not available in this phase. Network telemetry and tools will be connected in a future update.</p>
      <div className="coming-badge"><span /> NOT YET AVAILABLE</div>
    </section>
  );
}

function SettingsPage({
  settings,
  onSettingsChange,
  settingsLoaded,
  loadError,
  onNavigate,
}: {
  settings: AppSettings;
  onSettingsChange: (settings: AppSettings) => void;
  settingsLoaded: boolean;
  loadError: string | null;
  onNavigate: (page: string) => void;
}) {
  const [downloadSettings, setDownloadSettings] = useState<DownloadSettings | null>(null);
  const [browserStatus, setBrowserStatus] = useState<BrowserBridgeStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [feedback, setFeedback] = useState<{ kind: "success" | "error"; message: string } | null>(null);

  const refreshBrowserStatus = useCallback(async () => {
    try {
      setBrowserStatus(await invoke<BrowserBridgeStatus>("get_browser_bridge_status"));
      setFeedback({ kind: "success", message: "Browser host and handshake status refreshed." });
    } catch (error) {
      setFeedback({
        kind: "error",
        message: `Could not read browser connection status: ${error instanceof Error ? error.message : String(error)}`,
      });
    }
  }, []);

  useEffect(() => {
    let disposed = false;
    let unlisten: (() => void) | undefined;
    void listen<BrowserBridgeStatus>("browser-bridge-status", (event) => {
      setBrowserStatus(event.payload);
    }).then((stop) => {
      if (disposed) stop();
      else unlisten = stop;
    }).catch((error: unknown) => {
      if (!disposed) {
        setFeedback({
          kind: "error",
          message: `Could not listen for browser connection changes: ${error instanceof Error ? error.message : String(error)}`,
        });
      }
    });
    void Promise.all([
      invoke<DownloadSettings>("get_download_settings"),
      invoke<BrowserBridgeStatus>("get_browser_bridge_status"),
    ]).then(([downloads, browser]) => {
      if (disposed) return;
      setDownloadSettings(downloads);
      setBrowserStatus(browser);
    }).catch((error: unknown) => {
      if (!disposed) {
        setFeedback({
          kind: "error",
          message: `Could not load Settings: ${error instanceof Error ? error.message : String(error)}`,
        });
      }
    }).finally(() => {
      if (!disposed) setLoading(false);
    });
    return () => {
      disposed = true;
      unlisten?.();
    };
  }, []);

  const updateDownloadSettings = (patch: Partial<DownloadSettings>) => {
    setDownloadSettings((current) => current ? { ...current, ...patch } : current);
  };

  const selectDownloadFolder = async () => {
    try {
      const selected = await openDialog({
        directory: true,
        multiple: false,
        title: "Choose default download folder",
      });
      if (typeof selected === "string") {
        onSettingsChange({ ...settings, defaultDownloadDirectory: selected });
      }
    } catch (error) {
      setFeedback({
        kind: "error",
        message: `Could not open the folder picker: ${error instanceof Error ? error.message : String(error)}`,
      });
    }
  };

  const applySettings = useCallback(async (
    nextSettings: AppSettings,
    nextDownloads: DownloadSettings,
  ) => {
    setSaving(true);
    setFeedback(null);
    try {
      await invoke("validate_app_settings", { value: nextSettings });
      await invoke("configure_download_limits", {
        maxConcurrent: nextDownloads.maxConcurrent,
        bandwidthLimitMbps: nextDownloads.bandwidthLimitMbps,
      });
      await invoke("configure_download_notification_preferences", {
        notifyOnComplete: nextDownloads.notifyOnComplete,
        notifyOnFailure: nextDownloads.notifyOnFailure,
      });
      await invoke("configure_quality_monitor_preferences", {
        intervalSeconds: nextSettings.networkIntervalSeconds,
        pingTimeoutMs: nextSettings.pingTimeoutMs,
      });
      await invoke("configure_quality_alerts", {
        enabled: nextSettings.networkAlertsEnabled,
        failureThreshold: nextSettings.networkAlertFailureThreshold,
        cooldownSeconds: nextSettings.networkAlertCooldownSeconds,
      });
      await invoke("save_app_settings", { value: nextSettings });
      onSettingsChange(nextSettings);
      setDownloadSettings(nextDownloads);
      setFeedback({ kind: "success", message: "Settings saved and applied." });
    } catch (error) {
      setFeedback({
        kind: "error",
        message: `Could not apply all Settings: ${error instanceof Error ? error.message : String(error)}. Review the values and retry; earlier sections may already have been applied.`,
      });
    } finally {
      setSaving(false);
    }
  }, [onSettingsChange]);

  const resetToDefaults = () => {
    if (!window.confirm("Reset all supported FluxNet preferences to their defaults? This changes startup behavior, download limits and notifications, network monitoring preferences, and the download folder. Download history and other user data will be kept.")) return;
    const defaults = { ...DEFAULT_APP_SETTINGS };
    const downloadDefaults: DownloadSettings = {
      maxConcurrent: 2,
      bandwidthLimitMbps: 0,
      notifyOnComplete: false,
      notifyOnFailure: false,
    };
    void applySettings(defaults, downloadDefaults);
  };

  return (
    <div className="page settings-page">
      <header className="page-header">
        <div>
          <div className="eyebrow">FLUXNET PREFERENCES</div>
          <h1>Settings</h1>
          <p className="page-subtitle">Configure desktop behavior, downloads, browser integration, and network alerts.</p>
        </div>
        <button
          className="test-connection-button"
          type="button"
          disabled={loading || saving || !settingsLoaded || Boolean(loadError) || !downloadSettings}
          onClick={() => downloadSettings && void applySettings(settings, downloadSettings)}
        >
          {saving ? "Applying…" : "Save & Apply"}
        </button>
      </header>

      {loadError && <div className="interface-error" role="alert">Could not load persisted Settings: {loadError}</div>}
      <section className="panel settings-section">
        <div className="panel-heading"><div><div className="eyebrow">DESKTOP</div><h2>General</h2></div></div>
        <div className="settings-grid">
          <label className="settings-choice">
            <span><strong>Launch FluxNet on Windows startup</strong><small>Registers FluxNet for the current Windows user.</small></span>
            <input type="checkbox" checked={settings.launchOnStartup} onChange={(event) => onSettingsChange({ ...settings, launchOnStartup: event.currentTarget.checked })} />
          </label>
          <label className="settings-choice">
            <span><strong>Minimize to system tray on close</strong><small>Keep FluxNet running in the notification area when the window is closed.</small></span>
            <input type="checkbox" checked={settings.minimizeToTray} onChange={(event) => onSettingsChange({ ...settings, minimizeToTray: event.currentTarget.checked })} />
          </label>
          <label className="settings-choice">
            <span><strong>Start minimized</strong><small>Applies on the next launch; FluxNet remains available from the tray.</small></span>
            <input type="checkbox" checked={settings.startMinimized} onChange={(event) => onSettingsChange({ ...settings, startMinimized: event.currentTarget.checked })} />
          </label>
          <label className="settings-field">
            <span>Theme</span>
            <select value={settings.theme} onChange={(event) => onSettingsChange({ ...settings, theme: event.currentTarget.value as AppSettings["theme"] })}>
              <option value="dark">Dark</option><option value="light">Light</option><option value="system">System</option>
            </select>
          </label>
        </div>
      </section>

      <section className="panel settings-section">
        <div className="panel-heading"><div><div className="eyebrow">FILE TRANSFERS</div><h2>Downloads</h2></div></div>
        {downloadSettings && (
          <div className="settings-grid">
            <div className="settings-field settings-folder-field">
              <span>Default download directory</span>
              <div className="settings-folder-row">
                <input readOnly value={settings.defaultDownloadDirectory ?? "Ask for a destination each time"} aria-label="Default download directory" />
                <button className="secondary-button" type="button" onClick={() => void selectDownloadFolder()}>Choose folder</button>
                {settings.defaultDownloadDirectory && <button className="secondary-button" type="button" onClick={() => onSettingsChange({ ...settings, defaultDownloadDirectory: null })}>Clear</button>}
              </div>
            </div>
            <label className="settings-field">
              <span>Maximum simultaneous downloads</span>
              <select value={downloadSettings.maxConcurrent} onChange={(event) => updateDownloadSettings({ maxConcurrent: Number(event.currentTarget.value) })}>
                {[1, 2, 3, 4].map((count) => <option key={count} value={count}>{count}</option>)}
              </select>
            </label>
            <label className="settings-field">
              <span>Download speed limit</span>
              <select value={downloadSettings.bandwidthLimitMbps} onChange={(event) => updateDownloadSettings({ bandwidthLimitMbps: Number(event.currentTarget.value) })}>
                {[0, 0.25, 0.5, 1, 2, 5, 10].map((limit) => <option key={limit} value={limit}>{limit === 0 ? "Unlimited" : `${limit} Mbps`}</option>)}
              </select>
            </label>
            <label className="settings-choice">
              <span><strong>Download completed notifications</strong><small>Show a Windows notification after a download completes.</small></span>
              <input type="checkbox" checked={downloadSettings.notifyOnComplete} onChange={(event) => updateDownloadSettings({ notifyOnComplete: event.currentTarget.checked })} />
            </label>
            <label className="settings-choice">
              <span><strong>Download failed notifications</strong><small>Show a Windows notification when a download fails.</small></span>
              <input type="checkbox" checked={downloadSettings.notifyOnFailure} onChange={(event) => updateDownloadSettings({ notifyOnFailure: event.currentTarget.checked })} />
            </label>
          </div>
        )}
      </section>

      <section className="panel settings-section">
        <div className="panel-heading"><div><div className="eyebrow">CHROME / EDGE</div><h2>Browser integration</h2></div></div>
        <div className="settings-status-list">
          <div><span>Native Messaging host</span><strong className={browserStatus?.nativeHostStatus === "installed" ? "is-connected" : "is-disconnected"}>
            {browserStatus?.nativeHostStatus === "installed" ? `Installed for ${browserStatus.registeredBrowsers.join(" and ")}` : browserStatus?.nativeHostStatus === "unsupported" ? "Unsupported on this platform" : browserStatus?.nativeHostStatus === "notInstalled" ? "Not installed or invalid" : "Checking…"}
          </strong></div>
          <div><span>Extension connection</span><strong className={browserStatus?.connectionStatus === "connected" ? "is-connected" : "is-disconnected"}>
            {browserStatus?.connectionStatus === "connected"
              ? `Connected · authenticated ${browserStatus.lastHandshakeAtMs ? `${Math.max(0, Math.floor((Date.now() - browserStatus.lastHandshakeAtMs) / 1000))}s ago` : "recently"}`
              : browserStatus?.connectionStatus === "disconnected"
                ? "Disconnected · handshake failed or expired"
                : "Awaiting extension connection"}
          </strong></div>
        </div>
        <div className="settings-browser-actions">
          <button className="secondary-button" type="button" onClick={() => void refreshBrowserStatus()}>Refresh connection status</button>
          <button className="secondary-button" type="button" onClick={() => onNavigate("Video Detection")}>Troubleshoot / setup</button>
        </div>
        <p className="settings-note">The extension connects automatically and retries when FluxNet becomes available; no pairing code is required. Refresh rereads the Windows host registration and recent authenticated extension handshake. “Connected” means a valid Native Messaging request was received within the last 150 seconds. Native Messaging itself is not a persistent connection. No cookies or authorization data are shared.</p>
      </section>

      <section className="panel settings-section">
        <div className="panel-heading"><div><div className="eyebrow">WINDOWS ICMP</div><h2>Network monitoring</h2></div></div>
        <div className="settings-grid">
          <label className="settings-field">
            <span>Monitoring interval</span>
            <select value={settings.networkIntervalSeconds} onChange={(event) => onSettingsChange({ ...settings, networkIntervalSeconds: Number(event.currentTarget.value) })}>
              {[1, 2, 5].map((value) => <option key={value} value={value}>{value} sec</option>)}
            </select>
          </label>
          <label className="settings-field">
            <span>Ping timeout (milliseconds)</span>
            <input type="number" min={500} max={5000} step={500} value={settings.pingTimeoutMs} onChange={(event) => onSettingsChange({ ...settings, pingTimeoutMs: Number(event.currentTarget.value) })} />
          </label>
          <label className="settings-choice">
            <span><strong>Network alert notifications</strong><small>Alert after consecutive failed probes; packet loss remains part of quality statistics.</small></span>
            <input type="checkbox" checked={settings.networkAlertsEnabled} onChange={(event) => onSettingsChange({ ...settings, networkAlertsEnabled: event.currentTarget.checked })} />
          </label>
          <label className="settings-field">
            <span>Failed probes before alert</span>
            <select value={settings.networkAlertFailureThreshold} onChange={(event) => onSettingsChange({ ...settings, networkAlertFailureThreshold: Number(event.currentTarget.value) })}>
              {[2, 3, 5, 10].map((value) => <option key={value} value={value}>{value}</option>)}
            </select>
          </label>
          <label className="settings-field">
            <span>Network alert cooldown</span>
            <select value={settings.networkAlertCooldownSeconds} onChange={(event) => onSettingsChange({ ...settings, networkAlertCooldownSeconds: Number(event.currentTarget.value) })}>
              {[60, 300, 900].map((value) => <option key={value} value={value}>{value / 60} min</option>)}
            </select>
          </label>
        </div>
        <p className="settings-note">Monitoring interval and ping timeout are applied to Network Quality probes. Packet loss is always measured from probe outcomes and cannot be disabled separately.</p>
      </section>

      <section className="panel settings-section">
        <div className="panel-heading"><div><div className="eyebrow">CHROMIUM EXTENSION</div><h2>Video detection & notifications</h2></div></div>
        <p className="settings-note">Automatic video detection, browser download prompts, player overlays, file-type filters, domain exclusions, and extension notification behavior are stored by Chrome/Edge in the extension. They cannot be changed from the desktop app. Open Video Detection to see connection status and setup guidance, then manage these options in the extension popup.</p>
        <button className="secondary-button" type="button" onClick={() => onNavigate("Video Detection")}>Open Video Detection</button>
      </section>

      <section className="panel settings-section">
        <div className="panel-heading"><div><div className="eyebrow">ALERTS</div><h2>Notifications</h2></div></div>
        <div className="settings-grid">
          <label className="settings-choice">
            <span><strong>Download completed</strong><small>Managed above with the download preferences.</small></span>
            <input type="checkbox" checked={downloadSettings?.notifyOnComplete ?? false} onChange={(event) => updateDownloadSettings({ notifyOnComplete: event.currentTarget.checked })} />
          </label>
          <label className="settings-choice">
            <span><strong>Download failed</strong><small>Managed above with the download preferences.</small></span>
            <input type="checkbox" checked={downloadSettings?.notifyOnFailure ?? false} onChange={(event) => updateDownloadSettings({ notifyOnFailure: event.currentTarget.checked })} />
          </label>
          <div className="settings-choice settings-readonly">
            <span><strong>Network alerts</strong><small>Uses the Network Quality failure threshold and cooldown above.</small></span>
            <span>{settings.networkAlertsEnabled ? "Enabled" : "Disabled"}</span>
          </div>
        </div>
      </section>

      {feedback && <div className={`download-feedback is-${feedback.kind}`} role={feedback.kind === "error" ? "alert" : "status"}>{feedback.message}</div>}
      <footer className="settings-footer">
        <button className="secondary-button" type="button" onClick={resetToDefaults} disabled={saving || loading || !settingsLoaded || Boolean(loadError) || !downloadSettings}>Reset to Defaults</button>
        <span>Settings are stored in FluxNet’s per-user configuration and download history files.</span>
      </footer>
    </div>
  );
}

function parseNetworkCounter(value: string | null): bigint | null {
  if (value === null) return null;
  try {
    return BigInt(value);
  } catch {
    return null;
  }
}

function formatNetworkBytes(value: bigint | null): string {
  if (value === null) return "Unavailable";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  let divisor = 1n;
  let unitIndex = 0;
  while (value >= divisor * 1024n && unitIndex < units.length - 1) {
    divisor *= 1024n;
    unitIndex += 1;
  }
  const whole = (value / divisor).toLocaleString();
  const fraction = unitIndex === 0 ? 0n : (value % divisor) * 100n / divisor;
  const decimal = fraction === 0n ? "" : `.${fraction.toString().padStart(2, "0").replace(/0+$/, "")}`;
  return `${whole}${decimal} ${units[unitIndex]}`;
}

function NetworkInterfacesPage({
  interfaces,
  isLoading,
  error,
  onRefresh,
}: {
  interfaces: NetworkInterface[] | null;
  isLoading: boolean;
  error: string | null;
  onRefresh: () => void;
}) {
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const adapterKey = (networkInterface: NetworkInterface) =>
    `${networkInterface.name}\u0000${networkInterface.adapterType ?? ""}`;

  const activeAdapter = interfaces?.find((networkInterface) => (
    adapterKey(networkInterface) === selectedKey
  )) ?? interfaces?.find((networkInterface) => networkInterface.operationalStatus === "Up")
    ?? interfaces?.[0] ?? null;
  const activeKey = activeAdapter ? adapterKey(activeAdapter) : null;

  return (
    <section className="network-page">
      <header className="page-header">
        <div>
          <div className="eyebrow">WINDOWS ADAPTER MONITOR</div>
          <h1>Network Interfaces</h1>
          <p className="page-subtitle">Live adapter details and system-reported traffic counters.</p>
        </div>
        <div className="network-page-actions">
          <span className="network-live"><i /> SHARED LIVE MONITOR</span>
          <button
            className="refresh-button"
            type="button"
            onClick={onRefresh}
            disabled={isLoading}
            aria-label="Refresh network interfaces"
          >
            <Icon name="refresh" size={14} />
            {isLoading ? "Refreshing" : "Refresh"}
          </button>
        </div>
      </header>

      {error && (
        <div className="interface-error" role="alert">
          <strong>Unable to read network interfaces</strong>
          <span>{error}</span>
        </div>
      )}
      {isLoading && interfaces === null && (
        <div className="interface-message">Reading Windows network adapters…</div>
      )}
      {interfaces?.length === 0 && !error && (
        <div className="interface-message">
          <strong>No network interfaces found</strong>
          <span>Windows did not return any network adapters for this device.</span>
        </div>
      )}
      {activeAdapter && (
        <div className="network-page-layout">
          <div className="network-adapter-list panel">
            <div className="panel-heading">
              <div>
                <div className="eyebrow">DETECTED HARDWARE</div>
                <h2>Adapters <span className="network-count">{interfaces?.length ?? 0}</span></h2>
              </div>
            </div>
            <div className="network-adapter-items">
              {interfaces?.map((networkInterface) => {
                const key = adapterKey(networkInterface);
                const isSelected = key === activeKey;
                const isUp = networkInterface.operationalStatus === "Up";
                return (
                  <button
                    className={`network-adapter-item${isSelected ? " is-selected" : ""}`}
                    key={key}
                    type="button"
                    onClick={() => setSelectedKey(key)}
                    aria-pressed={isSelected}
                  >
                    <span className={`network-adapter-icon is-${networkInterface.category.toLowerCase()}`}>
                      <Icon name="interfaces" size={19} />
                    </span>
                    <span className="network-adapter-label">
                      <strong>{networkInterface.name}</strong>
                      <small>{networkInterface.category}{networkInterface.isVirtual ? " · Virtual" : ""}</small>
                    </span>
                    <span className={`network-status-dot${isUp ? " is-up" : ""}`} title={networkInterface.operationalStatus} />
                  </button>
                );
              })}
            </div>
          </div>

          <article className="network-adapter-detail panel">
            <div className="network-detail-heading">
              <div className="network-detail-title">
                <span className={`network-adapter-icon is-${activeAdapter.category.toLowerCase()}`}>
                  <Icon name="interfaces" size={21} />
                </span>
                <div>
                  <div className="eyebrow">ACTIVE ADAPTER DETAIL</div>
                  <h2>{activeAdapter.name}</h2>
                  <p>{activeAdapter.description ?? activeAdapter.adapterType ?? "Adapter description unavailable"}</p>
                </div>
              </div>
              <span className={`interface-status${activeAdapter.operationalStatus === "Up" ? " is-up" : ""}`}>
                <i />{activeAdapter.operationalStatus}
              </span>
            </div>

            {activeAdapter.dataWarning && (
              <div className="network-warning" role="status">{activeAdapter.dataWarning}</div>
            )}
            {activeAdapter.wifiError && (
              <div className="network-warning" role="status">{activeAdapter.wifiError}</div>
            )}

            <div className="network-throughput">
              <div className="network-throughput-card">
                <span><Icon name="download" size={16} /> DOWNLOAD</span>
                <strong>{activeAdapter.downloadMbps !== null ? activeAdapter.downloadMbps.toFixed(2) : "—"} <small>Mbps</small></strong>
              </div>
              <div className="network-throughput-card is-upload">
                <span><Icon name="upload" size={16} /> UPLOAD</span>
                <strong>{activeAdapter.uploadMbps !== null ? activeAdapter.uploadMbps.toFixed(2) : "—"} <small>Mbps</small></strong>
              </div>
            </div>

            <div className="network-total-traffic">
              <div><span>Total received</span><strong>{formatNetworkBytes(parseNetworkCounter(activeAdapter.bytesReceived))}</strong></div>
              <div><span>Total sent</span><strong>{formatNetworkBytes(parseNetworkCounter(activeAdapter.bytesSent))}</strong></div>
              <div><span>Link speed</span><strong>{activeAdapter.linkSpeedMbps !== null ? `${activeAdapter.linkSpeedMbps.toFixed(0)} Mbps` : "Unavailable"}</strong></div>
            </div>

            {activeAdapter.category === "Wi-Fi" && (
              <div className="network-wifi-details">
                <div className="network-section-title">WI-FI CONNECTION</div>
                <div className="network-facts-grid">
                  <InterfaceDetail label="Network name (SSID)" value={activeAdapter.wifiSsid} />
                  <InterfaceDetail
                    label="Signal quality"
                    value={activeAdapter.wifiSignalQuality !== null
                      ? `${activeAdapter.wifiSignalQuality}%`
                      : null}
                  />
                  <InterfaceDetail
                    label="Frequency"
                    value={activeAdapter.wifiFrequencyMhz !== null
                      ? `${(activeAdapter.wifiFrequencyMhz / 1000).toFixed(3)} GHz`
                      : null}
                  />
                  <InterfaceDetail
                    label="Receive / transmit link"
                    value={activeAdapter.wifiReceiveMbps !== null && activeAdapter.wifiTransmitMbps !== null
                      ? `${activeAdapter.wifiReceiveMbps.toFixed(1)} / ${activeAdapter.wifiTransmitMbps.toFixed(1)} Mbps`
                      : null}
                  />
                </div>
              </div>
            )}

            <div className="network-wifi-details">
              <div className="network-section-title">ADDRESSING & ROUTING</div>
              <div className="network-facts-grid">
                <InterfaceDetail label="MAC address" value={activeAdapter.macAddress} />
                <InterfaceDetail label="IPv4" value={activeAdapter.ipv4Addresses.join(", ") || null} isLinkLocal={activeAdapter.ipv4IsLinkLocal} />
                <InterfaceDetail label="IPv6" value={activeAdapter.ipv6Addresses.join(", ") || null} />
                <InterfaceDetail label="Default gateway(s)" value={activeAdapter.gatewayAddresses.join(", ") || null} />
                <InterfaceDetail label="DNS servers" value={activeAdapter.dnsServers.join(", ") || null} />
                <InterfaceDetail label="Interface type" value={activeAdapter.adapterType} />
              </div>
            </div>
          </article>
        </div>
      )}
      <p className="network-data-note">
        Adapter counters and link details are read from Windows. Wi-Fi frequency and connection rates are shown only when the WLAN API reports them; fields unavailable from the active driver are marked unavailable.
      </p>
    </section>
  );
}

function QualityLatencyChart({ probes }: { probes: QualityProbe[] }) {
  const width = 760;
  const height = 190;
  const padding = 24;
  const successfulLatencies = probes.flatMap((probe) => (
    probe.status === "success" && probe.latencyMs !== null ? [probe.latencyMs] : []
  ));
  const maxLatency = Math.max(10, ...successfulLatencies) * 1.15;
  const x = (index: number) => padding
    + (index / Math.max(1, probes.length - 1)) * (width - padding * 2);
  const y = (latency: number) => height - padding
    - (latency / maxLatency) * (height - padding * 2);
  const paths: string[] = [];
  let segment: string[] = [];

  probes.forEach((probe, index) => {
    if (probe.status !== "success" || probe.latencyMs === null) {
      if (segment.length > 0) paths.push(segment.join(" "));
      segment = [];
      return;
    }
    segment.push(`${segment.length === 0 ? "M" : "L"} ${x(index).toFixed(1)} ${y(probe.latencyMs).toFixed(1)}`);
  });
  if (segment.length > 0) paths.push(segment.join(" "));

  return (
    <div className="quality-chart">
      {probes.length === 0 ? (
        <div className="quality-chart-empty">Latency history will appear after the first real probe.</div>
      ) : (
        <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Latency history chart in milliseconds">
          {[0, 1, 2, 3].map((step) => {
            const gridY = padding + (step / 3) * (height - padding * 2);
            const labelValue = maxLatency * (1 - step / 3);
            return (
              <g key={step}>
                <line x1={padding} y1={gridY} x2={width - padding} y2={gridY} />
                <text x={padding - 5} y={gridY - 4} textAnchor="end">{labelValue.toFixed(0)}</text>
              </g>
            );
          })}
          {paths.map((path, index) => <path className="quality-chart-line" d={path} key={index} />)}
          {probes.map((probe, index) => probe.status === "success" && probe.latencyMs !== null ? (
            <circle
              className="quality-chart-point"
              cx={x(index)}
              cy={y(probe.latencyMs)}
              r="2.8"
              key={`${probe.timestampMs}-${index}`}
            >
              <title>{`${new Date(probe.timestampMs).toLocaleTimeString()} · ${probe.latencyMs.toFixed(1)} ms`}</title>
            </circle>
          ) : (
            <circle
              className="quality-chart-failure"
              cx={x(index)}
              cy={height - padding}
              r="2.8"
              key={`${probe.timestampMs}-${index}`}
            >
              <title>{`${new Date(probe.timestampMs).toLocaleTimeString()} · ${probe.status}`}</title>
            </circle>
          ))}
          <text x={padding} y={height - 4}>Oldest</text>
          <text x={width - padding} y={height - 4} textAnchor="end">Latest</text>
        </svg>
      )}
      <div className="quality-chart-axis">Latency (ms) · gaps indicate failed probes</div>
    </div>
  );
}

function NetworkQualityPage({
  onAppSettingsChange,
}: {
  onAppSettingsChange: (settings: AppSettings) => void;
}) {
  const [target, setTarget] = useState("1.1.1.1");
  const [intervalSeconds, setIntervalSeconds] = useState(1);
  const [probes, setProbes] = useState<QualityProbe[]>([]);
  const [stats, setStats] = useState<QualityStats>({
    samples: 0,
    packetLossPercent: null,
    minLatencyMs: null,
    averageLatencyMs: null,
    maxLatencyMs: null,
  });
  const [isRunning, setIsRunning] = useState(false);
  const [isStopping, setIsStopping] = useState(false);
  const [status, setStatus] = useState<"idle" | "running" | "stopped" | "error">("idle");
  const [error, setError] = useState<string | null>(null);
  const [latestProbe, setLatestProbe] = useState<QualityProbe | null>(null);
  const [qualityRating, setQualityRating] = useState<QualityMonitorUpdate["qualityRating"]>({
    level: "noData",
    explanation: "No probe outcomes are available yet.",
  });
  const [exportFeedback, setExportFeedback] = useState<{
    status: "success" | "cancelled" | "error";
    message: string;
  } | null>(null);
  const [alertSettings, setAlertSettings] = useState<QualityAlertSettings>({
    enabled: false,
    failureThreshold: 3,
    cooldownSeconds: 300,
  });
  const [alertSettingsLoaded, setAlertSettingsLoaded] = useState(false);
  const isMounted = useRef(false);
  const runningRef = useRef(false);

  useEffect(() => {
    isMounted.current = true;
    let unlistenProbe: (() => void) | undefined;
    let unlistenStatus: (() => void) | undefined;

    void Promise.all([
      listen<QualityMonitorUpdate>("quality-monitor-probe", (event) => {
        const { probe, rollingStats } = event.payload;
        setLatestProbe(probe);
        setStats(rollingStats);
        setQualityRating(event.payload.qualityRating);
        setProbes((current) => [...current.filter((item) => item.timestampMs !== probe.timestampMs), probe].slice(-300));
      }),
      listen<QualityMonitorStatus>("quality-monitor-status", (event) => {
        runningRef.current = false;
        setIsRunning(false);
        setIsStopping(false);
        if (event.payload.status === "error") {
          setStatus("error");
          setError(event.payload.message ?? "Monitoring stopped because a network error occurred.");
        } else {
          setStatus("stopped");
          if (event.payload.message) setError(event.payload.message);
        }
      }),
    ]).then(async ([probeListener, statusListener]) => {
      if (isMounted.current) {
        unlistenProbe = probeListener;
        unlistenStatus = statusListener;
      } else {
        probeListener();
        statusListener();
        return;
      }

      const snapshot = await invoke<QualityMonitorSnapshot>("get_quality_monitor_snapshot");
      if (!isMounted.current) return;
      setTarget(snapshot.target);
      setIntervalSeconds(snapshot.intervalSeconds);
      setAlertSettings(snapshot.alertSettings);
      setAlertSettingsLoaded(true);
      setProbes((current) => {
        const merged = new Map<number, QualityProbe>();
        [...snapshot.history, ...current].forEach((probe) => merged.set(probe.timestampMs, probe));
        return [...merged.values()].sort((a, b) => a.timestampMs - b.timestampMs).slice(-300);
      });
      setStats(snapshot.rollingStats);
      setQualityRating(snapshot.qualityRating);
      const latest = snapshot.history[snapshot.history.length - 1] ?? null;
      setLatestProbe(latest);
      runningRef.current = snapshot.running;
      setIsRunning(snapshot.running);
      setStatus(snapshot.running ? "running" : snapshot.history.length > 0 ? "stopped" : "idle");
    }).catch((listenerError: unknown) => {
      if (isMounted.current) {
        setError(`Could not restore Network Quality Monitor state: ${listenerError instanceof Error ? listenerError.message : String(listenerError)}`);
      }
    });

    return () => {
      isMounted.current = false;
      unlistenProbe?.();
      unlistenStatus?.();
    };
  }, []);

  useEffect(() => {
    if (!alertSettingsLoaded) return;
    void invoke<AppSettings>("get_app_settings")
      .then(async (current) => {
        const updated = {
          ...current,
          networkAlertsEnabled: alertSettings.enabled,
          networkAlertFailureThreshold: alertSettings.failureThreshold,
          networkAlertCooldownSeconds: alertSettings.cooldownSeconds,
        };
        await invoke("save_app_settings", { value: updated });
        onAppSettingsChange(updated);
      })
      .then(() => invoke("configure_quality_alerts", alertSettings))
      .catch((configurationError: unknown) => {
        setError(`Could not configure and persist Network Quality alerts: ${configurationError instanceof Error ? configurationError.message : String(configurationError)}`);
      });
  }, [alertSettings, alertSettingsLoaded, onAppSettingsChange]);

  const startMonitoring = async () => {
    if (runningRef.current) return;
    setError(null);
    setProbes([]);
    setStats({
      samples: 0,
      packetLossPercent: null,
      minLatencyMs: null,
      averageLatencyMs: null,
      maxLatencyMs: null,
    });
    setLatestProbe(null);
    setQualityRating({
      level: "noData",
      explanation: "No probe outcomes are available yet.",
    });
    setExportFeedback(null);
    setIsStopping(false);
    setStatus("running");
    setIsRunning(true);
    runningRef.current = true;
    try {
      await invoke("start_quality_monitor", { hostname: target, intervalSeconds });
      if (!isMounted.current) {
        await invoke("stop_quality_monitor");
      }
    } catch (startError) {
      runningRef.current = false;
      if (isMounted.current) {
        setIsRunning(false);
        setStatus("error");
        setError(startError instanceof Error ? startError.message : String(startError));
      }
    }
  };

  const persistMonitorOptions = async (hostname: string, interval: number) => {
    try {
      await invoke("configure_quality_monitor_options", {
        hostname,
        intervalSeconds: interval,
      });
    } catch (optionsError) {
      setError(optionsError instanceof Error ? optionsError.message : String(optionsError));
    }
  };

  const stopMonitoring = async () => {
    setIsStopping(true);
    try {
      await invoke("stop_quality_monitor");
      runningRef.current = false;
      setIsRunning(false);
      setIsStopping(false);
      setStatus("stopped");
    } catch (stopError) {
      setIsStopping(false);
      setError(stopError instanceof Error ? stopError.message : String(stopError));
    }
  };

  const clearHistory = async () => {
    try {
      await invoke("clear_quality_monitor_history");
      setProbes([]);
      setStats({
        samples: 0,
        packetLossPercent: null,
        minLatencyMs: null,
        averageLatencyMs: null,
        maxLatencyMs: null,
      });
      setLatestProbe(null);
      setQualityRating({
        level: "noData",
        explanation: "No probe outcomes are available yet.",
      });
      setError(null);
      setExportFeedback(null);
    } catch (clearError) {
      setError(clearError instanceof Error ? clearError.message : String(clearError));
    }
  };

  const exportHistory = async () => {
    setExportFeedback(null);
    if (probes.length === 0) {
      setExportFeedback({
        status: "error",
        message: "There are no recorded probes to export.",
      });
      return;
    }

    try {
      const path = await save({
        defaultPath: `fluxnet-network-quality-${new Date().toISOString().replace(/:/g, "-")}.csv`,
        filters: [{ name: "CSV file", extensions: ["csv"] }],
      });
      if (!path) {
        setExportFeedback({ status: "cancelled", message: "CSV export was cancelled." });
        return;
      }
      const exportedCount = await invoke<number>("export_quality_monitor_history", { path });
      setExportFeedback({
        status: "success",
        message: `Exported ${exportedCount} retained probe ${exportedCount === 1 ? "record" : "records"}.`,
      });
    } catch (exportError) {
      setExportFeedback({
        status: "error",
        message: exportError instanceof Error ? exportError.message : String(exportError),
      });
    }
  };

  const connectionStatus = isRunning
    ? isStopping
      ? "Stopping…"
      : latestProbe?.status === "success"
        ? "Connected"
        : latestProbe
          ? latestProbe.status === "timeout" ? "Timed out" : "Unreachable"
          : "Monitoring"
    : status === "stopped"
      ? "Stopped"
      : status === "error"
        ? "Error"
        : latestProbe?.status === "success"
          ? "Connected"
          : latestProbe
            ? latestProbe.status === "timeout" ? "Timed out" : "Unreachable"
            : "Waiting for samples";
  const qualityStatusClass = status === "error"
    ? "error"
    : isRunning && latestProbe && latestProbe.status !== "success"
      ? "warning"
      : isRunning
        ? "running"
        : "idle";
  const value = (latency: number | null) => latency === null ? "—" : `${latency.toFixed(1)} ms`;
  const recentProbes = [...probes].reverse().slice(0, 20);

  return (
    <div className="quality-page">
      <header className="page-header">
        <div>
          <div className="eyebrow">NETWORK TOOLS</div>
          <h1>Network Quality Monitor</h1>
          <p className="page-subtitle">Track real ICMP latency and packet loss over time.</p>
        </div>
        <div className="header-meta">
          <div className={`live-indicator${isRunning ? "" : " is-idle"}`}><span /> {isRunning ? "LIVE MONITORING" : "ON-DEMAND MONITOR"}</div>
          <div className="updated-label">ICMP reachability · not throughput</div>
        </div>
      </header>

      <section className="panel quality-panel">
        <div className="panel-heading">
          <div>
            <div className="eyebrow">MONITORING TARGET</div>
            <h2>Connection status</h2>
          </div>
          <span className={`quality-status quality-status-${qualityStatusClass}`}>
            <i /> {connectionStatus}
          </span>
        </div>
        <p className="diagnostic-description">
          Each sample is a real Windows ICMP echo probe. Packet loss and latency statistics use the latest 60 probe outcomes; the chart and log retain up to 300 probes.
        </p>
        <div className={`quality-rating quality-rating-${qualityRating.level}`} aria-live="polite">
          <span className="quality-rating-label">CONNECTION QUALITY</span>
          <strong>{qualityRating.level === "noData" ? "No data" : qualityRating.level === "disconnected" ? "Disconnected" : qualityRating.level[0].toUpperCase() + qualityRating.level.slice(1)}</strong>
          <p>{qualityRating.explanation}</p>
        </div>
        <form
          className="quality-controls"
          onSubmit={(event) => {
            event.preventDefault();
            void startMonitoring();
          }}
        >
          <label>
            Target hostname or IP
            <input
              value={target}
              maxLength={253}
              onChange={(event) => setTarget(event.currentTarget.value)}
              onBlur={() => void persistMonitorOptions(target, intervalSeconds)}
            />
          </label>
          <label>
            Probe interval
            <select
              value={intervalSeconds}
              onChange={(event) => {
                const nextInterval = Number(event.currentTarget.value);
                setIntervalSeconds(nextInterval);
                void persistMonitorOptions(target, nextInterval);
              }}
            >
              <option value={1}>1 second</option>
              <option value={2}>2 seconds</option>
              <option value={5}>5 seconds</option>
            </select>
          </label>
          <div className="quality-actions">
            {!isRunning ? (
              <button className="test-connection-button" type="submit" disabled={!target.trim()}>
                <Icon name="monitor" size={16} /> Start Monitoring
              </button>
            ) : (
              <button className="speed-stop-button" type="button" onClick={() => void stopMonitoring()} disabled={isStopping}>
                {isStopping ? "Stopping…" : "Stop Monitoring"}
              </button>
            )}
            <button className="refresh-button" type="button" onClick={() => void clearHistory()} disabled={probes.length === 0}>
              Clear history
            </button>
            <button className="refresh-button quality-export-button" type="button" onClick={() => void exportHistory()}>
              <Icon name="download" size={14} /> Export CSV
            </button>
          </div>
        </form>
        <div className="quality-alert-settings">
          <label className="quality-alert-toggle">
            <input
              type="checkbox"
              checked={alertSettings.enabled}
              disabled={!alertSettingsLoaded}
              onChange={(event) => setAlertSettings((current) => ({ ...current, enabled: event.currentTarget.checked }))}
            />
            <span>Notify me about sustained connection problems</span>
          </label>
          <label>
            Consecutive failed probes
            <select
              value={alertSettings.failureThreshold}
              disabled={!alertSettingsLoaded}
              onChange={(event) => setAlertSettings((current) => ({ ...current, failureThreshold: Number(event.currentTarget.value) }))}
            >
              <option value={2}>2 probes</option>
              <option value={3}>3 probes</option>
              <option value={5}>5 probes</option>
              <option value={10}>10 probes</option>
            </select>
          </label>
          <label>
            Notification cooldown
            <select
              value={alertSettings.cooldownSeconds}
              disabled={!alertSettingsLoaded}
              onChange={(event) => setAlertSettings((current) => ({ ...current, cooldownSeconds: Number(event.currentTarget.value) }))}
            >
              <option value={60}>1 minute</option>
              <option value={300}>5 minutes</option>
              <option value={900}>15 minutes</option>
            </select>
          </label>
          <p>Notifications are sent only after the selected consecutive failures, then limited by the cooldown. Windows notification settings can also suppress alerts.</p>
        </div>
        {exportFeedback && (
          <div className={`quality-export-feedback is-${exportFeedback.status}`} role={exportFeedback.status === "error" ? "alert" : "status"}>
            {exportFeedback.message}
          </div>
        )}

        {isRunning && (
          <div className="diagnostic-message is-pending" role="status" aria-live="polite">
            {isStopping
              ? "Waiting for the current ICMP probe to finish or time out before stopping."
              : `Monitoring ${target}. Next probe starts ${intervalSeconds} second${intervalSeconds === 1 ? "" : "s"} after the previous probe completes.`}
          </div>
        )}
        {error && (
          <div className="diagnostic-message is-error" role="alert">
            <strong>{latestProbe?.status === "timeout" || latestProbe?.status === "unreachable" ? "Target cannot be reached" : "Monitoring error"}</strong>
            <span>{error}</span>
          </div>
        )}
        {!error && latestProbe && latestProbe.status !== "success" && (
          <div className="diagnostic-message is-error" role="alert">
            <strong>Target cannot be reached</strong>
            <span>{latestProbe.detail ?? `Latest probe status: ${latestProbe.status}.`}</span>
          </div>
        )}

        <div className="quality-metrics">
          <DiagnosticStat label="Current latency" value={latestProbe?.status === "success" ? value(latestProbe.latencyMs) : "—"} />
          <DiagnosticStat label="Packet loss · last 60" value={stats.packetLossPercent === null ? "—" : `${stats.packetLossPercent.toFixed(1)}%`} />
          <DiagnosticStat label="Minimum · last 60" value={value(stats.minLatencyMs)} />
          <DiagnosticStat label="Average · last 60" value={value(stats.averageLatencyMs)} />
          <DiagnosticStat label="Maximum · last 60" value={value(stats.maxLatencyMs)} />
        </div>
        {stats.samples === 0 && <p className="quality-no-samples">No real probe measurements yet.</p>}
      </section>

      <section className="panel quality-panel">
        <div className="panel-heading">
          <div>
            <div className="eyebrow">LIVE HISTORY · LAST {probes.length} / 300</div>
            <h2>Latency chart</h2>
          </div>
        </div>
        <QualityLatencyChart probes={probes} />
      </section>

      <section className="panel quality-panel quality-history-panel">
        <div className="panel-heading">
          <div>
            <div className="eyebrow">RECENT SAMPLES</div>
            <h2>Probe history</h2>
          </div>
          <span className="diagnostic-probe-note">Latest 20 shown</span>
        </div>
        {recentProbes.length === 0 ? (
          <div className="quality-chart-empty">Probe records will appear here with their actual timestamps.</div>
        ) : (
          <div className="quality-history-list">
            {recentProbes.map((probe) => (
              <div className={`quality-history-row is-${probe.status}`} key={probe.timestampMs}>
                <time dateTime={new Date(probe.timestampMs).toISOString()}>{new Date(probe.timestampMs).toLocaleString()}</time>
                <span>{probe.status === "success" ? `${probe.latencyMs?.toFixed(1)} ms` : probe.status}</span>
                <small>{probe.detail ?? "ICMP echo reply received"}</small>
              </div>
            ))}
          </div>
        )}
      </section>
      <p className="quality-caveat">
        Quality ratings are configurable heuristics based on the latest 60 outcomes (average successful-probe latency and packet loss), not universal internet-quality standards. ICMP results can be affected by filtering and the selected target; they are not download/upload speeds and do not verify BDIX status.
      </p>
    </div>
  );
}

function BdixAnalysisPage({
  target,
  onTargetChange,
  probeCount,
  onProbeCountChange,
  timeoutMs,
  onTimeoutChange,
  result,
  isRunning,
  error,
  wasStopped,
  onStart,
  onStop,
  onClear,
  servers,
  onServersChange,
  storageError,
  onStorageError,
}: {
  target: string;
  onTargetChange: (target: string) => void;
  probeCount: number;
  onProbeCountChange: (count: number) => void;
  timeoutMs: number;
  onTimeoutChange: (timeoutMs: number) => void;
  result: BdixAnalysisResult | null;
  isRunning: boolean;
  error: string | null;
  wasStopped: boolean;
  onStart: () => void;
  onStop: () => void;
  onClear: () => void;
  servers: SavedBdixServer[];
  onServersChange: (servers: SavedBdixServer[]) => void;
  storageError: string | null;
  onStorageError: (error: string | null) => void;
}) {
  const [newServerName, setNewServerName] = useState("");
  const [newServerTarget, setNewServerTarget] = useState("");

  const addServer = () => {
    if (!newServerName.trim() || !newServerTarget.trim()) return;
    onServersChange([
      ...servers,
      {
        id: crypto.randomUUID(),
        name: newServerName.trim(),
        target: newServerTarget.trim(),
        verificationSource: "",
        verificationNotes: "",
        verified: false,
      },
    ]);
    onStorageError(null);
    setNewServerName("");
    setNewServerTarget("");
  };

  const updateServer = (id: string, updates: Partial<SavedBdixServer>) => {
    onStorageError(null);
    onServersChange(servers.map((server) =>
      server.id === id ? { ...server, ...updates } : server,
    ));
  };

  const verificationLabel = result?.status === "success"
    ? "All ICMP probes received replies"
    : result?.status === "partial"
      ? "Partial ICMP response"
    : result?.status === "timeout"
      ? "Probe timeout"
      : result?.status === "dnsResolutionFailed"
        ? "DNS resolution failed"
        : result?.status === "noAddress"
          ? "No addresses found"
          : result?.status === "unreachable"
            ? "Destination unreachable"
            : null;

  return (
    <div className="bdix-page">
      <header className="page-header">
        <div>
          <div className="eyebrow">NETWORK TOOLS</div>
          <h1>BDIX Analysis</h1>
          <p className="page-subtitle">Measure DNS resolution and ICMP reachability for targets you supply.</p>
        </div>
        <div className="header-meta">
          <div className="live-indicator"><span /> ON-DEMAND ANALYSIS</div>
          <div className="updated-label">ICMP is not a speed test</div>
        </div>
      </header>

      <section className="panel bdix-panel">
        <div className="panel-heading">
          <div>
            <div className="eyebrow">TARGET DIAGNOSTICS</div>
            <h2>Analyze a server</h2>
          </div>
          <span className="diagnostic-probe-note">{probeCount} ICMP probes · {timeoutMs} ms each</span>
        </div>
        <p className="diagnostic-description">
          BDIX status cannot be determined from a hostname, IP address, latency, or ping alone. Use a server supplied by your ISP or another trusted source and verify its BDIX status separately.
        </p>
        <form
          className="bdix-form"
          onSubmit={(event) => {
            event.preventDefault();
            onStart();
          }}
        >
          <label className="diagnostic-input-label" htmlFor="bdix-target">Hostname or IP address</label>
          <input
            id="bdix-target"
            className="bdix-target-input"
            value={target}
            maxLength={253}
            autoComplete="url"
            placeholder="Enter a server supplied by your ISP"
            disabled={isRunning}
            onChange={(event) => onTargetChange(event.currentTarget.value)}
          />
          <div className="bdix-options">
            <label>
              Probe count
              <select value={probeCount} disabled={isRunning} onChange={(event) => onProbeCountChange(Number(event.currentTarget.value))}>
                <option value={4}>4 probes</option>
                <option value={6}>6 probes</option>
                <option value={10}>10 probes</option>
              </select>
            </label>
            <label>
              Timeout per probe
              <select value={timeoutMs} disabled={isRunning} onChange={(event) => onTimeoutChange(Number(event.currentTarget.value))}>
                <option value={1000}>1 second</option>
                <option value={1500}>1.5 seconds</option>
                <option value={3000}>3 seconds</option>
                <option value={5000}>5 seconds</option>
              </select>
            </label>
            <div className="bdix-actions">
              {!isRunning ? (
                <button className="test-connection-button" type="submit" disabled={!target.trim()}>
                  <Icon name="analysis" size={16} /> Start Analysis
                </button>
              ) : (
                <button className="speed-stop-button" type="button" onClick={onStop}>Stop</button>
              )}
              <button className="refresh-button" type="button" onClick={onStart} disabled={isRunning || !target.trim()}>
                <Icon name="refresh" size={14} /> Refresh
              </button>
              <button className="refresh-button" type="button" onClick={onClear} disabled={isRunning}>
                Clear results
              </button>
            </div>
          </div>
        </form>

        {isRunning && (
          <div className="diagnostic-message is-pending" role="status" aria-live="polite">
            Resolving DNS and running real ICMP probes. You can stop the analysis; each active probe is bounded by its timeout.
          </div>
        )}
        {error && !isRunning && (
          <div className="diagnostic-message is-error" role="alert">
            <strong>Analysis could not run</strong>
            <span>{error}</span>
          </div>
        )}
        {wasStopped && !isRunning && !error && (
          <div className="diagnostic-message is-pending" role="status">Analysis stopped. No partial statistics are reported.</div>
        )}
        {result && !isRunning && (
          <div className={`bdix-result is-${result.status}`}>
            <div className="bdix-result-heading">
              <div>
                <span className="diagnostic-result-label">CONNECTIVITY RESULT · {result.hostname}</span>
                <strong>{verificationLabel}</strong>
              </div>
              <span className="bdix-timestamp">{new Date(result.timestampMs).toLocaleString()}</span>
            </div>
            {result.message && <p className="diagnostic-result-message">{result.message}</p>}
            <div className="bdix-addresses">
              <span>
                DNS resolution · {result.resolvedAddresses.length > 0
                  ? "Resolved"
                  : result.status === "dnsResolutionFailed"
                    ? "Failed"
                    : result.status === "timeout"
                      ? "Timed out"
                      : "No addresses returned"}
              </span>
            </div>
            {result.resolvedAddresses.length > 0 && (
              <div className="bdix-addresses">
                <span>Resolved addresses</span>
                <strong>{result.resolvedAddresses.join(" · ")}</strong>
                {result.probedAddress && <small>Probed address: {result.probedAddress}</small>}
              </div>
            )}
            {result.attempts > 0 && (
              <>
                <div className="diagnostic-stats bdix-stats">
                  <DiagnosticStat label="Average latency" value={result.averageLatencyMs === null ? "—" : `${result.averageLatencyMs.toFixed(1)} ms`} />
                  <DiagnosticStat label="Minimum latency" value={result.minLatencyMs === null ? "—" : `${result.minLatencyMs.toFixed(1)} ms`} />
                  <DiagnosticStat label="Maximum latency" value={result.maxLatencyMs === null ? "—" : `${result.maxLatencyMs.toFixed(1)} ms`} />
                  <DiagnosticStat label="Packet loss" value={result.packetLossPercent === null ? "—" : `${result.packetLossPercent.toFixed(0)}%`} />
                </div>
                <div className="bdix-probes" aria-label="Individual ICMP probe results">
                  {result.probes.map((probe) => (
                    <div className={`bdix-probe is-${probe.status}`} key={probe.attempt}>
                      <strong>Probe {probe.attempt}</strong>
                      <span>{probe.latencyMs === null ? probe.status : `${probe.latencyMs.toFixed(1)} ms`}</span>
                      {probe.detail && <small>{probe.detail}</small>}
                    </div>
                  ))}
                </div>
              </>
            )}
            <p className="bdix-caveat">
              ICMP latency and packet loss describe reachability only. They are not download/upload speeds and do not establish BDIX status.
            </p>
          </div>
        )}
      </section>

      <section className="panel bdix-panel bdix-server-panel">
        <div className="panel-heading">
          <div>
            <div className="eyebrow">USER-MANAGED TARGETS</div>
            <h2>BDIX Server List</h2>
          </div>
          <span className="diagnostic-probe-note">No addresses are preloaded</span>
        </div>
        <p className="diagnostic-description">
          Add targets supplied by your ISP or a trusted source. A target is marked verified only when you provide the source and your verification notes. FluxNet does not infer BDIX status from test results.
        </p>
        {storageError && <div className="diagnostic-message is-error" role="alert">{storageError}</div>}
        <div className="bdix-add-form">
          <label>Server name<input value={newServerName} maxLength={80} onChange={(event) => setNewServerName(event.currentTarget.value)} placeholder="ISP-provided name" /></label>
          <label>Hostname or IP<input value={newServerTarget} maxLength={253} onChange={(event) => setNewServerTarget(event.currentTarget.value)} placeholder="Provided target address" /></label>
          <button className="test-connection-button" type="button" onClick={addServer} disabled={!newServerName.trim() || !newServerTarget.trim()}>
            Add target
          </button>
        </div>
        {servers.length === 0 ? (
          <div className="bdix-empty">No saved servers. Add a target supplied by your ISP or another trusted source.</div>
        ) : (
          <div className="bdix-server-list">
            {servers.map((server) => (
              <article className="bdix-server-entry" key={server.id}>
                <div className="bdix-server-topline">
                  <input aria-label="Server name" value={server.name} maxLength={80} onChange={(event) => updateServer(server.id, { name: event.currentTarget.value })} />
                  <span className={`bdix-verification${server.verified ? " is-verified" : ""}`}>
                    {server.verified ? "BDIX verified" : "Unverified target"}
                  </span>
                  <button className="refresh-button" type="button" onClick={() => onTargetChange(server.target)}>Use target</button>
                  <button className="refresh-button" type="button" onClick={() => onServersChange(servers.filter((entry) => entry.id !== server.id))}>Remove</button>
                </div>
                <label>Target<input value={server.target} maxLength={253} onChange={(event) => updateServer(server.id, { target: event.currentTarget.value, verified: false })} /></label>
                <label>Verification source<input value={server.verificationSource} maxLength={300} onChange={(event) => updateServer(server.id, { verificationSource: event.currentTarget.value, verified: false })} placeholder="ISP notice, trusted list, or manual verification source" /></label>
                <label>Verification notes<textarea value={server.verificationNotes} maxLength={1000} rows={2} onChange={(event) => updateServer(server.id, { verificationNotes: event.currentTarget.value, verified: false })} placeholder="Record how you verified this server's BDIX status" /></label>
                <button
                  className={`bdix-verify-button${server.verified ? " is-verified" : ""}`}
                  type="button"
                  disabled={!server.verificationSource.trim() || !server.verificationNotes.trim()}
                  onClick={() => updateServer(server.id, { verified: !server.verified })}
                >
                  {server.verified ? "Mark as unverified" : "Mark BDIX verified"}
                </button>
              </article>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}

function SpeedTestPage({
  provider,
  onProviderChange,
  endpoint,
  onEndpointChange,
  trustedEndpoint,
  onTrustedEndpointChange,
  isRunning,
  progress,
  elapsedMs,
  result,
  error,
  wasStopped,
  onStart,
  onStop,
  onProgress,
  onProgressError,
}: {
  provider: "cloudflare" | "custom";
  onProviderChange: (provider: "cloudflare" | "custom") => void;
  endpoint: string;
  onEndpointChange: (endpoint: string) => void;
  trustedEndpoint: boolean;
  onTrustedEndpointChange: (trusted: boolean) => void;
  isRunning: boolean;
  progress: SpeedTestProgress | null;
  elapsedMs: number;
  result: SpeedTestResult | null;
  error: string | null;
  wasStopped: boolean;
  onStart: () => void;
  onStop: () => void;
  onProgress: (progress: SpeedTestProgress) => void;
  onProgressError: (message: string) => void;
}) {
  useEffect(() => {
    let unlisten: (() => void) | undefined;
    let mounted = true;
    void listen<SpeedTestProgress>("speed-test-progress", (event) => {
      onProgress(event.payload);
    }).then((stopListening) => {
      if (mounted) {
        unlisten = stopListening;
      } else {
        stopListening();
      }
    }).catch((listenerError: unknown) => {
      onProgressError(
        `Could not receive speed-test progress: ${listenerError instanceof Error ? listenerError.message : String(listenerError)}`,
      );
    });
    return () => {
      mounted = false;
      unlisten?.();
    };
  }, [onProgress, onProgressError]);

  const phaseProgress = progress?.phase === "upload"
    ? 50 + (progress.percent / 2)
    : (progress?.percent ?? 0) / 2;

  return (
    <div className="speed-test-page">
      <header className="page-header">
        <div>
          <div className="eyebrow">NETWORK TOOLS</div>
          <h1>Speed test</h1>
          <p className="page-subtitle">Measure real throughput using a trusted test server you configure.</p>
        </div>
        <div className="header-meta speed-test-meta">
          <div className="live-indicator"><span /> ON-DEMAND TEST</div>
          <div className="updated-label">No tests run in the background</div>
        </div>
      </header>

      <section className="panel speed-test-panel">
        <div className="speed-test-heading">
          <div className="speed-test-icon"><Icon name="speed" size={23} /></div>
          <div>
            <div className="eyebrow">CONFIGURE TEST SERVER</div>
            <h2>Choose your endpoint</h2>
          </div>
        </div>
        <p className="speed-test-copy">
          Choose Cloudflare's public speed-test service or configure a custom HTTPS server you trust.
        </p>
        <label className="speed-endpoint-label" htmlFor="speed-test-provider">Test server</label>
        <select
          id="speed-test-provider"
          className="speed-endpoint-input"
          value={provider}
          disabled={isRunning}
          onChange={(event) => {
            const selectedProvider = event.currentTarget.value;
            if (selectedProvider === "cloudflare" || selectedProvider === "custom") {
              onProviderChange(selectedProvider);
            }
          }}
        >
          <option value="cloudflare">Cloudflare public speed test</option>
          <option value="custom">Custom server</option>
        </select>
        {provider === "cloudflare" ? (
          <div className="speed-contract">
            <strong>Cloudflare endpoints</strong>
            <ul>
              <li><code>GET https://speed.cloudflare.com/__down?bytes=5242880</code></li>
              <li><code>POST https://speed.cloudflare.com/__up</code> with a 5 MiB <code>application/octet-stream</code> body.</li>
            </ul>
          </div>
        ) : (
          <>
            <label className="speed-endpoint-label" htmlFor="speed-test-endpoint">Custom HTTPS server origin</label>
            <input
              id="speed-test-endpoint"
              className="speed-endpoint-input"
              type="url"
              autoComplete="url"
              placeholder="https://speed.example.com"
              value={endpoint}
              disabled={isRunning}
              onChange={(event) => {
                onEndpointChange(event.currentTarget.value);
                onTrustedEndpointChange(false);
              }}
            />
            <div className="speed-contract">
              <strong>Required custom-server behavior</strong>
              <ul>
                <li><code>GET /download?bytes=5242880</code> returns exactly 5 MiB of data.</li>
                <li><code>POST /upload</code> consumes exactly 5 MiB of binary data and returns a successful HTTP status.</li>
                <li>Use HTTPS with a valid certificate; redirects are not followed.</li>
              </ul>
            </div>
          </>
        )}
        <label className="speed-trust-check">
          <input
            type="checkbox"
            checked={trustedEndpoint}
            disabled={isRunning}
            onChange={(event) => onTrustedEndpointChange(event.currentTarget.checked)}
          />
          <span>
            {provider === "cloudflare"
              ? "I understand this test sends data to Cloudflare and have reviewed the endpoint behavior above."
              : "I trust this endpoint and have verified that it supports both tests above."}
          </span>
        </label>

        <div className="speed-data-warning">
          <span className="warning-icon">!</span>
          <span>Each run transfers up to 5 MiB down and 5 MiB up. A speed test consumes data. Results depend on this server, its load, and the network route to it.</span>
        </div>

        <div className="speed-actions">
          {!isRunning ? (
            <button
              className="test-connection-button speed-start-button"
              type="button"
              disabled={(provider === "custom" && !endpoint.trim()) || !trustedEndpoint}
              onClick={onStart}
            >
              <Icon name="speed" size={17} /> Start speed test
            </button>
          ) : (
            <button className="speed-stop-button" type="button" onClick={onStop}>
              Stop test
            </button>
          )}
          {isRunning && (
            <span className="speed-elapsed">
              Elapsed <strong>{(elapsedMs / 1000).toFixed(1)} s</strong>
            </span>
          )}
        </div>

        {provider === "custom" && !endpoint.trim() && !isRunning && (
          <div className="speed-config-hint">Configure a trusted endpoint to enable the test.</div>
        )}

        {isRunning && (
          <div className="speed-progress" role="status" aria-live="polite">
            <div className="speed-progress-header">
              <strong>{progress?.phase === "upload" ? "Uploading test data" : "Downloading test data"}</strong>
              <span>{Math.floor(phaseProgress)}%</span>
            </div>
            <div
              className="speed-progress-track"
              role="progressbar"
              aria-valuenow={phaseProgress}
              aria-valuemin={0}
              aria-valuemax={100}
            >
              <span style={{ width: `${phaseProgress}%` }} />
            </div>
            <div className="speed-progress-detail">
              {progress
                ? `${formatBytes(progress.bytesTransferred)} of ${formatBytes(progress.totalBytes)} · ${progress.phase === "upload" ? "Upload" : "Download"} phase`
                : "Preparing the download phase…"}
              <span>{(elapsedMs / 1000).toFixed(1)} s elapsed</span>
            </div>
          </div>
        )}

        {error && !isRunning && (
          <div className="diagnostic-message is-error speed-test-message" role="alert">
            <strong>Speed test could not complete</strong>
            <span>{error}</span>
          </div>
        )}
        {wasStopped && !isRunning && !error && (
          <div className="diagnostic-message is-pending speed-test-message" role="status">
            Speed test stopped. No partial throughput results are reported.
          </div>
        )}
        {result && !isRunning && !error && !wasStopped && (
          <div className="speed-results">
            <div className="speed-result-heading">
              <div>
                <div className="eyebrow">MEASURED RESULT</div>
                <h2>Test complete</h2>
              </div>
              <span>{(result.elapsedMs / 1000).toFixed(1)} s total</span>
            </div>
            <div className="speed-result-metrics">
              <SpeedResultMetric
                label="Download"
                value={result.downloadMbps}
                duration={result.downloadElapsedMs}
                icon="download"
              />
              <SpeedResultMetric
                label="Upload"
                value={result.uploadMbps}
                duration={result.uploadElapsedMs}
                icon="upload"
              />
            </div>
            <p className="speed-result-caveat">
              Measured from {result.endpoint}. Results vary with server load and the route between this device and the test server.
            </p>
          </div>
        )}
      </section>
    </div>
  );
}

function SpeedResultMetric({
  label,
  value,
  duration,
  icon,
}: {
  label: string;
  value: number;
  duration: number;
  icon: "download" | "upload";
}) {
  return (
    <article className="speed-result-card">
      <div className="speed-result-label"><span><Icon name={icon} size={18} /></span>{label}</div>
      <strong>{value.toFixed(2)}<small> Mbps</small></strong>
      <span className="speed-result-duration">Elapsed {(duration / 1000).toFixed(2)} s</span>
    </article>
  );
}

function formatBytes(bytes: number): string {
  return `${(bytes / (1024 * 1024)).toFixed(2)} MiB`;
}

function loadSavedBdixServers(): { servers: SavedBdixServer[]; error: string | null } {
  try {
    const stored = localStorage.getItem("fluxnet.bdix-servers");
    if (!stored) return { servers: [], error: null };
    const parsed: unknown = JSON.parse(stored);
    if (!Array.isArray(parsed)) {
      throw new Error("Saved BDIX server data is not a list.");
    }
    const servers = parsed.filter((item): item is SavedBdixServer => (
      typeof item === "object"
      && item !== null
      && "id" in item && typeof item.id === "string"
      && "name" in item && typeof item.name === "string"
      && "target" in item && typeof item.target === "string"
      && "verificationSource" in item && typeof item.verificationSource === "string"
      && "verificationNotes" in item && typeof item.verificationNotes === "string"
      && "verified" in item && typeof item.verified === "boolean"
    ));
    if (servers.length !== parsed.length) {
      throw new Error("Saved BDIX server data contains invalid entries.");
    }
    return {
      servers: servers.map((server) => ({
        ...server,
        verified: server.verified
          && Boolean(server.verificationSource.trim())
          && Boolean(server.verificationNotes.trim()),
      })),
      error: null,
    };
  } catch (loadError) {
    return {
      servers: [],
      error: `Could not load saved BDIX server entries: ${loadError instanceof Error ? loadError.message : String(loadError)}`,
    };
  }
}

function formatDownloadBytes(bytes: number | null): string {
  if (bytes === null) return "Unknown size";
  if (bytes < 1000) return `${bytes} B`;
  const units = ["kB", "MB", "GB", "TB"];
  let value = bytes;
  let unitIndex = -1;
  do {
    value /= 1000;
    unitIndex += 1;
  } while (value >= 1000 && unitIndex < units.length - 1);
  return `${value.toFixed(value >= 100 ? 0 : value >= 10 ? 1 : 2)} ${units[unitIndex]}`;
}

function formatDuration(seconds: number | null): string {
  if (seconds === null || !Number.isFinite(seconds)) return "—";
  if (seconds < 60) return `${Math.ceil(seconds)} sec`;
  const minutes = Math.floor(seconds / 60);
  const remainder = Math.ceil(seconds % 60);
  return `${minutes} min ${remainder} sec`;
}

function displayDownloadUrl(value: string): string {
  try {
    const url = new URL(value);
    return `${url.origin}${url.pathname}`;
  } catch {
    return "HTTP(S) download URL";
  }
}

type BrowserBridgeStatus = {
  connectionStatus: "connected" | "disconnected" | "awaitingPairing";
  lastHandshakeAtMs: number | null;
  nativeHostStatus: "installed" | "notInstalled" | "unsupported";
  registeredBrowsers: string[];
};

type BrowserMediaResource = {
  url: string;
  mediaType: string;
  pageTitle: string;
  pageDomain: string;
  tabId?: number | null;
  filename?: string | null;
  mimeType?: string | null;
  totalBytes?: number | null;
  observedBytes: number | null;
  width: number | null;
  height: number | null;
  receivedAtMs: number;
  queueState: "detected" | "unsupported" | "queued" | "completed" | "expired" | "error";
  message: string | null;
};

type BrowserMediaPayload = Omit<BrowserMediaResource, "queueState" | "message">;

function mapBrowserMediaResource(
  payload: BrowserMediaPayload,
  downloads: DownloadEntry[],
): BrowserMediaResource {
  const unsupported = payload.mediaType === "hls" || payload.mediaType === "dash";
  const initial: BrowserMediaResource = {
    ...payload,
    queueState: unsupported ? "unsupported" : "detected",
    message: unsupported ? "Manifest detected; segment merging is unavailable, so FluxNet cannot create a complete video file." : null,
  };
  if (unsupported) return initial;
  const entry = downloads.find((download) => download.url === payload.url);
  if (!entry) return initial;
  if (entry.status === "completed") {
    return { ...initial, queueState: "completed", message: "Download completed." };
  }
  if (entry.status === "cancelled") {
    return { ...initial, queueState: "detected", message: "Previous download was cancelled." };
  }
  if (entry.status === "failed") {
    const message = entry.error ?? "The download failed.";
    const expired = /\b(401|403|404)\b|expired|authentication|unauthorized|forbidden/i.test(message);
    return {
      ...initial,
      queueState: expired ? "expired" : "error",
      message: expired
        ? "The server rejected this link; it may be expired or require browser authentication. FluxNet does not reuse browser credentials."
        : message,
    };
  }
  return { ...initial, queueState: "queued", message: `Download ${entry.status}.` };
}

function VideoDetectionPage({ defaultDownloadDirectory }: { defaultDownloadDirectory: string | null }) {
  const [bridge, setBridge] = useState<BrowserBridgeStatus | null>(null);
  const [resources, setResources] = useState<BrowserMediaResource[]>([]);
  const [feedback, setFeedback] = useState<{
    kind: "success" | "cancelled" | "error";
    message: string;
  } | null>(null);
  const [pendingUrl, setPendingUrl] = useState<string | null>(null);
  const reportFeedback = (message: string, kind: "success" | "cancelled" | "error" = "success") => {
    setFeedback({ kind, message });
  };

  useEffect(() => {
    let disposed = false;
    let stopStatus: (() => void) | undefined;
    let stopMedia: (() => void) | undefined;
    let stopDownloads: (() => void) | undefined;
    void invoke<BrowserBridgeStatus>("get_browser_bridge_status")
      .then((status) => {
        if (!disposed) setBridge(status);
      })
      .catch((statusError: unknown) => {
        if (!disposed) reportFeedback(`Browser bridge unavailable: ${statusError instanceof Error ? statusError.message : String(statusError)}`, "error");
      });
    void listen<BrowserBridgeStatus>("browser-bridge-status", (event) => {
      setBridge(event.payload);
    }).then((stop) => {
      if (disposed) stop();
      else stopStatus = stop;
    }).catch((listenError: unknown) => {
      reportFeedback(`Could not listen for browser connection status: ${listenError instanceof Error ? listenError.message : String(listenError)}`, "error");
    });
    void listen<BrowserMediaPayload>(
      "browser-media-detected",
      (event) => {
        const resource = event.payload;
        setResources((current) => {
          if (current.some((item) => item.url === resource.url)) return current;
          return [mapBrowserMediaResource(resource, []), ...current].slice(0, 100);
        });
        reportFeedback(resource.filename
          ? "A browser download handoff arrived. Choose a destination to add it to the FluxNet queue."
          : "A selected media link arrived from the extension. Review it before choosing a download destination.");
      },
    ).then((stop) => {
      if (disposed) stop();
      else {
        stopMedia = stop;
        void Promise.all([
          invoke<BrowserMediaPayload[]>("get_browser_media_resources"),
          invoke<DownloadEntry[]>("get_downloads"),
        ]).then(([mediaResources, downloads]) => {
          if (disposed) return;
          setResources((current) => {
            const existing = new Set(current.map((resource) => resource.url));
            const restored = mediaResources
              .filter((resource) => !existing.has(resource.url))
              .map((resource) => mapBrowserMediaResource(resource, downloads));
            return [...current, ...restored].slice(0, 100);
          });
        }).catch((snapshotError: unknown) => {
          reportFeedback(`Could not restore detected media: ${snapshotError instanceof Error ? snapshotError.message : String(snapshotError)}`, "error");
        });
      }
    }).catch((listenError: unknown) => {
      reportFeedback(`Could not receive media from the browser: ${listenError instanceof Error ? listenError.message : String(listenError)}`, "error");
    });
    void listen<DownloadEntry>("download-manager-update", (event) => {
      const entry = event.payload;
      setResources((current) => current.map((resource) => {
        if (resource.url !== entry.url) return resource;
        return mapBrowserMediaResource(resource, [entry]);
      }));
    }).then((stop) => {
      if (disposed) stop();
      else stopDownloads = stop;
    }).catch((listenError: unknown) => {
      reportFeedback(`Could not monitor queued browser downloads: ${listenError instanceof Error ? listenError.message : String(listenError)}`, "error");
    });
    return () => {
      disposed = true;
      stopStatus?.();
      stopMedia?.();
      stopDownloads?.();
    };
  }, []);

  const queueResource = useCallback(async (resource: BrowserMediaResource) => {
    setPendingUrl(resource.url);
    try {
      const filename = await invoke<string>("get_download_filename", { url: resource.url });
      const defaultPath = defaultDownloadDirectory
        ? `${defaultDownloadDirectory.replace(/[\\/]$/, "")}\\${filename}`
        : filename;
      const destination = await save({ title: "Save detected media", defaultPath });
      if (!destination) {
        reportFeedback("Canceled; no file was downloaded.", "cancelled");
        return;
      }
      await invoke<DownloadEntry>("start_download", { url: resource.url, destination });
      setResources((current) => current.map((item) => item.url === resource.url
        ? { ...item, queueState: "queued", message: "Added to the Download Manager." }
        : item));
      reportFeedback("Selected browser resource was added to the Download Manager.");
    } catch (queueError) {
      const message = queueError instanceof Error ? queueError.message : String(queueError);
      const expired = /\b(401|403|404)\b|expired|authentication|unauthorized|forbidden/i.test(message);
      setResources((current) => current.map((item) => item.url === resource.url
        ? { ...item, queueState: expired ? "expired" : "error", message }
        : item));
      reportFeedback(expired
        ? "The server rejected this link. It may have expired or require browser authentication; FluxNet does not reuse browser credentials."
        : `Could not queue this media: ${message}`, "error");
    } finally {
      setPendingUrl(null);
    }
  }, [defaultDownloadDirectory]);

  const statusLabel = bridge?.connectionStatus === "connected"
    ? `Connected · ${bridge.lastHandshakeAtMs ? `${Math.max(0, Math.floor((Date.now() - bridge.lastHandshakeAtMs) / 1000))}s ago` : "recent handshake"}`
    : bridge?.connectionStatus === "disconnected"
      ? "Disconnected"
      : "Awaiting connection";

  return (
    <div className="page video-page">
      <header className="page-header">
        <div>
          <div className="eyebrow">BROWSER INTEGRATION</div>
          <h1>Video Detection</h1>
          <p>Review automatically detected video and browser download resources. Nothing enters FluxNet's queue without your confirmation.</p>
        </div>
        <span className={`video-connection is-${bridge?.connectionStatus ?? "awaitingPairing"}`} role="status">
          <span />{statusLabel}
        </span>
      </header>

      <section className="panel video-setup-panel">
        <div className="panel-heading">
          <div>
            <div className="eyebrow">SECURE EXTENSION HANDOFF</div>
            <h2>Connect Chrome or Edge</h2>
          </div>
        </div>
        <ol className="video-setup-steps">
          <li>Build the native host with <code>cargo build --manifest-path src-tauri\Cargo.toml --release --bin fluxnet_native_host</code>.</li>
          <li>Load <code>browser-extension\</code> as an unpacked extension from <code>chrome://extensions</code> or <code>edge://extensions</code> with Developer mode enabled.</li>
          <li>Run <code>browser-extension\install-native-host.ps1 -HostExePath src-tauri\target\release\fluxnet_native_host.exe -Browser Both</code>.</li>
          <li>Keep FluxNet open; the extension connects automatically and retries when the app becomes available.</li>
        </ol>
        <p className="video-setup-note">Native Messaging is Windows-only in this version. The extension's stable ID is allowlisted by the per-user Native Messaging host; no pairing code is required. “Connected” means a valid Native Messaging request was received within the last 150 seconds; the host does not keep a persistent connection.</p>
      </section>

      {feedback && (
        <div className={`download-feedback is-${feedback.kind}`} role={feedback.kind === "error" ? "alert" : "status"}>
          {feedback.message}
        </div>
      )}

      <section className="panel video-resources-panel">
        <div className="panel-heading">
          <div>
            <div className="eyebrow">SELECTED BROWSER RESOURCES</div>
            <h2>Detected resources <span className="download-history-count">{resources.length}</span></h2>
          </div>
          <button className="secondary-button" type="button" onClick={() => {
            void invoke("clear_browser_media_resources")
              .then(() => {
                setResources([]);
                reportFeedback("Detected media list cleared.");
              })
              .catch((clearError: unknown) => reportFeedback(
                `Could not clear detected media: ${clearError instanceof Error ? clearError.message : String(clearError)}`,
                "error",
              ));
          }}>Clear list</button>
        </div>
        {resources.length === 0 ? (
          <div className="download-empty-state">
            <Icon name="video" size={24} />
            <strong>No resources sent yet</strong>
            <span>Browse normally; the extension detects supported video resources. Downloads start only after you choose them in the player overlay. Manual scanning remains available.</span>
          </div>
        ) : (
          <div className="video-resource-list">
            {resources.map((resource) => (
              <article className="video-resource" key={resource.url}>
                <div className="video-resource-heading">
                  <strong>{resource.pageTitle || resource.pageDomain || "Browser media"}</strong>
                  <span className={`video-resource-state is-${resource.queueState}`}>
                    {resource.queueState === "queued" ? "Queued" : resource.queueState === "completed" ? "Completed" : resource.queueState === "unsupported" ? "Unsupported manifest" : resource.queueState === "expired" ? "Expired / access required" : resource.queueState === "error" ? "Error" : "Detected"}
                  </span>
                </div>
                <code className="video-resource-url">{displayDownloadUrl(resource.url)}</code>
                <div className="video-resource-meta">
                  <span>{resource.mediaType.toUpperCase()}</span>
                  {resource.filename && <span>{resource.filename}</span>}
                  {resource.mimeType && <span>{resource.mimeType}</span>}
                  {resource.totalBytes !== null && resource.totalBytes !== undefined && <span>{formatDownloadBytes(resource.totalBytes)}</span>}
                  {resource.width && resource.height && <span>{resource.width} × {resource.height}</span>}
                  {resource.observedBytes !== null && <span>Observed transfer: {formatDownloadBytes(resource.observedBytes)}</span>}
                  <span>{new Date(resource.receivedAtMs).toLocaleString()}</span>
                </div>
                {resource.message && <p className="video-resource-message">{resource.message}</p>}
                {resource.queueState === "detected" && (
                  <button className="test-connection-button" type="button"
                    disabled={pendingUrl !== null}
                    onClick={() => void queueResource(resource)}>
                    <Icon name="download" size={15} />
                    {pendingUrl === resource.url ? "Opening save dialog…" : "Choose destination & queue"}
                  </button>
                )}
              </article>
            ))}
          </div>
        )}
      </section>
      <p className="video-caveat">HLS (.m3u8) and DASH (.mpd) manifests are shown as unsupported by the extension, not as complete files. No cookies, passwords, authorization headers, browser history, DRM, or login restrictions are bypassed. A listed resource can expire or require authentication; FluxNet downloads without browser credentials.</p>
    </div>
  );
}

function DownloadManagerPage({ defaultDownloadDirectory }: { defaultDownloadDirectory: string | null }) {
  const [url, setUrl] = useState("");
  const [entries, setEntries] = useState<DownloadEntry[]>([]);
  const [settings, setSettings] = useState<DownloadSettings>({
    maxConcurrent: 2,
    bandwidthLimitMbps: 0,
    notifyOnComplete: false,
    notifyOnFailure: false,
  });
  const [loading, setLoading] = useState(true);
  const [feedback, setFeedback] = useState<{ kind: "success" | "cancelled" | "error"; message: string } | null>(null);

  const refresh = useCallback(async () => {
    try {
      const [downloadEntries, downloadSettings] = await Promise.all([
        invoke<DownloadEntry[]>("get_downloads"),
        invoke<DownloadSettings>("get_download_settings"),
      ]);
      setEntries(downloadEntries);
      setSettings(downloadSettings);
      setFeedback(null);
    } catch (loadError) {
      setFeedback({
        kind: "error",
        message: loadError instanceof Error ? loadError.message : String(loadError),
      });
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    let disposed = false;
    let unlisten: (() => void) | undefined;
    void listen<DownloadEntry>("download-manager-update", (event) => {
      setEntries((current) => {
        const index = current.findIndex((entry) => entry.id === event.payload.id);
        if (index < 0) return [...current, event.payload];
        const next = [...current];
        if (next[index].updatedAtMs <= event.payload.updatedAtMs) next[index] = event.payload;
        return next;
      });
    }).then((stopListening) => {
      if (disposed) stopListening();
      else unlisten = stopListening;
    }).catch((listenError: unknown) => {
      setFeedback({
        kind: "error",
        message: `Could not subscribe to download progress: ${listenError instanceof Error ? listenError.message : String(listenError)}`,
      });
    });
    void refresh();
    return () => {
      disposed = true;
      unlisten?.();
    };
  }, [refresh]);

  const runAction = useCallback(async (action: () => Promise<unknown>, successMessage?: string) => {
    try {
      await action();
      if (successMessage) setFeedback({ kind: "success", message: successMessage });
      else setFeedback(null);
    } catch (actionError) {
      setFeedback({
        kind: "error",
        message: actionError instanceof Error ? actionError.message : String(actionError),
      });
    }
  }, []);

  const startDownload = useCallback(async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setFeedback(null);
    try {
      const filename = await invoke<string>("get_download_filename", { url: url.trim() });
      const defaultPath = defaultDownloadDirectory
        ? `${defaultDownloadDirectory.replace(/[\\/]$/, "")}\\${filename}`
        : filename;
      const destination = await save({ title: "Save downloaded file", defaultPath });
      if (!destination) {
        setFeedback({ kind: "cancelled", message: "Download canceled; no destination was selected." });
        return;
      }
      await invoke<DownloadEntry>("start_download", { url: url.trim(), destination });
      setUrl("");
      setFeedback({ kind: "success", message: "Download added to the queue." });
    } catch (startError) {
      setFeedback({
        kind: "error",
        message: startError instanceof Error ? startError.message : String(startError),
      });
    }
  }, [defaultDownloadDirectory, url]);

  const activeCount = entries.filter((entry) => (
    entry.status === "queued" || entry.status === "downloading"
  )).length;
  const completedCount = entries.filter((entry) => entry.status === "completed").length;
  const sortedEntries = [...entries].sort((a, b) => b.createdAtMs - a.createdAtMs);

  return (
    <div className="page download-page">
      <header className="page-header">
        <div>
          <div className="eyebrow">FILE TRANSFERS</div>
          <h1>Download Manager</h1>
          <p>Stream HTTP and HTTPS files to a location you choose. Downloads are never opened automatically.</p>
        </div>
        <button className="refresh-button" type="button" onClick={() => void refresh()} disabled={loading}>
          <Icon name="refresh" size={14} />
          {loading ? "Loading" : "Refresh"}
        </button>
      </header>

      <section className="panel download-add-panel">
        <div className="panel-heading">
          <div>
            <div className="eyebrow">NEW DOWNLOAD</div>
            <h2>Download a file</h2>
          </div>
          <span className="download-queue-count">{activeCount} active</span>
        </div>
        <form className="download-form" onSubmit={(event) => void startDownload(event)}>
          <label htmlFor="download-url">HTTP or HTTPS URL</label>
          <div className="download-form-row">
            <input
              id="download-url"
              type="url"
              inputMode="url"
              maxLength={8192}
              autoComplete="url"
              placeholder="https://example.com/file.zip"
              value={url}
              onChange={(event) => setUrl(event.currentTarget.value)}
              required
            />
            <button className="test-connection-button" type="submit" disabled={!url.trim()}>
              <Icon name="download" size={16} />
              Choose destination
            </button>
          </div>
          <p>FluxNet verifies TLS normally, follows HTTP redirects, and streams data to a partial file before committing it.</p>
        </form>
      </section>

      <section className="panel download-settings-panel">
        <div className="panel-heading">
          <div>
            <div className="eyebrow">RESOURCE LIMITS</div>
            <h2>Download settings</h2>
          </div>
        </div>
        <div className="download-settings">
          <label>
            Concurrent downloads
            <select
              value={settings.maxConcurrent}
              onChange={(event) => {
                const maxConcurrent = Number(event.currentTarget.value);
                void runAction(async () => {
                  await invoke("configure_download_limits", {
                    maxConcurrent,
                    bandwidthLimitMbps: settings.bandwidthLimitMbps,
                  });
                  setSettings((current) => ({ ...current, maxConcurrent }));
                });
              }}
            >
              {[1, 2, 3, 4].map((value) => <option key={value} value={value}>{value}</option>)}
            </select>
          </label>
          <label>
            Bandwidth limit
            <select
              value={settings.bandwidthLimitMbps}
              onChange={(event) => {
                const bandwidthLimitMbps = Number(event.currentTarget.value);
                void runAction(async () => {
                  await invoke("configure_download_limits", {
                    maxConcurrent: settings.maxConcurrent,
                    bandwidthLimitMbps,
                  });
                  setSettings((current) => ({ ...current, bandwidthLimitMbps }));
                });
              }}
            >
              {[0, 0.25, 0.5, 1, 2, 5, 10].map((value) => (
                <option key={value} value={value}>{value === 0 ? "Unlimited" : `${value} Mbps`}</option>
              ))}
            </select>
          </label>
          <label className="download-notification-toggle">
            <input
              type="checkbox"
              checked={settings.notifyOnComplete}
              onChange={(event) => {
                const notifyOnComplete = event.currentTarget.checked;
                void runAction(async () => {
                  await invoke("configure_download_notification_preferences", {
                    notifyOnComplete,
                    notifyOnFailure: settings.notifyOnFailure,
                  });
                  setSettings((current) => ({
                    ...current,
                    notifyOnComplete,
                  }));
                });
              }}
            />
            Completion notifications
          </label>
          <label className="download-notification-toggle">
            <input
              type="checkbox"
              checked={settings.notifyOnFailure}
              onChange={(event) => {
                const notifyOnFailure = event.currentTarget.checked;
                void runAction(async () => {
                  await invoke("configure_download_notification_preferences", {
                    notifyOnComplete: settings.notifyOnComplete,
                    notifyOnFailure,
                  });
                  setSettings((current) => ({
                    ...current,
                    notifyOnFailure,
                  }));
                });
              }}
            />
            Failure notifications
          </label>
        </div>
        <p className="download-settings-note">
          Bandwidth limits apply across active downloads. Notifications also depend on Windows notification settings.
        </p>
      </section>

      {feedback && (
        <div className={`download-feedback is-${feedback.kind}`} role={feedback.kind === "error" ? "alert" : "status"}>
          {feedback.message}
        </div>
      )}

      <section className="panel download-list-panel">
        <div className="panel-heading">
          <div>
            <div className="eyebrow">QUEUE AND HISTORY</div>
            <h2>Downloads <span className="download-history-count">{entries.length}</span></h2>
          </div>
          <button
            className="quality-export-button"
            type="button"
            disabled={completedCount === 0}
            onClick={() => void runAction(
              async () => {
                await invoke("clear_completed_downloads");
                setEntries((current) => current.filter((entry) => entry.status !== "completed"));
              },
              "Completed history cleared. Downloaded files were not deleted.",
            )}
          >
            Clear completed history
          </button>
        </div>
        {loading && entries.length === 0 ? (
          <div className="download-empty-state">Loading saved download history…</div>
        ) : sortedEntries.length === 0 ? (
          <div className="download-empty-state">
            <Icon name="download" size={24} />
            <strong>No downloads yet</strong>
            <span>Enter an HTTP or HTTPS URL above to add a file.</span>
          </div>
        ) : (
          <div className="download-list">
            {sortedEntries.map((entry) => {
              const percent = entry.totalBytes && entry.totalBytes > 0
                ? Math.min(100, (entry.downloadedBytes / entry.totalBytes) * 100)
                : null;
              return (
                <article className="download-entry" key={entry.id}>
                  <div className="download-entry-heading">
                    <div className="download-file-icon"><Icon name="download" size={18} /></div>
                    <div className="download-file-name">
                      <strong title={entry.destination}>{entry.filename}</strong>
                      <span title="Query parameters are hidden to protect URL tokens">{displayDownloadUrl(entry.url)}</span>
                    </div>
                    <span className={`download-status status-${entry.status}`}>{entry.status}</span>
                  </div>
                  <div className="download-progress-track" aria-label="Download progress">
                    <span style={{ width: `${percent ?? 0}%` }} />
                    {percent === null && entry.status === "downloading" && <i />}
                  </div>
                  <div className="download-details">
                    <span>{formatDownloadBytes(entry.downloadedBytes)} / {formatDownloadBytes(entry.totalBytes)}</span>
                    <span>{percent === null ? (entry.status === "downloading" ? "Progress unknown" : "—") : `${percent.toFixed(1)}%`}</span>
                    <span>{entry.status === "downloading" ? `${(entry.speedBytesPerSecond / 1_000_000).toFixed(2)} MB/s` : "—"}</span>
                    <span>Elapsed {formatDuration(entry.elapsedMs / 1000)}</span>
                    <span>Remaining {formatDuration(entry.remainingSeconds)}</span>
                  </div>
                  {entry.error && <div className="download-entry-error" role="alert">{entry.error}</div>}
                  <div className="download-entry-actions">
                    {entry.status === "downloading" && (
                      <button type="button" onClick={() => void runAction(() => invoke("pause_download", { id: entry.id }))}>Pause</button>
                    )}
                    {entry.status === "paused" && (
                      <button type="button" onClick={() => void runAction(() => invoke("resume_download", { id: entry.id }))}>Resume</button>
                    )}
                    {["queued", "downloading", "paused"].includes(entry.status) && (
                      <button type="button" onClick={() => void runAction(() => invoke("cancel_download", { id: entry.id }))}>Cancel</button>
                    )}
                    {(entry.status === "failed" || entry.status === "cancelled") && (
                      <>
                        <button type="button" onClick={() => void runAction(() => invoke("retry_download", { id: entry.id, restart: false }))}>Retry / Resume</button>
                        <button type="button" onClick={() => void runAction(() => invoke("retry_download", { id: entry.id, restart: true }))}>Restart</button>
                      </>
                    )}
                    {entry.status === "completed" && (
                      <button type="button" onClick={() => void runAction(() => invoke("open_download_containing_folder", { id: entry.id }))}>Open containing folder</button>
                    )}
                    <span>Attempt {entry.attempts} · {new Date(entry.updatedAtMs).toLocaleString()}</span>
                  </div>
                </article>
              );
            })}
          </div>
        )}
      </section>
    </div>
  );
}

function App() {
  const [activePage, setActivePage] = useState("Dashboard");
  const [appSettings, setAppSettings] = useState<AppSettings>(DEFAULT_APP_SETTINGS);
  const [appSettingsLoaded, setAppSettingsLoaded] = useState(false);
  const [settingsBootstrapError, setSettingsBootstrapError] = useState<string | null>(null);
  const [showTrayNotice, setShowTrayNotice] = useState(
    () => localStorage.getItem("fluxnet.tray-notice-dismissed") !== "true",
  );
  const [interfaces, setInterfaces] = useState<NetworkInterface[] | null>(null);
  const [networkTelemetry, setNetworkTelemetry] = useState<QualityMonitorUpdate | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [diagnosticHostname, setDiagnosticHostname] = useState("1.1.1.1");
  const [diagnostic, setDiagnostic] = useState<ConnectivityDiagnostic | null>(null);
  const [isDiagnosticLoading, setIsDiagnosticLoading] = useState(false);
  const [diagnosticError, setDiagnosticError] = useState<string | null>(null);
  const [bdixTarget, setBdixTarget] = useState("");
  const [bdixProbeCount, setBdixProbeCount] = useState(4);
  const [bdixTimeoutMs, setBdixTimeoutMs] = useState(1200);
  const [bdixResult, setBdixResult] = useState<BdixAnalysisResult | null>(null);
  const [isBdixRunning, setIsBdixRunning] = useState(false);
  const [bdixError, setBdixError] = useState<string | null>(null);
  const [bdixWasStopped, setBdixWasStopped] = useState(false);
  const [bdixServerList, setBdixServerList] = useState(loadSavedBdixServers);
  const [speedTestProvider, setSpeedTestProvider] = useState<"cloudflare" | "custom">("cloudflare");
  const [speedEndpoint, setSpeedEndpoint] = useState("");
  const [trustedEndpoint, setTrustedEndpoint] = useState(false);
  const [isSpeedTestRunning, setIsSpeedTestRunning] = useState(false);
  const [speedTestProgress, setSpeedTestProgress] = useState<SpeedTestProgress | null>(null);
  const [speedTestElapsedMs, setSpeedTestElapsedMs] = useState(0);
  const [speedTestResult, setSpeedTestResult] = useState<SpeedTestResult | null>(null);
  const [speedTestError, setSpeedTestError] = useState<string | null>(null);
  const [speedTestWasStopped, setSpeedTestWasStopped] = useState(false);
  const [browserHandoffError, setBrowserHandoffError] = useState<string | null>(null);
  const speedTestStartedAt = useRef<number | null>(null);
  const interfaceRefreshInFlight = useRef(false);
  const runBrowserHandoff = useRef(createBrowserHandoffRunner());

  useEffect(() => {
    let disposed = false;
    void invoke<AppSettings>("get_app_settings")
      .then((settings) => {
        if (disposed) return;
        setAppSettings(settings);
        setAppSettingsLoaded(true);
      })
      .catch((error: unknown) => {
        if (disposed) return;
        setSettingsBootstrapError(
          error instanceof Error ? error.message : String(error),
        );
        setAppSettingsLoaded(true);
      });
    return () => { disposed = true; };
  }, []);

  useEffect(() => {
    document.documentElement.dataset.theme = appSettings.theme;
  }, [appSettings.theme]);

  useEffect(() => {
    let disposed = false;
    let unlisten: (() => void) | undefined;
    void listen("navigate-to-download-manager", () => setActivePage("Download Manager"))
      .then((stopListening) => {
        if (disposed) stopListening();
        else unlisten = stopListening;
      })
      .catch((listenError: unknown) => {
        console.error("Could not listen for tray Download Manager navigation:", listenError);
      });
    return () => {
      disposed = true;
      unlisten?.();
    };
  }, []);

  useEffect(() => {
    let disposed = false;
    let unlisten: (() => void) | undefined;
    void listen<BrowserMediaPayload>("browser-download-handoff", async (event) => {
      const resource = event.payload;
      await runBrowserHandoff.current(resource.url, async () => {
        setBrowserHandoffError(null);
        setActivePage("Video Detection");
        try {
          const downloads = await invoke<DownloadEntry[]>("get_downloads");
          const existing = downloads.find((entry) => entry.url === resource.url
            && ["queued", "downloading", "paused", "completed"].includes(entry.status));
          if (existing) {
            setActivePage("Download Manager");
            return true;
          }
          const suggestedFilename = resource.filename
            || await invoke<string>("get_download_filename", { url: resource.url });
          const defaultPath = appSettings.defaultDownloadDirectory
            ? `${appSettings.defaultDownloadDirectory.replace(/[\\/]$/, "")}\\${suggestedFilename}`
            : suggestedFilename;
          const destination = await save({
            title: "Save browser download with FluxNet",
            defaultPath,
          });
          if (!destination) return false;
          await invoke<DownloadEntry>("start_download", { url: resource.url, destination });
          setActivePage("Download Manager");
          return true;
        } catch (handoffError) {
          const detail = handoffError instanceof Error ? handoffError.message : String(handoffError);
          console.error("Could not queue the accepted browser download:", handoffError);
          setBrowserHandoffError(`FluxNet could not queue this item (${detail}). Your original browser download continues.`);
          setActivePage("Video Detection");
          return false;
        }
      });
    }).then((stop) => {
      if (disposed) stop();
      else unlisten = stop;
    }).catch((listenError: unknown) => {
      console.error("Could not listen for accepted browser downloads:", listenError);
    });
    return () => {
      disposed = true;
      unlisten?.();
    };
  }, [appSettings.defaultDownloadDirectory]);

  const startBdixAnalysis = useCallback(async () => {
    setIsBdixRunning(true);
    setBdixResult(null);
    setBdixError(null);
    setBdixWasStopped(false);
    try {
      const result = await invoke<BdixAnalysisResult>("analyze_bdix_target", {
        hostname: bdixTarget,
        probeCount: bdixProbeCount,
        timeoutMs: bdixTimeoutMs,
      });
      setBdixResult(result);
    } catch (analysisFailure) {
      const message = analysisFailure instanceof Error ? analysisFailure.message : String(analysisFailure);
      if (message.includes("stopped by user")) {
        setBdixWasStopped(true);
      } else {
        setBdixError(message);
      }
    } finally {
      setIsBdixRunning(false);
    }
  }, [bdixProbeCount, bdixTarget, bdixTimeoutMs]);

  const stopBdixAnalysis = useCallback(async () => {
    try {
      await invoke("cancel_bdix_analysis");
      setBdixError(null);
    } catch (cancelFailure) {
      setBdixError(
        cancelFailure instanceof Error ? cancelFailure.message : String(cancelFailure),
      );
    }
  }, []);

  const clearBdixResults = useCallback(() => {
    setBdixResult(null);
    setBdixError(null);
    setBdixWasStopped(false);
  }, []);

  useEffect(() => {
    if (bdixServerList.error) return;
    try {
      localStorage.setItem("fluxnet.bdix-servers", JSON.stringify(bdixServerList.servers));
    } catch (storageFailure) {
      setBdixServerList((current) => ({
        ...current,
        error: `Could not save BDIX server entries: ${storageFailure instanceof Error ? storageFailure.message : String(storageFailure)}`,
      }));
    }
  }, [bdixServerList]);

  const refreshInterfaces = useCallback(async (showLoading = true) => {
    if (interfaceRefreshInFlight.current) return;
    interfaceRefreshInFlight.current = true;
    if (showLoading) setIsLoading(true);
    setError(null);
    try {
      const result = await invoke<NetworkInterface[]>("get_network_interfaces");
      setInterfaces(result);
    } catch (scanError) {
      setError(scanError instanceof Error ? scanError.message : String(scanError));
    } finally {
      if (showLoading) setIsLoading(false);
      interfaceRefreshInFlight.current = false;
    }
  }, []);

  const testConnection = useCallback(async () => {
    setIsDiagnosticLoading(true);
    setDiagnosticError(null);
    setDiagnostic(null);
    try {
      const result = await invoke<ConnectivityDiagnostic>("test_connectivity", {
        hostname: diagnosticHostname,
      });
      setDiagnostic(result);
    } catch (diagnosticFailure) {
      setDiagnosticError(
        diagnosticFailure instanceof Error ? diagnosticFailure.message : String(diagnosticFailure),
      );
    } finally {
      setIsDiagnosticLoading(false);
    }
  }, [diagnosticHostname]);

  const startSpeedTest = useCallback(async () => {
    speedTestStartedAt.current = Date.now();
    setIsSpeedTestRunning(true);
    setSpeedTestProgress(null);
    setSpeedTestElapsedMs(0);
    setSpeedTestResult(null);
    setSpeedTestError(null);
    setSpeedTestWasStopped(false);
    try {
      const result = await invoke<SpeedTestResult>("run_speed_test", {
        provider: speedTestProvider,
        endpoint: speedEndpoint,
      });
      setSpeedTestResult(result);
      setSpeedTestElapsedMs(result.elapsedMs);
    } catch (testFailure) {
      const message = testFailure instanceof Error ? testFailure.message : String(testFailure);
      if (message.includes("stopped by user")) {
        setSpeedTestWasStopped(true);
      } else {
        setSpeedTestError(message);
      }
    } finally {
      setIsSpeedTestRunning(false);
      speedTestStartedAt.current = null;
    }
  }, [speedEndpoint, speedTestProvider]);

  const stopSpeedTest = useCallback(async () => {
    try {
      await invoke("cancel_speed_test");
      setSpeedTestError(null);
    } catch (cancelFailure) {
      setSpeedTestError(
        cancelFailure instanceof Error ? cancelFailure.message : String(cancelFailure),
      );
    }
  }, []);

  const updateSpeedTestProgress = useCallback((progress: SpeedTestProgress) => {
    setSpeedTestProgress(progress);
    setSpeedTestElapsedMs(progress.elapsedMs);
  }, []);

  const reportProgressError = useCallback((message: string) => {
    setSpeedTestError(message);
  }, []);

  useEffect(() => {
    void refreshInterfaces();
  }, [refreshInterfaces]);

  useEffect(() => {
    let unlisten: (() => void) | undefined;
    let active = true;
    void listen<QualityMonitorUpdate>("network-monitor-update", (event) => {
      setNetworkTelemetry(event.payload);
      setInterfaces(event.payload.interfaces);
      setError(event.payload.interfaceError);
      setIsLoading(false);
    }).then((stopListening) => {
      if (active) unlisten = stopListening;
      else stopListening();
    }).catch((listenError: unknown) => {
      console.error("Could not subscribe to shared network telemetry:", listenError);
    });
    return () => {
      active = false;
      unlisten?.();
    };
  }, []);

  useEffect(() => {
    if (!isSpeedTestRunning) return;
    const timer = window.setInterval(() => {
      if (speedTestStartedAt.current !== null) {
        setSpeedTestElapsedMs(Date.now() - speedTestStartedAt.current);
      }
    }, 100);
    return () => window.clearInterval(timer);
  }, [isSpeedTestRunning]);

  const interfaceSummary = isLoading
    ? "Scanning Windows adapters"
    : error
      ? "Detection unavailable"
      : `${interfaces?.length ?? 0} interface${interfaces?.length === 1 ? "" : "s"} detected`;

  return (
    <div className="app-shell">
      <Sidebar
        activePage={activePage}
        onNavigate={setActivePage}
        interfaceSummary={interfaceSummary}
        navigationDisabled={isSpeedTestRunning || isBdixRunning}
      />
      <main className="main-content">
        {browserHandoffError && (
          <div className="download-feedback is-error" role="alert">
            {browserHandoffError}
            <button type="button" className="secondary-button" onClick={() => setBrowserHandoffError(null)}>Dismiss</button>
          </div>
        )}
        {showTrayNotice && (
          <div className="tray-first-use-notice" role="status">
            <div>
              <strong>FluxNet stays available in the system tray.</strong>
              <span>Closing this window hides FluxNet; it does not exit the app. Use the tray menu’s “Exit FluxNet” action to stop monitoring and quit.</span>
            </div>
            <button
              type="button"
              onClick={() => {
                localStorage.setItem("fluxnet.tray-notice-dismissed", "true");
                setShowTrayNotice(false);
              }}
            >
              Got it
            </button>
          </div>
        )}
        {activePage === "Dashboard" ? (
          <Dashboard
            onNavigate={setActivePage}
            interfaces={interfaces}
            isLoading={isLoading}
            error={error}
            onRefresh={() => void refreshInterfaces()}
            diagnosticHostname={diagnosticHostname}
            onDiagnosticHostnameChange={setDiagnosticHostname}
            diagnostic={diagnostic}
            isDiagnosticLoading={isDiagnosticLoading}
            diagnosticError={diagnosticError}
            onTestConnection={() => void testConnection()}
            telemetry={networkTelemetry}
          />
        ) : (
          activePage === "Network Interfaces" ? (
            <NetworkInterfacesPage
              interfaces={interfaces}
              isLoading={isLoading}
              error={error}
              onRefresh={() => void refreshInterfaces()}
            />
          ) : activePage === "BDIX Analysis" ? (
            <BdixAnalysisPage
              target={bdixTarget}
              onTargetChange={setBdixTarget}
              probeCount={bdixProbeCount}
              onProbeCountChange={setBdixProbeCount}
              timeoutMs={bdixTimeoutMs}
              onTimeoutChange={setBdixTimeoutMs}
              result={bdixResult}
              isRunning={isBdixRunning}
              error={bdixError}
              wasStopped={bdixWasStopped}
              onStart={() => void startBdixAnalysis()}
              onStop={() => void stopBdixAnalysis()}
              onClear={clearBdixResults}
              servers={bdixServerList.servers}
              onServersChange={(servers) => setBdixServerList({ servers, error: null })}
              storageError={bdixServerList.error}
              onStorageError={(storageError) => setBdixServerList((current) => ({ ...current, error: storageError }))}
            />
          ) : activePage === "Speed Test" ? (
            <SpeedTestPage
              provider={speedTestProvider}
              onProviderChange={(provider) => {
                setSpeedTestProvider(provider);
                setTrustedEndpoint(false);
                setSpeedTestResult(null);
                setSpeedTestError(null);
                setSpeedTestWasStopped(false);
              }}
              endpoint={speedEndpoint}
              onEndpointChange={setSpeedEndpoint}
              trustedEndpoint={trustedEndpoint}
              onTrustedEndpointChange={setTrustedEndpoint}
              isRunning={isSpeedTestRunning}
              progress={speedTestProgress}
              elapsedMs={speedTestElapsedMs}
              result={speedTestResult}
              error={speedTestError}
              wasStopped={speedTestWasStopped}
              onStart={() => void startSpeedTest()}
              onStop={() => void stopSpeedTest()}
              onProgress={updateSpeedTestProgress}
              onProgressError={reportProgressError}
            />
          ) : activePage === "Network Quality" ? (
            <NetworkQualityPage onAppSettingsChange={setAppSettings} />
          ) : activePage === "Download Manager" ? (
            <DownloadManagerPage defaultDownloadDirectory={appSettings.defaultDownloadDirectory} />
          ) : activePage === "Video Detection" ? (
            <VideoDetectionPage defaultDownloadDirectory={appSettings.defaultDownloadDirectory} />
          ) : activePage === "Settings" ? (
            <SettingsPage
              settings={appSettings}
              onSettingsChange={setAppSettings}
              settingsLoaded={appSettingsLoaded}
              loadError={settingsBootstrapError}
              onNavigate={setActivePage}
            />
          ) : (
            <ComingSoon page={activePage} />
          )
        )}
      </main>
    </div>
  );
}

export default App;
