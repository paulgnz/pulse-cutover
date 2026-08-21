//! `pulse-cutover doctor` — read-only environment survey.
//!
//! The 22 recorded ceremonies ran on boxes we built; real BPs have
//! heterogeneous setups. Doctor DETECTS rather than assumes: how nodeos runs
//! (native vs docker, which systemd unit), where the public /v1 actually
//! routes (the live nginx server_name -> proxy_pass map), what else is on the
//! box (legacy Hyperion, Elasticsearch, metalgo), and which ports the
//! ceremony needs are already spoken for. It emits BOTH a human table and
//! machine JSON (`--json`) — install.sh consumes the JSON to template the
//! flip scripts from the DETECTED layout instead of assuming one.
//!
//! Refusal philosophy: precise reasons, never guesses. A detectable-but-
//! exotic setup (caddy instead of nginx, nodeos under kubernetes) is
//! UNSUPPORTED-with-explanation, and the explanation points at
//! `pulse-cutover report` so we learn what to support next.
//!
//! Every probe is READ-ONLY: process listings, config dumps (`nginx -T`),
//! GET/POST get_info-class endpoints, `docker ps`/`inspect`, certificate
//! reads. Nothing is restarted, written, or flipped.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::Serialize;
use serde_json::{
    Value,
    json,
};

use crate::ops::run_shell;

// ---------------------------------------------------------------- model --

#[derive(Debug, Clone, Default, Serialize)]
pub struct HostInfo {
    pub os: String,
    pub os_version: String,
    pub os_supported: bool,
    pub kernel: String,
    pub arch: String,
    pub cpu_model: String,
    pub cpu_cores: u32,
    pub ram_mb: u64,
    pub disks: Vec<DiskInfo>,
    pub virtualization: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DiskInfo {
    pub mount: String,
    pub avail_gb: u64,
    pub total_gb: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct NodeosInfo {
    pub detected: bool,
    /// "native" | "docker" | "unknown" (+ host_kubelet flag below for k8s).
    pub runtime: String,
    pub pid: Option<u32>,
    pub binary: Option<String>,
    pub container_name: Option<String>,
    pub container_image: Option<String>,
    /// systemd units whose definition execs nodeos or docker-wraps the
    /// detected container.
    pub systemd_units: Vec<String>,
    pub chain_api_url: Option<String>,
    pub chain_id: Option<String>,
    pub head_block_num: Option<u64>,
    pub server_version_string: Option<String>,
    /// "enabled" | "enabled-restricted" | "disabled" | "unreachable"
    pub producer_api: String,
    pub state_history: Option<bool>,
    pub config_dir: Option<String>,
    /// kubelet runs on this host — nodeos may be kube-managed.
    pub host_kubelet: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct NginxRoute {
    pub file: String,
    pub server_names: Vec<String>,
    pub listens: Vec<String>,
    pub location: String,
    pub proxy_pass: String,
    /// Set when proxy_pass targets a named `upstream` block.
    pub upstream_name: Option<String>,
    /// Final backend addresses (the upstream's servers, or the proxy_pass
    /// authority itself).
    pub backends: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct TlsCert {
    pub server_names: Vec<String>,
    pub cert_path: String,
    pub not_after: Option<String>,
    pub expired: Option<bool>,
    pub expires_within_30d: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct WebInfo {
    /// "nginx" | "apache" | "caddy" | "none"
    pub server: String,
    pub version: Option<String>,
    pub routes: Vec<NginxRoute>,
    pub tls: Vec<TlsCert>,
    /// Upstream blocks seen in the dump: name -> servers.
    pub upstreams: BTreeMap<String, Vec<String>>,
    /// nginx detected but `nginx -T` failed (permissions?) — routes unknown.
    pub dump_failed: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct HyperionInfo {
    pub detected: bool,
    /// "pm2" | "systemd" | "unknown"
    pub manager: Option<String>,
    pub processes: Vec<String>,
    pub api_url: Option<String>,
    pub healthy: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct EsInfo {
    pub detected: bool,
    pub url: Option<String>,
    pub version: Option<String>,
    pub heap: Option<String>,
    pub in_docker: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct MetalgoInfo {
    pub binary_present: bool,
    pub binary_path: Option<String>,
    pub version: Option<String>,
    pub node_running: bool,
    pub plugins_dir: Option<String>,
    pub plugins: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PortInfo {
    pub port: u16,
    pub needed_by: String,
    pub in_use: bool,
    pub process: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Verdict {
    /// "READY" | "NEEDS" | "UNSUPPORTED"
    pub status: String,
    pub needs: Vec<String>,
    pub unsupported: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DoctorReport {
    pub schema: String,
    pub agent_version: String,
    pub generated_at: String,
    pub hostname: String,
    pub host: HostInfo,
    pub nodeos: NodeosInfo,
    pub web: WebInfo,
    pub hyperion_legacy: HyperionInfo,
    pub elasticsearch: EsInfo,
    pub metalgo: MetalgoInfo,
    pub docker_present: bool,
    pub systemd_present: bool,
    /// pulse-cutover's own services, if a previous install staged them.
    pub pulse_services: BTreeMap<String, String>,
    pub ports: Vec<PortInfo>,
    pub verdicts: BTreeMap<String, Verdict>,
}

// ------------------------------------------------------------- helpers --

/// Run a read-only shell probe; None on any failure. Probes must never
/// mutate — commands here are ps/df/ls/grep/systemctl show/docker ps class.
fn sh(cmd: &str) -> Option<String> {
    run_shell(cmd).ok().filter(|s| !s.is_empty())
}

fn http_json(url: &str, body: Option<Value>) -> Option<Value> {
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(4))
        .build();
    let resp = match body {
        Some(b) => agent.post(url).send_json(b),
        None => agent.get(url).call(),
    };
    resp.ok()?.into_json().ok()
}

/// POST that also reports the HTTP status on error (producer_api probe needs
/// to tell 404-not-enabled from 401-restricted).
fn http_post_status(url: &str) -> Result<Value, Option<u16>> {
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(4))
        .build();
    match agent.post(url).send_string("") {
        Ok(r) => r.into_json().map_err(|_| None),
        Err(ureq::Error::Status(code, _)) => Err(Some(code)),
        Err(_) => Err(None),
    }
}

// -------------------------------------------------------- pure parsers --

pub fn parse_os_release(text: &str) -> (String, String) {
    let field = |key: &str| {
        text.lines()
            .find(|l| l.starts_with(&format!("{key}=")))
            .map(|l| l[key.len() + 1..].trim_matches('"').to_string())
            .unwrap_or_default()
    };
    (field("NAME"), field("VERSION_ID"))
}

pub fn parse_meminfo_mb(text: &str) -> Option<u64> {
    let line = text.lines().find(|l| l.starts_with("MemTotal:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb / 1024)
}

/// One structural event in an nginx config stream.
enum NginxToken {
    /// `# configuration file <path>:` marker from `nginx -T`.
    File(String),
    /// `<header> {`
    Open(String),
    /// `}`
    Close,
    /// `<directive args...>;` (or an EOL-terminated fragment)
    Stmt(String),
}

/// Tokenize an `nginx -T` dump: braces and semicolons are structure, so
/// single-line blocks (`upstream x { server y; }`) parse identically to
/// pretty-printed ones.
fn nginx_tokens(dump: &str) -> Vec<NginxToken> {
    let mut tokens = Vec::new();
    for raw in dump.lines() {
        let line = raw.trim();
        if let Some(f) = line.strip_prefix("# configuration file ") {
            tokens.push(NginxToken::File(f.trim_end_matches(':').to_string()));
            continue;
        }
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let mut acc = String::new();
        for ch in line.chars() {
            match ch {
                '{' => {
                    tokens.push(NginxToken::Open(acc.trim().to_string()));
                    acc.clear();
                }
                '}' => {
                    if !acc.trim().is_empty() {
                        tokens.push(NginxToken::Stmt(acc.trim().to_string()));
                    }
                    acc.clear();
                    tokens.push(NginxToken::Close);
                }
                ';' => {
                    if !acc.trim().is_empty() {
                        tokens.push(NginxToken::Stmt(acc.trim().to_string()));
                    }
                    acc.clear();
                }
                _ => acc.push(ch),
            }
        }
        if !acc.trim().is_empty() {
            tokens.push(NginxToken::Stmt(acc.trim().to_string()));
        }
    }
    tokens
}

/// Parse `nginx -T` output into routes (server_name -> location ->
/// proxy_pass/backends), upstream blocks, and TLS certificate paths.
///
/// Exotic hand-minified configs degrade to fewer detected routes — never to
/// wrong ones, because every emitted route carries the exact `proxy_pass`
/// string install.sh will look for verbatim before templating a flip.
pub fn parse_nginx_dump(dump: &str) -> WebInfo {
    #[derive(Default, Clone)]
    struct ServerAcc {
        file: String,
        names: Vec<String>,
        listens: Vec<String>,
        certs: Vec<String>,
        locations: Vec<(String, String)>, // (path, proxy_pass)
    }
    enum Ctx {
        Other,
        Server(ServerAcc),
        Location(String),
        Upstream(String),
    }

    let mut web = WebInfo::default();
    let mut file = String::new();
    let mut stack: Vec<Ctx> = Vec::new();
    let mut servers: Vec<ServerAcc> = Vec::new();

    for token in nginx_tokens(dump) {
        match token {
            NginxToken::File(f) => file = f,
            NginxToken::Open(header) => {
                let mut words = header.split_whitespace();
                match words.next().unwrap_or("") {
                    "server" if header == "server" => {
                        // `server {` opens a server block only outside upstreams.
                        if matches!(stack.last(), Some(Ctx::Upstream(_))) {
                            stack.push(Ctx::Other);
                        } else {
                            stack.push(Ctx::Server(ServerAcc {
                                file: file.clone(),
                                ..Default::default()
                            }));
                        }
                    }
                    "location" => {
                        let path = words
                            .filter(|t| !matches!(*t, "=" | "~" | "~*" | "^~"))
                            .next()
                            .unwrap_or("/")
                            .to_string();
                        stack.push(Ctx::Location(path));
                    }
                    "upstream" => {
                        let name = words.next().unwrap_or("").to_string();
                        web.upstreams.entry(name.clone()).or_default();
                        stack.push(Ctx::Upstream(name));
                    }
                    _ => stack.push(Ctx::Other),
                }
            }
            NginxToken::Close => {
                if let Some(Ctx::Server(acc)) = stack.pop() {
                    servers.push(acc);
                }
            }
            NginxToken::Stmt(stmt) => {
                let mut words = stmt.split_whitespace();
                let directive = words.next().unwrap_or("").to_string();
                let args: Vec<String> = words.map(str::to_string).collect();
                // Find enclosing location / upstream context.
                let mut in_location: Option<String> = None;
                let mut in_upstream: Option<String> = None;
                for ctx in stack.iter().rev() {
                    match ctx {
                        Ctx::Location(p) if in_location.is_none() => {
                            in_location = Some(p.clone())
                        }
                        Ctx::Upstream(u) => {
                            in_upstream = Some(u.clone());
                            break;
                        }
                        Ctx::Server(_) => break,
                        _ => {}
                    }
                }
                if let Some(up) = in_upstream {
                    if directive == "server" {
                        if let Some(addr) = args.first() {
                            web.upstreams.entry(up).or_default().push(addr.clone());
                        }
                    }
                    continue;
                }
                // Attach to the innermost server block on the stack.
                let server = stack.iter_mut().rev().find_map(|c| match c {
                    Ctx::Server(acc) => Some(acc),
                    _ => None,
                });
                if let Some(acc) = server {
                    match directive.as_str() {
                        "server_name" => acc.names.extend(args),
                        "listen" => acc.listens.push(args.join(" ")),
                        "ssl_certificate" => acc.certs.extend(args),
                        "proxy_pass" => {
                            let loc = in_location.unwrap_or_else(|| "/".into());
                            if let Some(url) = args.first() {
                                acc.locations.push((loc, url.clone()));
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    for acc in servers {
        for (path, proxy_pass) in &acc.locations {
            let authority = proxy_pass
                .trim_start_matches("http://")
                .trim_start_matches("https://")
                .trim_end_matches('/')
                .to_string();
            let (upstream_name, backends) = match web.upstreams.get(&authority) {
                Some(servers) => (Some(authority.clone()), servers.clone()),
                None => (None, vec![authority.clone()]),
            };
            web.routes.push(NginxRoute {
                file: acc.file.clone(),
                server_names: acc.names.clone(),
                listens: acc.listens.clone(),
                location: path.clone(),
                proxy_pass: proxy_pass.clone(),
                upstream_name,
                backends,
            });
        }
        for cert in &acc.certs {
            if !web.tls.iter().any(|t| &t.cert_path == cert) {
                web.tls.push(TlsCert {
                    server_names: acc.names.clone(),
                    cert_path: cert.clone(),
                    ..Default::default()
                });
            }
        }
    }
    web
}

/// `ss -ltnpH` (or `ss -ltnp` minus header) -> (port, process) pairs.
pub fn parse_ss_listeners(text: &str) -> Vec<(u16, Option<String>)> {
    let mut out = Vec::new();
    for line in text.lines() {
        let cols: Vec<&str> = line.split_whitespace().collect();
        if cols.len() < 4 || cols[0] == "State" {
            continue;
        }
        let local = cols[3];
        let port: u16 = match local.rsplit(':').next().and_then(|p| p.parse().ok()) {
            Some(p) => p,
            None => continue,
        };
        let process = line
            .split("users:((\"")
            .nth(1)
            .and_then(|s| s.split('"').next())
            .map(str::to_string);
        out.push((port, process));
    }
    out
}

// ------------------------------------------------------------ verdicts --

const RAM_MIN_API_MB: u64 = 7_500;
const RAM_MIN_FULL_MB: u64 = 15_000;
const DISK_MIN_GB: u64 = 50;

pub fn compute_verdicts(r: &DoctorReport) -> BTreeMap<String, Verdict> {
    let mut out = BTreeMap::new();
    for mode in ["bp", "api", "hyperion"] {
        let mut needs: Vec<String> = Vec::new();
        let mut unsupported: Vec<String> = Vec::new();

        // OS gate: install.sh pins Ubuntu 22.04/24.04.
        if !r.host.os_supported {
            unsupported.push(format!(
                "OS is {} {} — install.sh currently supports Ubuntu 22.04/24.04 only; \
                 run `pulse-cutover report` and share the bundle so we learn what to support next",
                r.host.os, r.host.os_version
            ));
        }
        if !r.systemd_present {
            unsupported.push(
                "no systemd detected — the staged metalgo/gateway services are systemd units; \
                 run `pulse-cutover report` so we can scope an alternative"
                    .into(),
            );
        }

        // Resources.
        let ram_min = if mode == "api" { RAM_MIN_API_MB } else { RAM_MIN_FULL_MB };
        if r.host.ram_mb > 0 && r.host.ram_mb < ram_min {
            needs.push(format!(
                "{}GB+ RAM for {mode} mode (found {:.1}GB)",
                ram_min / 1000,
                r.host.ram_mb as f64 / 1024.0
            ));
        }
        let disk_ok = r.host.disks.iter().any(|d| d.avail_gb >= DISK_MIN_GB);
        if !r.host.disks.is_empty() && !disk_ok {
            needs.push(format!(
                "{DISK_MIN_GB}GB+ free disk (largest free: {}GB)",
                r.host.disks.iter().map(|d| d.avail_gb).max().unwrap_or(0)
            ));
        }

        // nodeos — the source side is the operator's own.
        if !r.nodeos.detected {
            if r.nodeos.host_kubelet {
                unsupported.push(
                    "kubelet is running and no native/docker nodeos was found — \
                     kubernetes-managed nodeos is not yet supported (the ceremony needs a \
                     stop/start command and a reachable producer_api); run `pulse-cutover report`"
                        .into(),
                );
            } else {
                needs.push(
                    "a running, synced nodeos (native or docker) serving /v1/chain/get_info"
                        .into(),
                );
            }
        } else {
            if r.nodeos.chain_api_url.is_none() {
                needs.push(
                    "nodeos process found but its chain API did not answer get_info \
                     (checked --http-server-address, docker port maps, and 127.0.0.1:8888)"
                        .into(),
                );
            }
            match r.nodeos.producer_api.as_str() {
                "enabled" => {}
                "enabled-restricted" => needs.push(
                    "producer_api answers but is access-restricted — the agent needs \
                     create_snapshot on localhost"
                        .into(),
                ),
                _ => needs.push(
                    "producer_api_plugin (localhost-bound) — required for create_snapshot; \
                     add `plugin = eosio::producer_api_plugin` (R10: keep it OFF the public edge)"
                        .into(),
                ),
            }
            if r.nodeos.runtime == "unknown" {
                needs.push(
                    "could not tell how nodeos is managed (no systemd unit, no docker \
                     container) — declare source.stop_cmd/start_cmd explicitly in the manifest"
                        .into(),
                );
            }
        }

        // api / hyperion: the public edge.
        if mode != "bp" {
            match r.web.server.as_str() {
                "nginx" | "none" => {} // none: install.sh installs nginx
                other => unsupported.push(format!(
                    "{other} is serving the public edge — the flip templater currently \
                     speaks nginx only; a {other} setup is detectable but not yet supported: \
                     run `pulse-cutover report` and share the bundle (flipping by hand is \
                     documented in the README meanwhile)"
                )),
            }
            if r.web.server == "nginx" && r.web.dump_failed {
                needs.push(
                    "nginx detected but `nginx -T` failed — doctor could not read the \
                     server_name -> upstream map (run as root?)"
                        .into(),
                );
            }
            if r.nodeos.detected
                && r.nodeos.runtime == "native"
                && r.nodeos.systemd_units.is_empty()
            {
                needs.push(
                    "api mode retires nodeos AFTER the flip: no systemd unit detected for \
                     the native nodeos, so source.stop_cmd must be declared in the manifest"
                        .into(),
                );
            }
        }

        // hyperion extras.
        if mode == "hyperion" {
            if !r.docker_present && !r.elasticsearch.detected {
                needs.push(
                    "docker (for the Elasticsearch container) — or a running Elasticsearch \
                     on :9200"
                        .into(),
                );
            }
        }

        // Port conflicts: a port the ceremony must bind, held by a foreign process.
        for p in &r.ports {
            if !p.in_use || !p.needed_by.contains(mode) {
                continue;
            }
            let owner = p.process.clone().unwrap_or_default();
            let expected = matches!(
                owner.as_str(),
                "metalgo" | "node" | "nginx" | "hyperion" | "java" | "docker-proxy"
            );
            if !expected {
                needs.push(format!(
                    "port {} is in use by `{}` — the {} needs it",
                    p.port,
                    if owner.is_empty() { "unknown" } else { &owner },
                    port_role(p.port)
                ));
            }
        }

        let status = if !unsupported.is_empty() {
            "UNSUPPORTED"
        } else if !needs.is_empty() {
            "NEEDS"
        } else {
            "READY"
        };
        out.insert(
            mode.to_string(),
            Verdict {
                status: status.into(),
                needs,
                unsupported,
            },
        );
    }
    out
}

fn port_role(port: u16) -> &'static str {
    match port {
        9650 => "metalgo HTTP API",
        9651 => "metalgo staking port",
        8899 => "PulseVM /v1 REST gateway",
        80 => "public nginx edge",
        443 => "public nginx TLS edge",
        9200 => "Elasticsearch",
        7000 => "hyperion-rs API",
        7010 => "federating history router",
        7019 => "legacy /v2 passthrough",
        _ => "ceremony",
    }
}

/// The ceremony's port plan, per mode (comma-joined into `needed_by`).
fn port_plan() -> Vec<(u16, &'static str)> {
    vec![
        (9650, "bp,api,hyperion"),
        (9651, "bp,api,hyperion"),
        (8899, "api,hyperion"),
        (80, "api,hyperion"),
        (9200, "hyperion"),
        (7000, "hyperion"),
        (7010, "hyperion"),
        (7019, "hyperion"),
    ]
}

// -------------------------------------------------------------- survey --

pub fn survey() -> DoctorReport {
    let mut r = DoctorReport {
        schema: "pulse-cutover-doctor-v1".into(),
        agent_version: env!("CARGO_PKG_VERSION").into(),
        generated_at: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        hostname: sh("hostname").unwrap_or_default(),
        ..Default::default()
    };

    // ---- host ----
    if let Some(osr) = sh("cat /etc/os-release 2>/dev/null") {
        let (name, version) = parse_os_release(&osr);
        r.host.os = name;
        r.host.os_version = version;
    } else if let Some(mac) = sh("sw_vers -productName 2>/dev/null") {
        r.host.os = mac;
        r.host.os_version = sh("sw_vers -productVersion 2>/dev/null").unwrap_or_default();
    }
    r.host.os_supported = r.host.os.to_lowercase().contains("ubuntu")
        && matches!(r.host.os_version.as_str(), "22.04" | "24.04");
    r.host.kernel = sh("uname -sr").unwrap_or_default();
    r.host.arch = sh("uname -m").unwrap_or_default();
    r.host.cpu_cores = sh("nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null")
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);
    r.host.cpu_model = sh("grep -m1 '^model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2-")
        .or_else(|| sh("sysctl -n machdep.cpu.brand_string 2>/dev/null"))
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    r.host.ram_mb = sh("cat /proc/meminfo 2>/dev/null")
        .and_then(|t| parse_meminfo_mb(&t))
        .or_else(|| {
            sh("sysctl -n hw.memsize 2>/dev/null")
                .and_then(|s| s.trim().parse::<u64>().ok())
                .map(|b| b / 1024 / 1024)
        })
        .unwrap_or(0);
    // Relevant mounts: / plus wherever the big artifacts land.
    for mount in ["/", "/var/lib", "/root", "/opt"] {
        if let Some(df) = sh(&format!("df -Pk {mount} 2>/dev/null | tail -1")) {
            let cols: Vec<&str> = df.split_whitespace().collect();
            if cols.len() >= 6 {
                let total_gb = cols[1].parse::<u64>().unwrap_or(0) / 1024 / 1024;
                let avail_gb = cols[3].parse::<u64>().unwrap_or(0) / 1024 / 1024;
                let mounted_on = cols[5].to_string();
                if !r.host.disks.iter().any(|d| d.mount == mounted_on) {
                    r.host.disks.push(DiskInfo {
                        mount: mounted_on,
                        avail_gb,
                        total_gb,
                    });
                }
            }
        }
    }
    r.host.virtualization =
        sh("systemd-detect-virt 2>/dev/null").unwrap_or_else(|| "unknown".into());
    r.systemd_present = sh("test -d /run/systemd/system && echo yes").is_some();
    r.docker_present = sh("command -v docker >/dev/null 2>&1 && docker info --format ok 2>/dev/null").is_some();

    // ---- nodeos ----
    survey_nodeos(&mut r);

    // ---- web server ----
    survey_web(&mut r);

    // ---- legacy hyperion + ES ----
    survey_hyperion_es(&mut r);

    // ---- metalgo / PulseVM ----
    survey_metalgo(&mut r);

    // ---- pulse-cutover's own staged services ----
    for unit in [
        "metalgo-pulse",
        "pulse-gateway",
        "hyperion-indexer",
        "hyperion-api",
        "hyperion-federator",
    ] {
        if let Some(state) = sh(&format!("systemctl is-active {unit} 2>/dev/null")) {
            r.pulse_services.insert(unit.into(), state);
        }
    }

    // ---- ports ----
    let listeners = sh("ss -ltnpH 2>/dev/null || ss -ltnp 2>/dev/null || netstat -ltnp 2>/dev/null")
        .map(|t| parse_ss_listeners(&t))
        .unwrap_or_default();
    for (port, needed_by) in port_plan() {
        let hit = listeners.iter().find(|(p, _)| *p == port);
        r.ports.push(PortInfo {
            port,
            needed_by: needed_by.into(),
            in_use: hit.is_some(),
            process: hit.and_then(|(_, proc)| proc.clone()),
        });
    }

    r.verdicts = compute_verdicts(&r);
    r
}

fn survey_nodeos(r: &mut DoctorReport) {
    let n = &mut r.nodeos;
    n.runtime = "unknown".into();
    n.host_kubelet = sh("ps axo comm= 2>/dev/null | grep -x kubelet").is_some();

    // Native process? A containerized nodeos ALSO shows up in host ps (its
    // cmdline is just `nodeos ...`), so the pid's cgroup decides: docker/
    // containerd/kubepods there means it is NOT native — the docker pass
    // below (or the kubelet flag) owns it.
    if let Some(ps) = sh("ps axo pid=,command= 2>/dev/null | grep -E '[n]odeos( |$)'") {
        let native_line = ps.lines().find(|l| !l.contains("docker")).filter(|l| {
            let pid = l.split_whitespace().next().unwrap_or("");
            sh(&format!(
                "grep -sqE 'docker|containerd|kubepods' /proc/{pid}/cgroup && echo containerized"
            ))
            .is_none()
        });
        if let Some(line) = native_line {
            let mut it = line.split_whitespace();
            n.pid = it.next().and_then(|p| p.parse().ok());
            n.binary = it.next().map(str::to_string);
            n.detected = true;
            n.runtime = "native".into();
            let cmdline = line.to_string();
            if let Some(cfg) = arg_value(&cmdline, "--config-dir") {
                n.config_dir = Some(cfg);
            }
            n.state_history = Some(
                cmdline.contains("state_history_plugin")
                    || n.config_dir
                        .as_ref()
                        .and_then(|d| sh(&format!("grep -s state_history_plugin {d}/config.ini")))
                        .is_some(),
            );
            if let Some(addr) = arg_value(&cmdline, "--http-server-address").or_else(|| {
                n.config_dir.as_ref().and_then(|d| {
                    sh(&format!(
                        "grep -s '^http-server-address' {d}/config.ini | cut -d= -f2"
                    ))
                    .map(|s| s.trim().to_string())
                })
            }) {
                n.chain_api_url = Some(normalize_api_url(&addr));
            }
        }
    }
    // Docker container?
    if !n.detected && r.docker_present {
        if let Some(rows) =
            sh("docker ps --no-trunc --format '{{.Names}}\t{{.Image}}\t{{.Command}}\t{{.Ports}}' 2>/dev/null")
        {
            for row in rows.lines() {
                let cols: Vec<&str> = row.split('\t').collect();
                if cols.len() < 3 {
                    continue;
                }
                let (name, image, command) = (cols[0], cols[1], cols[2]);
                if image.contains("nodeos")
                    || image.contains("leap")
                    || image.contains("antelope")
                    || command.contains("nodeos")
                    || name.contains("nodeos")
                {
                    n.detected = true;
                    n.runtime = "docker".into();
                    n.container_name = Some(name.to_string());
                    n.container_image = Some(image.to_string());
                    // Host port that maps to the container's chain API.
                    if let Some(ports) = cols.get(3) {
                        if let Some(host_port) = ports
                            .split(',')
                            .filter_map(|m| {
                                let m = m.trim();
                                let (host, _cont) = m.split_once("->")?;
                                host.rsplit(':').next()?.parse::<u16>().ok()
                            })
                            .find(|p| [8888u16, 8890, 8080].contains(p))
                        {
                            n.chain_api_url = Some(format!("http://127.0.0.1:{host_port}"));
                        }
                    }
                    if let Some(cmd) = sh(&format!(
                        "docker inspect --format '{{{{join .Config.Cmd \" \"}}}} {{{{join .Args \" \"}}}}' {name} 2>/dev/null"
                    )) {
                        n.state_history = Some(cmd.contains("state_history_plugin"));
                    }
                    break;
                }
            }
        }
    }

    // systemd units exec'ing nodeos, or docker-wrapping the container.
    if r.systemd_present {
        let mut patterns = vec!["nodeos".to_string()];
        if let Some(c) = &n.container_name {
            patterns.push(c.clone());
        }
        for pat in patterns {
            if let Some(files) = sh(&format!(
                "grep -RilE '{pat}' /etc/systemd/system --include='*.service' 2>/dev/null"
            )) {
                for f in files.lines() {
                    let unit = f
                        .rsplit('/')
                        .next()
                        .unwrap_or(f)
                        .trim_end_matches(".service")
                        .to_string();
                    // Only units that actually START it (ExecStart mentions it).
                    let execs = sh(&format!("grep -sE '^ExecStart' {f}")).unwrap_or_default();
                    if execs.contains("nodeos") || execs.contains("docker") {
                        if !n.systemd_units.contains(&unit) {
                            n.systemd_units.push(unit);
                        }
                    }
                }
            }
        }
    }

    // Chain API probe: detected address, else the default.
    let candidates: Vec<String> = n
        .chain_api_url
        .clone()
        .into_iter()
        .chain(["http://127.0.0.1:8888".to_string()])
        .collect();
    for base in candidates {
        if let Some(info) = http_json(&format!("{base}/v1/chain/get_info"), Some(json!({}))) {
            if let Some(chain_id) = info.get("chain_id").and_then(|v| v.as_str()) {
                n.detected = true; // an answering nodeos counts even if ps was opaque
                n.chain_api_url = Some(base.clone());
                n.chain_id = Some(chain_id.to_string());
                n.head_block_num = info.get("head_block_num").and_then(|v| v.as_u64());
                n.server_version_string = info
                    .get("server_version_string")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                break;
            }
        }
    }

    // producer_api probe (read-only: /v1/producer/paused).
    n.producer_api = match &n.chain_api_url {
        None => "unreachable".into(),
        Some(base) => match http_post_status(&format!("{base}/v1/producer/paused")) {
            Ok(v) if v.is_boolean() => "enabled".into(),
            Ok(_) => "enabled".into(),
            Err(Some(404)) => "disabled".into(),
            Err(Some(401)) | Err(Some(403)) => "enabled-restricted".into(),
            Err(_) => "unreachable".into(),
        },
    };
}

fn survey_web(r: &mut DoctorReport) {
    let running = |name: &str| sh(&format!("ps axo comm= 2>/dev/null | grep -x '{name}'")).is_some()
        || sh(&format!("pgrep -x {name} >/dev/null 2>&1 && echo yes")).is_some();
    r.web.server = if running("nginx") {
        "nginx".into()
    } else if running("caddy") {
        "caddy".into()
    } else if running("apache2") || running("httpd") {
        "apache".into()
    } else {
        "none".into()
    };
    if r.web.server == "nginx" {
        r.web.version = sh("nginx -v 2>&1").map(|s| s.replace("nginx version: ", ""));
        match sh("nginx -T 2>/dev/null") {
            Some(dump) => {
                let parsed = parse_nginx_dump(&dump);
                r.web.routes = parsed.routes;
                r.web.upstreams = parsed.upstreams;
                r.web.tls = parsed.tls;
                // Certificate expiry, read-only via openssl.
                for cert in &mut r.web.tls {
                    if let Some(end) = sh(&format!(
                        "openssl x509 -enddate -noout -in '{}' 2>/dev/null",
                        cert.cert_path
                    )) {
                        cert.not_after =
                            Some(end.trim_start_matches("notAfter=").to_string());
                        cert.expired = Some(
                            sh(&format!(
                                "openssl x509 -checkend 0 -noout -in '{}' >/dev/null 2>&1 && echo ok",
                                cert.cert_path
                            ))
                            .is_none(),
                        );
                        cert.expires_within_30d = Some(
                            sh(&format!(
                                "openssl x509 -checkend 2592000 -noout -in '{}' >/dev/null 2>&1 && echo ok",
                                cert.cert_path
                            ))
                            .is_none(),
                        );
                    }
                }
            }
            None => r.web.dump_failed = true,
        }
    }
}

fn survey_hyperion_es(r: &mut DoctorReport) {
    // Legacy (node.js) Hyperion under pm2?
    if let Some(jlist) = sh("pm2 jlist 2>/dev/null") {
        if let Ok(Value::Array(apps)) = serde_json::from_str::<Value>(&jlist) {
            for app in apps {
                if let Some(name) = app.get("name").and_then(|v| v.as_str()) {
                    let lower = name.to_lowercase();
                    if lower.contains("hyperion") || lower.contains("indexer") || lower.contains("api") {
                        r.hyperion_legacy.detected = true;
                        r.hyperion_legacy.manager = Some("pm2".into());
                        r.hyperion_legacy.processes.push(name.to_string());
                    }
                }
            }
        }
    }
    // ... or under systemd (incl. our own hyperion-rs units)?
    if !r.hyperion_legacy.detected {
        if let Some(units) = sh(
            "systemctl list-units --type=service --state=running --no-legend --plain 2>/dev/null \
             | awk '{print $1}' | grep -i hyperion",
        ) {
            r.hyperion_legacy.detected = true;
            r.hyperion_legacy.manager = Some("systemd".into());
            r.hyperion_legacy.processes =
                units.lines().map(|s| s.trim().to_string()).collect();
        }
    }
    for port in [7000u16, 7010] {
        let url = format!("http://127.0.0.1:{port}/v2/health");
        if let Some(health) = http_json(&url, None) {
            r.hyperion_legacy.detected = true;
            r.hyperion_legacy.api_url = Some(url);
            r.hyperion_legacy.healthy = Some(
                health
                    .get("health")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter().all(|s| {
                            matches!(
                                s.get("status").and_then(|v| v.as_str()),
                                Some("OK") | Some("Warning")
                            )
                        })
                    })
                    .unwrap_or(health.get("federation").is_some()),
            );
            break;
        }
    }

    // Elasticsearch.
    if let Some(root) = http_json("http://127.0.0.1:9200/", None) {
        r.elasticsearch.detected = true;
        r.elasticsearch.url = Some("http://127.0.0.1:9200".into());
        r.elasticsearch.version = root
            .pointer("/version/number")
            .and_then(|v| v.as_str())
            .map(str::to_string);
    }
    if let Some(ps) = sh("ps axo command= 2>/dev/null | grep -m1 '[e]lasticsearch'") {
        r.elasticsearch.detected = true;
        r.elasticsearch.heap = ps
            .split_whitespace()
            .find(|t| t.starts_with("-Xmx"))
            .map(|t| t.trim_start_matches("-Xmx").to_string());
    }
    if r.docker_present
        && sh("docker ps --format '{{.Names}}' 2>/dev/null | grep -Ei 'elast|pulse-es'").is_some()
    {
        r.elasticsearch.detected = true;
        r.elasticsearch.in_docker = true;
    }
}

fn survey_metalgo(r: &mut DoctorReport) {
    for path in ["/opt/metalgo/metalgo", "/usr/local/bin/metalgo"] {
        if sh(&format!("test -x {path} && echo yes")).is_some() {
            r.metalgo.binary_present = true;
            r.metalgo.binary_path = Some(path.into());
            break;
        }
    }
    if let Some(v) = http_json(
        "http://127.0.0.1:9650/ext/info",
        Some(json!({"jsonrpc":"2.0","id":1,"method":"info.getNodeVersion"})),
    ) {
        r.metalgo.node_running = true;
        r.metalgo.version = v
            .pointer("/result/version")
            .and_then(|x| x.as_str())
            .map(str::to_string);
    }
    for dir in ["/opt/pulsevm/plugins", "/root/.metalgo/plugins"] {
        if let Some(ls) = sh(&format!("ls -1 {dir} 2>/dev/null")) {
            r.metalgo.plugins_dir = Some(dir.into());
            r.metalgo.plugins = ls.lines().map(str::to_string).collect();
            break;
        }
    }
}

fn arg_value(cmdline: &str, flag: &str) -> Option<String> {
    let tokens: Vec<&str> = cmdline.split_whitespace().collect();
    for (i, t) in tokens.iter().enumerate() {
        if let Some(v) = t.strip_prefix(&format!("{flag}=")) {
            return Some(v.to_string());
        }
        if *t == flag {
            return tokens.get(i + 1).map(|s| s.to_string());
        }
    }
    None
}

fn normalize_api_url(addr: &str) -> String {
    let addr = addr.trim();
    if addr.starts_with("http") {
        return addr.to_string();
    }
    let addr = addr.replace("0.0.0.0", "127.0.0.1");
    format!("http://{addr}")
}

// ------------------------------------------------------- human output --

pub fn render_human(r: &DoctorReport) -> String {
    let mut o = String::new();
    let line = |o: &mut String, k: &str, v: &str| {
        o.push_str(&format!("  {k:<22} {v}\n"));
    };
    let opt = |v: &Option<String>| v.clone().unwrap_or_else(|| "-".into());

    o.push_str(&format!(
        "pulse-cutover doctor v{} — {} ({})\n\n",
        r.agent_version, r.hostname, r.generated_at
    ));
    o.push_str("HOST\n");
    line(&mut o, "os", &format!("{} {} ({}, {})", r.host.os, r.host.os_version, r.host.arch, r.host.kernel));
    line(&mut o, "cpu", &format!("{} x {}", r.host.cpu_cores, r.host.cpu_model));
    line(&mut o, "ram", &format!("{:.1} GB", r.host.ram_mb as f64 / 1024.0));
    for d in &r.host.disks {
        line(&mut o, &format!("disk {}", d.mount), &format!("{} GB free / {} GB", d.avail_gb, d.total_gb));
    }
    line(&mut o, "systemd / docker", &format!("{} / {}", yn(r.systemd_present), yn(r.docker_present)));

    o.push_str("\nNODEOS (source chain — yours, untouched)\n");
    if r.nodeos.detected {
        line(&mut o, "runtime", &match r.nodeos.runtime.as_str() {
            "docker" => format!(
                "docker container `{}` ({})",
                opt(&r.nodeos.container_name),
                opt(&r.nodeos.container_image)
            ),
            "native" => format!("native (pid {}, {})", r.nodeos.pid.unwrap_or(0), opt(&r.nodeos.binary)),
            _ => "answering on the API but process not identified".into(),
        });
        line(&mut o, "systemd unit(s)", &if r.nodeos.systemd_units.is_empty() { "-".into() } else { r.nodeos.systemd_units.join(", ") });
        line(&mut o, "chain api", &opt(&r.nodeos.chain_api_url));
        line(&mut o, "version", &opt(&r.nodeos.server_version_string));
        line(&mut o, "chain_id", &opt(&r.nodeos.chain_id));
        line(&mut o, "head", &r.nodeos.head_block_num.map(|h| h.to_string()).unwrap_or_else(|| "-".into()));
        line(&mut o, "producer_api", &r.nodeos.producer_api);
        line(&mut o, "state_history", &r.nodeos.state_history.map(yn2).unwrap_or_else(|| "unknown".into()));
    } else {
        line(&mut o, "detected", "NO");
        if r.nodeos.host_kubelet {
            line(&mut o, "note", "kubelet present — kube-managed nodeos is not yet supported");
        }
    }

    o.push_str(&format!("\nWEB EDGE ({})\n", r.web.server));
    if r.web.server == "nginx" {
        line(&mut o, "version", &opt(&r.web.version));
        if r.web.dump_failed {
            line(&mut o, "routes", "nginx -T failed — run doctor as root to map routes");
        }
        for route in &r.web.routes {
            let names = if route.server_names.is_empty() { "_".to_string() } else { route.server_names.join(" ") };
            line(
                &mut o,
                &format!("route {names}"),
                &format!(
                    "{} -> {}{}",
                    route.location,
                    route.backends.join(","),
                    route.upstream_name.as_ref().map(|u| format!(" (upstream {u})")).unwrap_or_default()
                ),
            );
        }
        for cert in &r.web.tls {
            let status = match (cert.expired, cert.expires_within_30d) {
                (Some(true), _) => " !! EXPIRED",
                (_, Some(true)) => " !! expires <30d",
                _ => "",
            };
            line(
                &mut o,
                &format!("tls {}", cert.server_names.join(" ")),
                &format!("{} (until {}){status}", cert.cert_path, opt(&cert.not_after)),
            );
        }
    }

    o.push_str("\nHISTORY STACK\n");
    line(&mut o, "hyperion", &if r.hyperion_legacy.detected {
        format!(
            "{} ({}) {}",
            r.hyperion_legacy.processes.join(", "),
            r.hyperion_legacy.manager.clone().unwrap_or_else(|| "?".into()),
            r.hyperion_legacy.api_url.clone().map(|u| format!("api {u}")).unwrap_or_default()
        )
    } else {
        "not detected".into()
    });
    line(&mut o, "elasticsearch", &if r.elasticsearch.detected {
        format!(
            "{}{}{}",
            r.elasticsearch.version.clone().map(|v| format!("v{v} ")).unwrap_or_default(),
            r.elasticsearch.heap.clone().map(|h| format!("heap {h} ")).unwrap_or_default(),
            if r.elasticsearch.in_docker { "(docker)" } else { "" }
        )
    } else {
        "not detected".into()
    });

    o.push_str("\nPULSEVM TARGET\n");
    line(&mut o, "metalgo binary", &if r.metalgo.binary_present { opt(&r.metalgo.binary_path) } else { "not installed (install.sh stages it)".into() });
    line(&mut o, "metalgo node", &if r.metalgo.node_running { format!("running {}", opt(&r.metalgo.version)) } else { "not running".into() });
    if let Some(dir) = &r.metalgo.plugins_dir {
        line(&mut o, "plugins", &format!("{dir}: {}", if r.metalgo.plugins.is_empty() { "-".into() } else { r.metalgo.plugins.join(", ") }));
    }
    for (unit, state) in &r.pulse_services {
        line(&mut o, &format!("service {unit}"), state);
    }

    o.push_str("\nPORTS (ceremony plan)\n");
    for p in &r.ports {
        line(
            &mut o,
            &format!("{} ({})", p.port, port_role(p.port)),
            &if p.in_use {
                format!("in use by {}", p.process.clone().unwrap_or_else(|| "?".into()))
            } else {
                "free".into()
            },
        );
    }

    o.push_str("\nVERDICTS\n");
    for mode in ["bp", "api", "hyperion"] {
        if let Some(v) = r.verdicts.get(mode) {
            o.push_str(&format!("  {mode:<9} {}\n", v.status));
            for n in &v.needs {
                o.push_str(&format!("            NEEDS: {n}\n"));
            }
            for u in &v.unsupported {
                o.push_str(&format!("            UNSUPPORTED: {u}\n"));
            }
        }
    }
    o
}

fn yn(b: bool) -> &'static str {
    if b { "yes" } else { "no" }
}
fn yn2(b: bool) -> String {
    yn(b).to_string()
}

// --------------------------------------------------------------- tests --

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn os_release_parses_ubuntu() {
        let text = "NAME=\"Ubuntu\"\nVERSION_ID=\"24.04\"\nPRETTY_NAME=\"Ubuntu 24.04.2 LTS\"\n";
        assert_eq!(parse_os_release(text), ("Ubuntu".into(), "24.04".into()));
    }

    #[test]
    fn meminfo_parses_mb() {
        assert_eq!(parse_meminfo_mb("MemTotal:       16265216 kB\n"), Some(15884));
    }

    #[test]
    fn nginx_dump_maps_domains_to_upstreams() {
        // A realistic operator layout: named upstream + two server blocks
        // (one TLS with domains, one default) + a location-level proxy_pass.
        let dump = r#"
# configuration file /etc/nginx/nginx.conf:
user www-data;
http {
    include /etc/nginx/conf.d/*.conf;
    include /etc/nginx/sites-enabled/*;
}
# configuration file /etc/nginx/conf.d/pulse-cutover-upstream.conf:
upstream pulse_v1_backend { server 127.0.0.1:8888; }
# configuration file /etc/nginx/sites-enabled/rpc:
server {
    listen 443 ssl;
    server_name rpc.example.com api.example.com;
    ssl_certificate /etc/letsencrypt/live/rpc.example.com/fullchain.pem;
    location /v1/chain/ {
        proxy_pass http://pulse_v1_backend;
    }
    location /v2/ {
        proxy_pass http://127.0.0.1:7000;
    }
}
# configuration file /etc/nginx/sites-enabled/default:
server {
    listen 80 default_server;
    server_name _;
    location / {
        proxy_pass http://127.0.0.1:8888;
    }
}
"#;
        let web = parse_nginx_dump(dump);
        assert_eq!(web.upstreams["pulse_v1_backend"], vec!["127.0.0.1:8888"]);
        assert_eq!(web.routes.len(), 3);

        let v1 = &web.routes[0];
        assert_eq!(v1.server_names, vec!["rpc.example.com", "api.example.com"]);
        assert_eq!(v1.location, "/v1/chain/");
        assert_eq!(v1.upstream_name.as_deref(), Some("pulse_v1_backend"));
        assert_eq!(v1.backends, vec!["127.0.0.1:8888"]);
        assert_eq!(v1.file, "/etc/nginx/sites-enabled/rpc");

        let v2 = &web.routes[1];
        assert_eq!(v2.location, "/v2/");
        assert_eq!(v2.upstream_name, None);
        assert_eq!(v2.backends, vec!["127.0.0.1:7000"]);

        let default = &web.routes[2];
        assert_eq!(default.server_names, vec!["_"]);
        assert_eq!(default.backends, vec!["127.0.0.1:8888"]);

        assert_eq!(web.tls.len(), 1);
        assert_eq!(web.tls[0].cert_path, "/etc/letsencrypt/live/rpc.example.com/fullchain.pem");
        assert_eq!(web.tls[0].server_names, vec!["rpc.example.com", "api.example.com"]);
    }

    #[test]
    fn nginx_dump_single_line_upstream_and_server_directive_disambiguation() {
        // `server` inside an upstream is a backend, not a block — the
        // single-line form `upstream x { server ...; }` must also parse.
        let dump = "# configuration file /etc/nginx/conf.d/up.conf:\n\
                    upstream gw {\n    server 127.0.0.1:8899;\n    server 127.0.0.1:8898 backup;\n}\n";
        let web = parse_nginx_dump(dump);
        assert_eq!(web.upstreams["gw"], vec!["127.0.0.1:8899", "127.0.0.1:8898"]);
        assert!(web.routes.is_empty());
    }

    #[test]
    fn ss_listener_lines_parse_ports_and_processes() {
        let text = r#"LISTEN 0      4096   127.0.0.1:8888  0.0.0.0:* users:(("nodeos",pid=1234,fd=51))
LISTEN 0      511    0.0.0.0:80      0.0.0.0:* users:(("nginx",pid=800,fd=6))
LISTEN 0      4096   *:9650          *:*"#;
        let got = parse_ss_listeners(text);
        assert_eq!(got.len(), 3);
        assert_eq!(got[0], (8888, Some("nodeos".into())));
        assert_eq!(got[1], (80, Some("nginx".into())));
        assert_eq!(got[2], (9650, None));
    }

    fn ready_report() -> DoctorReport {
        DoctorReport {
            host: HostInfo {
                os: "Ubuntu".into(),
                os_version: "24.04".into(),
                os_supported: true,
                ram_mb: 32_000,
                disks: vec![DiskInfo { mount: "/".into(), avail_gb: 200, total_gb: 300 }],
                ..Default::default()
            },
            nodeos: NodeosInfo {
                detected: true,
                runtime: "docker".into(),
                container_name: Some("nodeos".into()),
                chain_api_url: Some("http://127.0.0.1:8888".into()),
                chain_id: Some("71ee83bc".into()),
                producer_api: "enabled".into(),
                ..Default::default()
            },
            web: WebInfo { server: "nginx".into(), ..Default::default() },
            docker_present: true,
            systemd_present: true,
            ..Default::default()
        }
    }

    #[test]
    fn verdict_ready_when_everything_detected() {
        let v = compute_verdicts(&ready_report());
        assert_eq!(v["bp"].status, "READY");
        assert_eq!(v["api"].status, "READY");
        assert_eq!(v["hyperion"].status, "READY");
    }

    #[test]
    fn verdict_needs_lists_precise_gaps() {
        let mut r = ready_report();
        r.nodeos.producer_api = "disabled".into();
        r.host.ram_mb = 8_000;
        let v = compute_verdicts(&r);
        assert_eq!(v["api"].status, "NEEDS");
        assert!(v["api"].needs.iter().any(|n| n.contains("producer_api_plugin")));
        // 8GB clears api mode's RAM bar but not hyperion's.
        assert!(!v["api"].needs.iter().any(|n| n.contains("RAM")));
        assert!(v["hyperion"].needs.iter().any(|n| n.contains("RAM")));
    }

    #[test]
    fn verdict_unsupported_for_caddy_and_kubernetes_with_report_pointer() {
        let mut r = ready_report();
        r.web.server = "caddy".into();
        let v = compute_verdicts(&r);
        assert_eq!(v["api"].status, "UNSUPPORTED");
        assert!(v["api"].unsupported[0].contains("caddy"));
        assert!(v["api"].unsupported[0].contains("pulse-cutover report"));
        // bp mode has no public edge — caddy does not block it.
        assert_eq!(v["bp"].status, "READY");

        let mut r = ready_report();
        r.nodeos = NodeosInfo { host_kubelet: true, ..Default::default() };
        let v = compute_verdicts(&r);
        assert_eq!(v["bp"].status, "UNSUPPORTED");
        assert!(v["bp"].unsupported[0].contains("kubernetes"));
        assert!(v["bp"].unsupported[0].contains("pulse-cutover report"));
    }

    #[test]
    fn verdict_native_nodeos_without_unit_needs_explicit_stop_cmd() {
        let mut r = ready_report();
        r.nodeos.runtime = "native".into();
        r.nodeos.container_name = None;
        r.nodeos.systemd_units = vec![];
        let v = compute_verdicts(&r);
        assert_eq!(v["api"].status, "NEEDS");
        assert!(v["api"].needs.iter().any(|n| n.contains("stop_cmd")));
    }

    #[test]
    fn verdict_port_conflict_is_reported_with_role() {
        let mut r = ready_report();
        r.ports = vec![PortInfo {
            port: 9650,
            needed_by: "bp,api,hyperion".into(),
            in_use: true,
            process: Some("python3".into()),
        }];
        let v = compute_verdicts(&r);
        assert!(v["api"].needs.iter().any(|n| n.contains("9650") && n.contains("python3")));
        // ...but the staged metalgo itself holding 9650 is expected.
        r.ports[0].process = Some("metalgo".into());
        let v = compute_verdicts(&r);
        assert_eq!(v["api"].status, "READY");
    }

    #[test]
    fn arg_value_both_forms() {
        assert_eq!(
            arg_value("nodeos --config-dir=/etc/nodeos --plugin x", "--config-dir"),
            Some("/etc/nodeos".into())
        );
        assert_eq!(
            arg_value("nodeos --config-dir /etc/nodeos", "--config-dir"),
            Some("/etc/nodeos".into())
        );
        assert_eq!(arg_value("nodeos", "--config-dir"), None);
    }

    #[test]
    fn doctor_report_serializes_with_schema() {
        let r = ready_report();
        let mut r = r;
        r.schema = "pulse-cutover-doctor-v1".into();
        r.verdicts = compute_verdicts(&r);
        let json = serde_json::to_value(&r).unwrap();
        assert_eq!(json["schema"], "pulse-cutover-doctor-v1");
        assert_eq!(json["verdicts"]["api"]["status"], "READY");
        assert!(json["nodeos"]["runtime"].is_string());
    }
}
