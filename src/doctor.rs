//! `pulse-cutover doctor` — read-only environment survey.
//!
//! The 22 recorded ceremonies ran on boxes we built; real BPs have
//! heterogeneous setups. Doctor DETECTS rather than assumes: how nodeos runs
//! (native vs docker, which systemd unit), where the public /v1 actually
//! routes (the live nginx server_name -> proxy_pass map AND/OR the haproxy
//! frontend -> backend map), what else is on the box (legacy Hyperion,
//! Elasticsearch, metalgo), and which ports the ceremony needs are already
//! spoken for. It emits BOTH a human table and machine JSON (`--json`) —
//! install.sh consumes the JSON to template the flip scripts from the
//! DETECTED layout instead of assuming one.
//!
//! Refusal philosophy: precise reasons, never guesses. A detectable-but-
//! exotic setup (caddy instead of nginx/haproxy, nodeos under kubernetes) is
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
    // ---- script-managed detection (native nodeos with NO systemd unit —
    // the classic Antelope BP pattern: a start script under screen/tmux/
    // nohup/cron, config.ini at some /opt path, logs to stderr.txt) ----
    /// How the unit-less native nodeos appears to be managed, classified
    /// from the parent process chain: "screen" | "tmux" | "cron" |
    /// "shell script (<shell>)" | "orphaned (nohup/setsid — parent is init)".
    pub script_manager: Option<String>,
    /// comm names of the ancestor processes, nearest parent first, up to
    /// (and excluding) pid 1 — the raw evidence behind `script_manager`.
    pub parent_chain: Vec<String>,
    /// Full command line of the native nodeos process (the stop script
    /// install.sh generates matches THIS exact process).
    pub cmdline: Option<String>,
    /// --data-dir (or -d) from the command line.
    pub data_dir: Option<String>,
    /// Resolved config file: --config, or --config-dir + "/config.ini".
    pub config_path: Option<String>,
    /// Working directory of the process (relative --config-dir/--data-dir
    /// resolve against this).
    pub cwd: Option<String>,
    /// Where the process's stdout/stderr actually go, when they are regular
    /// files (tn1 pattern: a start script redirecting to stderr.txt).
    pub stdio_log: Option<String>,
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

/// One `server` line inside a haproxy backend (or listen) block.
#[derive(Debug, Clone, Default, Serialize)]
pub struct HaproxyServer {
    pub name: String,
    /// addr:port exactly as written (may be a hostname).
    pub addr: String,
    /// `check` flag present (health-checked by haproxy).
    pub check: bool,
    /// `backup` flag present (only takes traffic when the others are down —
    /// still counts as an active server for drain-strategy purposes).
    pub backup: bool,
    /// `disabled` flag present (starts in maintenance; takes no traffic).
    pub disabled: bool,
}

/// One public-surface -> backend edge in the haproxy config: a frontend (or
/// listen) block's use_backend/default_backend target, with the backend's
/// servers resolved — the same route-map shape doctor builds for nginx.
#[derive(Debug, Clone, Default, Serialize)]
pub struct HaproxyRoute {
    /// The frontend (or listen) block name.
    pub frontend: String,
    /// Every `bind` address in the block, as written (":443", "*:80", ...).
    pub binds: Vec<String>,
    /// Any bind in the block carries `ssl` (TLS terminates here).
    pub tls: bool,
    /// Why traffic reaches this backend: "default" (default_backend), or the
    /// use_backend condition with named ACLs expanded, e.g. "if path_beg /v1".
    pub rule: String,
    pub backend: String,
    /// The backend's `server` lines (empty if the backend block wasn't found).
    pub servers: Vec<HaproxyServer>,
}

/// A `stats socket` line from the global section. `level admin` on a UNIX
/// socket is what makes the zero-reload runtime flip possible.
#[derive(Debug, Clone, Default, Serialize)]
pub struct StatsSocket {
    pub path: String,
    pub level: Option<String>,
    pub admin: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct HaproxyInfo {
    /// Any signal at all: process, systemd unit, container, or config file.
    pub detected: bool,
    /// Actually running (process / active unit / container) — the verdicts
    /// only reason about a RUNNING haproxy.
    pub running: bool,
    pub version: Option<String>,
    /// "native" | "docker" | "unknown"
    pub runtime: String,
    pub systemd_unit: Option<String>,
    pub container_name: Option<String>,
    /// The config file as readable on THIS host.
    pub cfg_path: Option<String>,
    /// The `-f` path as the (containerized) process sees it, when that path
    /// is not readable on the host (install.sh validates through the
    /// container with this).
    pub container_cfg_path: Option<String>,
    /// Public surface -> backend servers (same shape as the nginx routes).
    pub routes: Vec<HaproxyRoute>,
    /// Backend name -> its server lines (includes `listen` blocks' own).
    pub backends: BTreeMap<String, Vec<HaproxyServer>>,
    pub stats_sockets: Vec<StatsSocket>,
    /// First admin-level UNIX stats socket — presence selects the preferred
    /// flip strategy (transactional enable/disable, zero reload).
    pub admin_socket: Option<String>,
    /// "socat" | "nc" if installed — what a socket flip would talk through.
    pub socket_tool: Option<String>,
    /// haproxy detected but no readable config — routes unknown.
    pub parse_failed: bool,
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
    pub haproxy: HaproxyInfo,
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

/// Parse a haproxy.cfg into the same route-map shape as the nginx parser:
/// public surface (frontend binds + path rules) -> backend server lines.
///
/// haproxy configs are line-oriented: a section keyword in the first token
/// (`global`, `frontend x`, `backend x`, `listen x`, ...) opens a section and
/// everything until the next keyword belongs to it — indentation is
/// conventional, not structural, so this parser keys on first tokens only.
/// Exotic configs degrade to fewer detected routes, never wrong ones: every
/// emitted route carries the backend and server names install.sh will look
/// for verbatim before templating a flip.
pub fn parse_haproxy_cfg(text: &str) -> HaproxyInfo {
    #[derive(Default)]
    struct Fe {
        name: String,
        binds: Vec<String>,
        tls: bool,
        /// acl <name> <criterion...> definitions, for expanding use_backend rules.
        acls: Vec<(String, String)>,
        /// (backend, raw condition) in declaration order — haproxy evaluates
        /// use_backend rules first-match-wins, then default_backend.
        uses: Vec<(String, String)>,
        default_backend: Option<String>,
        /// `listen` blocks carry their own server lines (frontend + backend
        /// fused); they become an implicit backend of the same name.
        servers: Vec<HaproxyServer>,
    }
    enum Sect {
        Global,
        Fe(usize),
        Be(String),
        Other,
    }

    let mut hap = HaproxyInfo::default();
    let mut fes: Vec<Fe> = Vec::new();
    let mut sect = Sect::Other;

    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let tokens: Vec<&str> = line.split_whitespace().collect();
        let first = tokens[0];
        // Section openers.
        match first {
            "global" => {
                sect = Sect::Global;
                continue;
            }
            "defaults" | "resolvers" | "peers" | "userlist" | "mailers" | "program" | "ring"
            | "cache" | "http-errors" | "fcgi-app" => {
                sect = Sect::Other;
                continue;
            }
            "frontend" | "listen" => {
                fes.push(Fe {
                    name: tokens.get(1).unwrap_or(&"").to_string(),
                    ..Default::default()
                });
                sect = Sect::Fe(fes.len() - 1);
                continue;
            }
            "backend" => {
                let name = tokens.get(1).unwrap_or(&"").to_string();
                hap.backends.entry(name.clone()).or_default();
                sect = Sect::Be(name);
                continue;
            }
            _ => {}
        }
        // Directives inside the current section.
        match &sect {
            Sect::Global => {
                // stats socket <path> [mode ...] [level admin|operator|user] ...
                if first == "stats" && tokens.get(1) == Some(&"socket") {
                    let path = tokens.get(2).unwrap_or(&"").to_string();
                    let level = tokens
                        .iter()
                        .position(|t| *t == "level")
                        .and_then(|i| tokens.get(i + 1))
                        .map(|s| s.to_string());
                    let admin = level.as_deref() == Some("admin");
                    // Only a UNIX-path admin socket is usable for the flip
                    // (ipv4@/ipv6@ sockets exist but we don't drive those).
                    if admin && path.starts_with('/') && hap.admin_socket.is_none() {
                        hap.admin_socket = Some(path.clone());
                    }
                    hap.stats_sockets.push(StatsSocket { path, level, admin });
                }
            }
            Sect::Fe(i) => {
                let fe = &mut fes[*i];
                match first {
                    "bind" => {
                        if let Some(addr) = tokens.get(1) {
                            fe.binds.push(addr.to_string());
                        }
                        if tokens.contains(&"ssl") {
                            fe.tls = true;
                        }
                    }
                    "acl" if tokens.len() >= 3 => {
                        fe.acls.push((tokens[1].to_string(), tokens[2..].join(" ")));
                    }
                    "default_backend" => {
                        fe.default_backend = tokens.get(1).map(|s| s.to_string());
                    }
                    "use_backend" => {
                        if let Some(b) = tokens.get(1) {
                            fe.uses.push((b.to_string(), tokens[2..].join(" ")));
                        }
                    }
                    "server" => {
                        if let Some(s) = parse_haproxy_server(&tokens) {
                            fe.servers.push(s);
                        }
                    }
                    _ => {}
                }
            }
            Sect::Be(name) => {
                if first == "server" {
                    if let Some(s) = parse_haproxy_server(&tokens) {
                        hap.backends.entry(name.clone()).or_default().push(s);
                    }
                }
            }
            Sect::Other => {}
        }
    }

    // listen blocks: their own servers form an implicit backend of the same name.
    for fe in &fes {
        if !fe.servers.is_empty() {
            hap.backends
                .entry(fe.name.clone())
                .or_insert_with(|| fe.servers.clone());
        }
    }

    // Emit routes in evaluation order: use_backend rules, then default_backend
    // (or the listen block's own servers).
    for fe in &fes {
        // Expand named ACLs so the rule is readable standalone:
        // "if is_v1" -> "if path_beg /v1". Anonymous ACLs lose their braces.
        let expand = |cond: &str| -> String {
            cond.split_whitespace()
                .filter(|t| *t != "{" && *t != "}")
                .map(|t| {
                    let (neg, base) = match t.strip_prefix('!') {
                        Some(b) => ("!", b),
                        None => ("", t),
                    };
                    match fe.acls.iter().find(|(n, _)| n == base) {
                        Some((_, def)) => format!("{neg}{def}"),
                        None => t.to_string(),
                    }
                })
                .collect::<Vec<_>>()
                .join(" ")
        };
        let mut targets: Vec<(String, String)> = fe
            .uses
            .iter()
            .map(|(b, c)| {
                let rule = if c.trim().is_empty() { "always".into() } else { expand(c) };
                (b.clone(), rule)
            })
            .collect();
        if let Some(d) = &fe.default_backend {
            targets.push((d.clone(), "default".into()));
        } else if !fe.servers.is_empty() {
            // A `listen` block's own servers ARE its default backend.
            targets.push((fe.name.clone(), "default".into()));
        }
        for (backend, rule) in targets {
            hap.routes.push(HaproxyRoute {
                frontend: fe.name.clone(),
                binds: fe.binds.clone(),
                tls: fe.tls,
                rule,
                backend: backend.clone(),
                servers: hap.backends.get(&backend).cloned().unwrap_or_default(),
            });
        }
    }
    hap
}

/// "server <name> <addr[:port]> [check] [backup] [disabled] ..." -> parsed.
fn parse_haproxy_server(tokens: &[&str]) -> Option<HaproxyServer> {
    Some(HaproxyServer {
        name: tokens.get(1)?.to_string(),
        addr: tokens.get(2)?.to_string(),
        check: tokens.contains(&"check"),
        backup: tokens.contains(&"backup"),
        disabled: tokens.contains(&"disabled"),
    })
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

/// Classify how a unit-less native nodeos is managed from the comm names of
/// its ancestor processes (nearest parent first, pid 1 excluded).
///
/// The classic Antelope BP patterns, in the order we discriminate them:
/// a screen/tmux session, a cron job, a plain shell script still holding the
/// child, or an orphan re-parented to init (nohup/setsid/`script.sh &` +
/// logout). The raw chain is always reported next to the classification so
/// an operator can dispute it.
pub fn classify_parent_chain(chain: &[String]) -> Option<String> {
    let lower: Vec<String> = chain.iter().map(|c| c.to_lowercase()).collect();
    if lower.iter().any(|c| c.contains("screen")) {
        return Some("screen".into());
    }
    if lower.iter().any(|c| c.starts_with("tmux")) {
        return Some("tmux".into());
    }
    if lower.iter().any(|c| c.contains("cron")) {
        return Some("cron".into());
    }
    match lower.first().map(String::as_str) {
        // Direct child of init: the launcher (nohup ./start.sh &, setsid,
        // an rc.local one-shot, an exited wrapper script) is gone.
        None => Some("orphaned (nohup/setsid — parent is init; started by a script that has exited)".into()),
        Some(shell) if matches!(shell, "bash" | "sh" | "dash" | "zsh" | "ksh") => {
            Some(format!("shell script ({})", chain[0]))
        }
        Some(other) if matches!(other, "sudo" | "su" | "sshd" | "login") => {
            Some(format!("interactive session ({})", chain[0]))
        }
        Some(_) => Some(format!("under `{}`", chain[0])),
    }
}

/// Resolve a possibly-relative path from a process command line against the
/// process's working directory.
pub fn resolve_against_cwd(path: &str, cwd: Option<&str>) -> String {
    if path.starts_with('/') {
        return path.to_string();
    }
    match cwd {
        Some(dir) => format!("{}/{path}", dir.trim_end_matches('/')),
        None => path.to_string(),
    }
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

        // OS gate: install.sh pins Ubuntu 20.04/22.04/24.04.
        if !r.host.os_supported {
            unsupported.push(format!(
                "OS is {} {} — install.sh currently supports Ubuntu 20.04/22.04/24.04 only; \
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
                "nginx" | "none" => {} // none: haproxy may be the edge, else install.sh installs nginx
                other => unsupported.push(format!(
                    "{other} is serving the public edge — the flip templater currently \
                     speaks nginx and haproxy only; a {other} setup is detectable but not yet \
                     supported: run `pulse-cutover report` and share the bundle (flipping by \
                     hand is documented in the README meanwhile)"
                )),
            }
            if r.web.server == "nginx" && r.web.dump_failed {
                needs.push(
                    "nginx detected but `nginx -T` failed — doctor could not read the \
                     server_name -> upstream map (run as root?)"
                        .into(),
                );
            }
            // haproxy edge — parsed like nginx; flipped via the runtime socket
            // (preferred) or cfg-edit + graceful reload.
            if r.haproxy.running {
                if r.haproxy.parse_failed {
                    needs.push(
                        "haproxy is running but doctor could not read its config (checked \
                         the process's -f path and /etc/haproxy/haproxy.cfg) — run doctor \
                         as root, or run `pulse-cutover report` so we can support this layout"
                            .into(),
                    );
                }
                // Two live edges: doctor cannot know which one the public URL
                // reaches — the operator declares it in the manifest.
                if r.web.server == "nginx"
                    && !r.web.routes.is_empty()
                    && !r.haproxy.routes.is_empty()
                {
                    needs.push(
                        "two web edges detected (nginx AND haproxy are both running with \
                         routes) — set flip.edge = \"nginx\" or \"haproxy\" in the manifest \
                         so the ceremony flips the edge your users actually reach"
                            .into(),
                    );
                }
                // Multi-server backends fronting the source nodeos: BPs often
                // balance several nodeos boxes in one backend, but a
                // single-box ceremony flips only THIS box's server entry —
                // proceeding silently would leave the others serving the old
                // chain. The operator must decide the drain strategy first.
                let nodeos_port = r
                    .nodeos
                    .chain_api_url
                    .as_deref()
                    .and_then(|u| u.rsplit(':').next())
                    .unwrap_or("8888");
                let suffix = format!(":{nodeos_port}");
                let mut flagged: Vec<&str> = Vec::new();
                for route in &r.haproxy.routes {
                    let active = route.servers.iter().filter(|s| !s.disabled).count();
                    if active > 1
                        && route.servers.iter().any(|s| s.addr.ends_with(&suffix))
                        && !flagged.contains(&route.backend.as_str())
                    {
                        flagged.push(&route.backend);
                        needs.push(format!(
                            "haproxy backend '{}' balances {} active servers — a \
                             single-box ceremony flips only THIS box's backend entry, so \
                             decide the drain strategy first: mark the servers that must \
                             not take post-cut traffic `disabled`, or coordinate a fleet \
                             flip (see the README's HAProxy notes)",
                            route.backend, active
                        ));
                    }
                }
                // The zero-reload flip drives the admin socket through socat.
                if r.haproxy.admin_socket.is_some() && r.haproxy.socket_tool.is_none() {
                    needs.push(
                        "socat — haproxy exposes an admin-level stats socket, so the flip \
                         can be a zero-reload socket command; apt-get install -y socat \
                         (install.sh installs it automatically when it picks this strategy)"
                            .into(),
                    );
                }
            }
            if r.nodeos.detected
                && r.nodeos.runtime == "native"
                && r.nodeos.systemd_units.is_empty()
            {
                match &r.nodeos.script_manager {
                    // Script-managed: the STOP side is derivable (graceful
                    // SIGTERM to the exact pid — install.sh generates it);
                    // only the START side needs the operator's own script,
                    // and only for abort rollback.
                    Some(mgr) => needs.push(format!(
                        "source.start_cmd in the manifest — nodeos is script-managed \
                         (parent: {mgr}): install.sh derives a graceful stop (SIGTERM to the \
                         exact detected process + wait-for-exit), but only YOUR start script \
                         can bring nodeos back if the ceremony aborts after the stop"
                    )),
                    None => needs.push(
                        "api mode retires nodeos AFTER the flip: no systemd unit detected for \
                         the native nodeos, so source.stop_cmd must be declared in the manifest"
                            .into(),
                    ),
                }
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
                "metalgo" | "node" | "nginx" | "haproxy" | "hyperion" | "java" | "docker-proxy"
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
        && matches!(r.host.os_version.as_str(), "20.04" | "22.04" | "24.04");
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

    // ---- haproxy (may coexist with nginx — both are reported) ----
    survey_haproxy(&mut r);

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
            // Everything after the pid column is the command line the stop
            // script will have to re-identify this process by.
            n.cmdline = cmdline
                .split_once(char::is_whitespace)
                .map(|(_, rest)| rest.trim().to_string());
            n.cwd = n
                .pid
                .and_then(|pid| sh(&format!("readlink /proc/{pid}/cwd 2>/dev/null")))
                .map(|s| s.trim().to_string());
            if let Some(cfg) = arg_value(&cmdline, "--config-dir") {
                n.config_dir = Some(cfg);
            }
            n.config_path = arg_value(&cmdline, "--config")
                .or_else(|| n.config_dir.as_ref().map(|d| format!("{}/config.ini", d.trim_end_matches('/'))))
                .map(|p| resolve_against_cwd(&p, n.cwd.as_deref()));
            n.data_dir = arg_value(&cmdline, "--data-dir")
                .or_else(|| arg_value(&cmdline, "-d"))
                .map(|p| resolve_against_cwd(&p, n.cwd.as_deref()));
            // Where do this process's stdout/stderr land? A script-managed
            // nodeos typically redirects them to a file (tn1: stderr.txt).
            if let Some(pid) = n.pid {
                n.stdio_log = ["2", "1"].iter().find_map(|fd| {
                    sh(&format!("readlink /proc/{pid}/fd/{fd} 2>/dev/null"))
                        .map(|s| s.trim().to_string())
                        .filter(|t| t.starts_with('/') && !t.starts_with("/dev/"))
                });
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

    // Script-managed detection: a native nodeos with NO systemd unit is the
    // classic Antelope BP pattern (started by a script under screen/tmux/
    // nohup/cron). Walk the parent chain and classify HOW it is managed so
    // the report says "script-managed (parent: screen)" instead of just "-".
    if n.runtime == "native" && n.systemd_units.is_empty() {
        if let Some(pid) = n.pid {
            let mut cur = pid;
            for _ in 0..12 {
                let ppid: u32 = match sh(&format!("ps -o ppid= -p {cur} 2>/dev/null"))
                    .and_then(|s| s.trim().parse().ok())
                {
                    Some(p) => p,
                    None => break,
                };
                if ppid <= 1 {
                    break;
                }
                if let Some(comm) = sh(&format!("ps -o comm= -p {ppid} 2>/dev/null")) {
                    n.parent_chain.push(comm.trim().to_string());
                }
                cur = ppid;
            }
        }
        n.script_manager = classify_parent_chain(&n.parent_chain);
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

fn survey_haproxy(r: &mut DoctorReport) {
    let mut hap = HaproxyInfo {
        runtime: "unknown".into(),
        ..Default::default()
    };

    // Process. Match on the comm field (executable name), NOT a substring of
    // the full command line — an ancestor shell whose command line merely
    // mentions "haproxy" (e.g. `doctor --json | jq .haproxy`) must not count.
    // A containerized haproxy ALSO shows in host ps — the pid's cgroup
    // decides whether it's native (same trick as the nodeos survey).
    let ps = sh("ps axo pid=,comm=,args= 2>/dev/null | awk '$2==\"haproxy\"'");
    let mut native = false;
    if let Some(t) = &ps {
        hap.running = true;
        native = t.lines().any(|l| {
            let pid = l.split_whitespace().next().unwrap_or("");
            sh(&format!(
                "grep -sqE 'docker|containerd|kubepods' /proc/{pid}/cgroup && echo containerized"
            ))
            .is_none()
        });
    }
    // The config path the process was started with (-f).
    let cmdline_cfg = ps.as_ref().and_then(|t| t.lines().find_map(|l| arg_value(l, "-f")));

    // systemd unit (present even if currently stopped).
    if r.systemd_present
        && sh("systemctl cat haproxy.service >/dev/null 2>&1 && echo yes").is_some()
    {
        hap.systemd_unit = Some("haproxy".into());
        if sh("systemctl is-active haproxy 2>/dev/null | grep -x active").is_some() {
            hap.running = true;
            native = true;
        }
    }
    // Docker container.
    if r.docker_present {
        if let Some(rows) = sh("docker ps --format '{{.Names}}\t{{.Image}}' 2>/dev/null") {
            for row in rows.lines() {
                let cols: Vec<&str> = row.split('\t').collect();
                if cols.len() == 2 && (cols[0].contains("haproxy") || cols[1].contains("haproxy"))
                {
                    hap.container_name = Some(cols[0].to_string());
                    hap.running = true;
                    break;
                }
            }
        }
    }
    hap.runtime = if native {
        "native".into()
    } else if hap.container_name.is_some() {
        "docker".into()
    } else {
        "unknown".into()
    };

    // Config file: the -f path if it's readable on THIS host, else the
    // distro-standard /etc/haproxy/haproxy.cfg (a container's internal -f
    // path is kept separately so install.sh can validate through docker exec).
    let host_readable = |p: &str| sh(&format!("test -r '{p}' && echo yes")).is_some();
    if let Some(p) = &cmdline_cfg {
        if host_readable(p) {
            hap.cfg_path = Some(p.clone());
        } else {
            hap.container_cfg_path = Some(p.clone());
        }
    }
    if hap.cfg_path.is_none() && host_readable("/etc/haproxy/haproxy.cfg") {
        hap.cfg_path = Some("/etc/haproxy/haproxy.cfg".into());
    }

    hap.detected = hap.running || hap.systemd_unit.is_some() || hap.cfg_path.is_some();
    if !hap.detected {
        r.haproxy = hap;
        return;
    }

    hap.version = sh("haproxy -v 2>/dev/null | head -1")
        .or_else(|| {
            hap.container_name
                .as_ref()
                .and_then(|c| sh(&format!("docker exec {c} haproxy -v 2>/dev/null | head -1")))
        })
        .map(|s| s.trim().to_string());
    // What a runtime-socket flip would talk through (socat preferred).
    hap.socket_tool = sh("command -v socat >/dev/null 2>&1 && echo socat")
        .or_else(|| sh("command -v nc >/dev/null 2>&1 && echo nc"));

    match hap
        .cfg_path
        .as_ref()
        .and_then(|p| sh(&format!("cat '{p}' 2>/dev/null")))
    {
        Some(cfg) => {
            let parsed = parse_haproxy_cfg(&cfg);
            hap.routes = parsed.routes;
            hap.backends = parsed.backends;
            hap.stats_sockets = parsed.stats_sockets;
            hap.admin_socket = parsed.admin_socket;
        }
        None => hap.parse_failed = true,
    }
    r.haproxy = hap;
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
        if let Some(mgr) = &r.nodeos.script_manager {
            line(
                &mut o,
                "managed",
                &format!(
                    "script-managed (parent: {mgr}{})",
                    if r.nodeos.parent_chain.is_empty() {
                        String::new()
                    } else {
                        format!("; chain: {} -> init", r.nodeos.parent_chain.join(" -> "))
                    }
                ),
            );
            if let Some(cfg) = &r.nodeos.config_path {
                line(&mut o, "config", cfg);
            }
            if let Some(d) = &r.nodeos.data_dir {
                line(&mut o, "data dir", d);
            }
            if let Some(l) = &r.nodeos.stdio_log {
                line(&mut o, "stdout/stderr", l);
            }
        }
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

    if r.haproxy.detected {
        o.push_str("\nWEB EDGE (haproxy)\n");
        line(&mut o, "state", &match (r.haproxy.running, r.haproxy.runtime.as_str()) {
            (true, "docker") => format!("running (docker container `{}`)", opt(&r.haproxy.container_name)),
            (true, "native") => format!(
                "running (native{})",
                r.haproxy.systemd_unit.as_ref().map(|u| format!(", unit {u}")).unwrap_or_default()
            ),
            (true, _) => "running".into(),
            (false, _) => "installed but not running".into(),
        });
        line(&mut o, "version", &opt(&r.haproxy.version));
        line(&mut o, "config", &opt(&r.haproxy.cfg_path));
        if r.haproxy.parse_failed {
            line(&mut o, "routes", "config unreadable — run doctor as root to map routes");
        }
        for route in &r.haproxy.routes {
            let servers = route
                .servers
                .iter()
                .map(|s| {
                    format!(
                        "{} {}{}",
                        s.name,
                        s.addr,
                        if s.disabled { " (disabled)" } else if s.backup { " (backup)" } else { "" }
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            line(
                &mut o,
                &format!("route {}", route.frontend),
                &format!(
                    "{}{} [{}] -> {} {{ {} }}",
                    route.binds.join(","),
                    if route.tls { " ssl" } else { "" },
                    route.rule,
                    route.backend,
                    if servers.is_empty() { "-".to_string() } else { servers }
                ),
            );
        }
        line(&mut o, "admin socket", &match &r.haproxy.admin_socket {
            Some(path) => format!(
                "{path} (level admin) — zero-reload runtime flip{}",
                match &r.haproxy.socket_tool {
                    Some(tool) => format!(" via {tool}"),
                    None => " (needs socat: apt-get install -y socat)".into(),
                }
            ),
            None => "none — flips fall back to cfg edit + validate + graceful reload".into(),
        });
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
    fn verdict_script_managed_nodeos_softens_to_start_cmd_only() {
        // tn1 pattern: native nodeos, no unit, but the parent chain told us
        // HOW it is managed — the stop side is derivable, so the need is
        // only the operator's start script (for abort rollback).
        let mut r = ready_report();
        r.nodeos.runtime = "native".into();
        r.nodeos.container_name = None;
        r.nodeos.systemd_units = vec![];
        r.nodeos.parent_chain = vec!["bash".into(), "screen".into()];
        r.nodeos.script_manager = classify_parent_chain(&r.nodeos.parent_chain);
        let v = compute_verdicts(&r);
        assert_eq!(v["api"].status, "NEEDS");
        let need = v["api"]
            .needs
            .iter()
            .find(|n| n.contains("script-managed"))
            .expect("script-managed need");
        assert!(need.contains("start_cmd"));
        assert!(need.contains("screen"));
        assert!(!v["api"].needs.iter().any(|n| n.contains("stop_cmd must be declared")));
        // bp mode never stops nodeos — unchanged READY.
        assert_eq!(v["bp"].status, "READY");
    }

    #[test]
    fn parent_chain_classification_covers_the_classic_bp_patterns() {
        let c = |v: &[&str]| classify_parent_chain(&v.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        // screen/tmux anywhere in the chain win (the shell under them is detail).
        assert_eq!(c(&["bash", "SCREEN"]), Some("screen".into()));
        assert_eq!(c(&["bash", "tmux: server"]), Some("tmux".into()));
        // cron-launched.
        assert_eq!(c(&["sh", "cron"]), Some("cron".into()));
        // A shell still holding the child (operator ran ./start.sh in an
        // interactive session that is still open).
        assert_eq!(c(&["bash", "sshd"]), Some("shell script (bash)".into()));
        // Orphan re-parented to init: nohup/setsid/& + logout, or rc.local.
        assert_eq!(
            c(&[]),
            Some("orphaned (nohup/setsid — parent is init; started by a script that has exited)".into())
        );
        // Direct child of an ssh session (no wrapper shell fork).
        assert_eq!(c(&["sshd"]), Some("interactive session (sshd)".into()));
        // Anything else is reported, not guessed at.
        assert_eq!(c(&["supervisord"]), Some("under `supervisord`".into()));
    }

    #[test]
    fn os_gate_accepts_focal_jammy_noble_only() {
        for (ver, ok) in [("20.04", true), ("22.04", true), ("24.04", true), ("18.04", false)] {
            let mut r = ready_report();
            r.host.os_version = ver.into();
            r.host.os_supported = r.host.os.to_lowercase().contains("ubuntu")
                && matches!(ver, "20.04" | "22.04" | "24.04");
            let v = compute_verdicts(&r);
            assert_eq!(v["api"].status == "READY", ok, "version {ver}");
            if !ok {
                assert!(v["api"].unsupported[0].contains("20.04/22.04/24.04"));
            }
        }
    }

    #[test]
    fn cwd_resolution_for_relative_config_paths() {
        assert_eq!(resolve_against_cwd("/opt/x/config.ini", Some("/home/u")), "/opt/x/config.ini");
        assert_eq!(resolve_against_cwd("cfg/config.ini", Some("/opt/nodeos/")), "/opt/nodeos/cfg/config.ini");
        assert_eq!(resolve_against_cwd("cfg/config.ini", None), "cfg/config.ini");
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

    // ------------------------------------------------------ haproxy --

    /// Fixture 1: the simplest real layout — one frontend, one backend,
    /// single nodeos server. This is the shape the flip templater loves.
    const HAP_SIMPLE: &str = r#"
global
    log /dev/log local0
    maxconn 4096

defaults
    mode http
    timeout connect 5s
    timeout client 30s
    timeout server 30s

frontend fe_main
    bind *:80
    default_backend be_nodeos

backend be_nodeos
    server nodeos1 127.0.0.1:8888 check
"#;

    /// Fixture 2: TLS frontend with named-ACL path routing (/v1 vs /v2) and
    /// both flavors of stats socket (UNIX admin + inet operator).
    const HAP_TLS_ACL: &str = r#"
global
    stats socket /run/haproxy/admin.sock mode 660 level admin
    stats socket ipv4@127.0.0.1:9999 level operator

frontend fe_https
    bind :443 ssl crt /etc/haproxy/certs/rpc.pem alpn h2,http/1.1
    acl is_v1 path_beg /v1
    acl is_v2 path_beg /v2
    use_backend be_v1 if is_v1
    use_backend be_hyperion if is_v2
    default_backend be_web

backend be_v1
    server nodeos 127.0.0.1:8888 check

backend be_hyperion
    server hyp 127.0.0.1:7000 check

backend be_web
    server web 127.0.0.1:3000
"#;

    /// Fixture 3: the multi-nodeos load-balancing layout common among
    /// Antelope BPs — several servers (incl. backup + one drained) in one
    /// backend. The route map must carry all of them so the verdict can
    /// demand a drain decision.
    const HAP_MULTI: &str = r#"
frontend fe
    bind *:80
    default_backend be_pool

backend be_pool
    balance roundrobin
    option httpchk GET /v1/chain/get_info
    server api1 10.0.0.1:8888 check
    server api2 10.0.0.2:8888 check
    server api3 10.0.0.3:8888 check backup
    server old 10.0.0.4:8888 disabled
"#;

    /// Fixture 4: no stats socket at all, and a `listen` block (frontend +
    /// backend fused) — plus an anonymous-ACL use_backend.
    const HAP_LISTEN: &str = r#"
global
    log /dev/log local0

listen l_v1
    bind 0.0.0.0:8080
    use_backend be_history if { path_beg /v2 }
    server nodeos 127.0.0.1:8888 check

backend be_history
    server hyp 127.0.0.1:7000
"#;

    #[test]
    fn haproxy_simple_frontend_backend_maps_route() {
        let hap = parse_haproxy_cfg(HAP_SIMPLE);
        assert_eq!(hap.routes.len(), 1);
        let route = &hap.routes[0];
        assert_eq!(route.frontend, "fe_main");
        assert_eq!(route.binds, vec!["*:80"]);
        assert!(!route.tls);
        assert_eq!(route.rule, "default");
        assert_eq!(route.backend, "be_nodeos");
        assert_eq!(route.servers.len(), 1);
        assert_eq!(route.servers[0].name, "nodeos1");
        assert_eq!(route.servers[0].addr, "127.0.0.1:8888");
        assert!(route.servers[0].check);
        assert!(!route.servers[0].disabled);
        // No stats socket in the config -> no zero-reload flip path.
        assert!(hap.stats_sockets.is_empty());
        assert_eq!(hap.admin_socket, None);
    }

    #[test]
    fn haproxy_tls_acl_routing_and_admin_socket() {
        let hap = parse_haproxy_cfg(HAP_TLS_ACL);
        // Routes come out in haproxy's evaluation order: use_backend rules
        // first (declaration order), default_backend last.
        assert_eq!(hap.routes.len(), 3);
        assert_eq!(hap.routes[0].backend, "be_v1");
        assert_eq!(hap.routes[0].rule, "if path_beg /v1"); // named ACL expanded
        assert!(hap.routes[0].tls);
        assert_eq!(hap.routes[0].binds, vec![":443"]);
        assert_eq!(hap.routes[0].servers[0].addr, "127.0.0.1:8888");
        assert_eq!(hap.routes[1].backend, "be_hyperion");
        assert_eq!(hap.routes[1].rule, "if path_beg /v2");
        assert_eq!(hap.routes[2].backend, "be_web");
        assert_eq!(hap.routes[2].rule, "default");
        // Both stats sockets seen; only the UNIX admin one is the flip path.
        assert_eq!(hap.stats_sockets.len(), 2);
        assert!(hap.stats_sockets[0].admin);
        assert_eq!(hap.stats_sockets[1].level.as_deref(), Some("operator"));
        assert!(!hap.stats_sockets[1].admin);
        assert_eq!(hap.admin_socket.as_deref(), Some("/run/haproxy/admin.sock"));
    }

    #[test]
    fn haproxy_multi_server_backend_parses_flags() {
        let hap = parse_haproxy_cfg(HAP_MULTI);
        assert_eq!(hap.routes.len(), 1);
        let servers = &hap.routes[0].servers;
        assert_eq!(servers.len(), 4);
        assert!(servers[2].backup);
        assert!(servers[3].disabled);
        // 3 active (non-disabled) servers — the verdict test below insists on
        // a drain decision for exactly this shape.
        assert_eq!(servers.iter().filter(|s| !s.disabled).count(), 3);
    }

    #[test]
    fn haproxy_listen_block_and_anonymous_acl() {
        let hap = parse_haproxy_cfg(HAP_LISTEN);
        assert_eq!(hap.admin_socket, None);
        assert_eq!(hap.routes.len(), 2);
        // use_backend inside the listen block, anonymous ACL braces dropped.
        assert_eq!(hap.routes[0].backend, "be_history");
        assert_eq!(hap.routes[0].rule, "if path_beg /v2");
        // The listen block's own servers form an implicit same-name backend.
        assert_eq!(hap.routes[1].backend, "l_v1");
        assert_eq!(hap.routes[1].rule, "default");
        assert_eq!(hap.routes[1].servers[0].addr, "127.0.0.1:8888");
        assert_eq!(hap.backends["l_v1"].len(), 1);
    }

    /// A report where haproxy (not nginx) is the sole edge, socket flip ready.
    fn haproxy_ready_report(cfg: &str) -> DoctorReport {
        let mut r = ready_report();
        r.web = WebInfo { server: "none".into(), ..Default::default() };
        let parsed = parse_haproxy_cfg(cfg);
        r.haproxy = HaproxyInfo {
            detected: true,
            running: true,
            runtime: "native".into(),
            systemd_unit: Some("haproxy".into()),
            cfg_path: Some("/etc/haproxy/haproxy.cfg".into()),
            routes: parsed.routes,
            backends: parsed.backends,
            stats_sockets: parsed.stats_sockets,
            admin_socket: parsed.admin_socket,
            socket_tool: Some("socat".into()),
            ..Default::default()
        };
        r
    }

    #[test]
    fn verdict_haproxy_single_server_edge_is_ready() {
        let v = compute_verdicts(&haproxy_ready_report(HAP_SIMPLE));
        assert_eq!(v["api"].status, "READY");
        assert_eq!(v["hyperion"].status, "READY");
        assert_eq!(v["bp"].status, "READY");
    }

    #[test]
    fn verdict_haproxy_multi_server_backend_demands_drain_decision() {
        let v = compute_verdicts(&haproxy_ready_report(HAP_MULTI));
        assert_eq!(v["api"].status, "NEEDS");
        let need = v["api"]
            .needs
            .iter()
            .find(|n| n.contains("be_pool"))
            .expect("multi-server backend need");
        assert!(need.contains("3 active servers"));
        assert!(need.contains("drain strategy"));
        assert!(need.contains("HAProxy notes"));
        // bp mode has no public edge — the backend shape does not block it.
        assert_eq!(v["bp"].status, "READY");
    }

    #[test]
    fn verdict_two_edges_needs_flip_edge_declared() {
        let mut r = haproxy_ready_report(HAP_SIMPLE);
        // nginx is ALSO running with routes -> ambiguous public edge.
        r.web = parse_nginx_dump(
            "# configuration file /etc/nginx/sites-enabled/rpc:\n\
             server { listen 80; server_name rpc.example.com; \
             location /v1/chain/ { proxy_pass http://127.0.0.1:8888; } }\n",
        );
        r.web.server = "nginx".into();
        let v = compute_verdicts(&r);
        assert_eq!(v["api"].status, "NEEDS");
        assert!(v["api"].needs.iter().any(|n| n.contains("two web edges") && n.contains("flip.edge")));
    }

    #[test]
    fn verdict_haproxy_parse_failure_and_missing_socat_are_named() {
        let mut r = haproxy_ready_report(HAP_TLS_ACL);
        r.haproxy.socket_tool = None;
        let v = compute_verdicts(&r);
        assert_eq!(v["api"].status, "NEEDS");
        assert!(v["api"].needs.iter().any(|n| n.contains("socat")));

        let mut r = haproxy_ready_report(HAP_SIMPLE);
        r.haproxy.routes.clear();
        r.haproxy.backends.clear();
        r.haproxy.parse_failed = true;
        let v = compute_verdicts(&r);
        assert_eq!(v["api"].status, "NEEDS");
        assert!(v["api"].needs.iter().any(|n| n.contains("could not read its config")));
    }

    #[test]
    fn haproxy_report_serializes_route_map() {
        let mut r = haproxy_ready_report(HAP_TLS_ACL);
        r.verdicts = compute_verdicts(&r);
        let json = serde_json::to_value(&r).unwrap();
        assert_eq!(json["haproxy"]["running"], true);
        assert_eq!(json["haproxy"]["admin_socket"], "/run/haproxy/admin.sock");
        assert_eq!(json["haproxy"]["routes"][0]["backend"], "be_v1");
        assert_eq!(json["haproxy"]["routes"][0]["servers"][0]["addr"], "127.0.0.1:8888");
        assert_eq!(json["verdicts"]["api"]["status"], "READY");
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
