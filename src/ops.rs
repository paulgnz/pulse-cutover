//! Side effects behind a trait so the state machine is unit-testable.
//!
//! `HttpOps` is the real implementation: nodeos chain/producer APIs on the
//! source side, pulsevm JSON-RPC on the target side, systemctl/shell for
//! ignition and hooks.

use std::time::Duration;

use serde_json::{
    Value,
    json,
};

#[derive(Debug, Clone, PartialEq)]
pub struct ChainInfo {
    pub chain_id: String,
    pub head_block_num: u64,
    pub head_block_id: String,
    pub head_block_time: String,
    pub last_irreversible_block_num: u64,
}

#[derive(Debug, Clone)]
pub struct SnapshotResult {
    /// Path as reported by nodeos (its own filesystem namespace).
    pub snapshot_name: String,
    pub head_block_num: u64,
    pub head_block_id: String,
}

pub trait ChainOps {
    fn source_info(&self) -> Result<ChainInfo, String>;
    fn source_block_id(&self, block_num: u64) -> Result<(String, String), String>; // (id, timestamp)
    /// Number of transactions in a source block (burn-off audit: blocks
    /// between the cut and the pause must be empty once writes are frozen).
    fn source_block_tx_count(&self, block_num: u64) -> Result<u64, String>;
    fn producer_paused(&self) -> Result<bool, String>;
    fn pause(&self) -> Result<(), String>;
    fn resume(&self) -> Result<(), String>;
    fn create_snapshot(&self) -> Result<SnapshotResult, String>;
    /// Best-effort schedule_snapshot at a specific height (Leap 4+ snapshot
    /// scheduler). Err if unsupported — callers fall back to create_snapshot.
    fn schedule_snapshot(&self, height: u64) -> Result<(), String>;
    /// getInfo against the target pulsevm chain; Ok(None) while unreachable
    /// (still bootstrapping / chain not yet initialized).
    fn target_info(&self) -> Result<Option<ChainInfo>, String>;
    /// get_info via the PUBLIC /v1 endpoint (api mode: through nginx + the
    /// REST gateway). Ok(None) while unreachable/mid-reload — the flip
    /// health check treats that as a transient, bounded by its timeout.
    fn public_info(&self, public_url: &str) -> Result<Option<ChainInfo>, String>;
    fn ignite(&self) -> Result<String, String>;
    fn run_hook(&self, cmd: &str) -> Result<String, String>;
    fn now_ms(&self) -> u64;
    fn sleep_ms(&self, ms: u64);
}

/// Run a shell command, returning trimmed stdout (or stderr if stdout is
/// empty) on success. Shared by hooks, ignition, and the loop harness reset.
pub fn run_shell(cmd: &str) -> Result<String, String> {
    let out = std::process::Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .output()
        .map_err(|e| format!("spawn `{cmd}`: {e}"))?;
    let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
    if out.status.success() {
        Ok(if stdout.is_empty() { stderr } else { stdout })
    } else {
        Err(format!("`{cmd}` exited {}: {stderr} {stdout}", out.status))
    }
}

pub struct HttpOps {
    pub source_rpc: String,
    pub producer_api: String,
    pub target_rpc: String,
    pub ignite_cmd: String,
    pub snapshot_timeout: Duration,
    agent: ureq::Agent,
}

impl HttpOps {
    pub fn new(
        source_rpc: &str,
        producer_api: &str,
        target_rpc: &str,
        ignite_cmd: &str,
        snapshot_timeout_secs: u64,
    ) -> Self {
        HttpOps {
            source_rpc: source_rpc.trim_end_matches('/').to_string(),
            producer_api: producer_api.trim_end_matches('/').to_string(),
            target_rpc: target_rpc.to_string(),
            ignite_cmd: ignite_cmd.to_string(),
            snapshot_timeout: Duration::from_secs(snapshot_timeout_secs),
            agent: ureq::AgentBuilder::new()
                .timeout(Duration::from_secs(15))
                .build(),
        }
    }

    fn post(&self, url: &str, body: Option<Value>, timeout: Option<Duration>) -> Result<Value, String> {
        let mut req = self.agent.post(url);
        if let Some(t) = timeout {
            req = req.timeout(t);
        }
        let resp = match body {
            Some(b) => req.send_json(b),
            None => req.send_string(""),
        };
        match resp {
            Ok(r) => r
                .into_json::<Value>()
                .map_err(|e| format!("{url}: bad json: {e}")),
            Err(ureq::Error::Status(code, r)) => {
                let text = r.into_string().unwrap_or_default();
                Err(format!("{url}: HTTP {code}: {text}"))
            }
            Err(e) => Err(format!("{url}: {e}")),
        }
    }

    fn parse_info(v: &Value) -> Result<ChainInfo, String> {
        let s = |k: &str| -> Result<String, String> {
            v.get(k)
                .and_then(|x| x.as_str())
                .map(str::to_string)
                .ok_or_else(|| format!("get_info missing {k}: {v}"))
        };
        let n = |k: &str| -> Result<u64, String> {
            let field = v.get(k).ok_or_else(|| format!("get_info missing {k}: {v}"))?;
            field
                .as_u64()
                .or_else(|| field.as_str().and_then(|x| x.parse().ok()))
                .ok_or_else(|| format!("get_info bad {k}: {v}"))
        };
        Ok(ChainInfo {
            chain_id: s("chain_id")?,
            head_block_num: n("head_block_num")?,
            head_block_id: s("head_block_id")?,
            head_block_time: s("head_block_time").unwrap_or_default(),
            last_irreversible_block_num: n("last_irreversible_block_num").unwrap_or(0),
        })
    }
}

impl ChainOps for HttpOps {
    fn source_info(&self) -> Result<ChainInfo, String> {
        let v = self.post(&format!("{}/v1/chain/get_info", self.source_rpc), None, None)?;
        Self::parse_info(&v)
    }

    fn source_block_id(&self, block_num: u64) -> Result<(String, String), String> {
        let v = self.post(
            &format!("{}/v1/chain/get_block", self.source_rpc),
            Some(json!({"block_num_or_id": block_num})),
            None,
        )?;
        let id = v
            .get("id")
            .and_then(|x| x.as_str())
            .ok_or_else(|| format!("get_block({block_num}) missing id"))?;
        let ts = v
            .get("timestamp")
            .and_then(|x| x.as_str())
            .unwrap_or_default();
        Ok((id.to_string(), ts.to_string()))
    }

    fn source_block_tx_count(&self, block_num: u64) -> Result<u64, String> {
        let v = self.post(
            &format!("{}/v1/chain/get_block", self.source_rpc),
            Some(json!({"block_num_or_id": block_num})),
            None,
        )?;
        Ok(v.get("transactions")
            .and_then(|t| t.as_array())
            .map(|a| a.len() as u64)
            .unwrap_or(0))
    }

    fn producer_paused(&self) -> Result<bool, String> {
        let v = self.post(&format!("{}/v1/producer/paused", self.producer_api), None, None)?;
        v.as_bool().ok_or_else(|| format!("paused: bad reply {v}"))
    }

    fn pause(&self) -> Result<(), String> {
        self.post(&format!("{}/v1/producer/pause", self.producer_api), None, None)
            .map(|_| ())
    }

    fn resume(&self) -> Result<(), String> {
        self.post(&format!("{}/v1/producer/resume", self.producer_api), None, None)
            .map(|_| ())
    }

    fn create_snapshot(&self) -> Result<SnapshotResult, String> {
        let v = self.post(
            &format!("{}/v1/producer/create_snapshot", self.producer_api),
            None,
            Some(self.snapshot_timeout),
        )?;
        let name = v
            .get("snapshot_name")
            .and_then(|x| x.as_str())
            .ok_or_else(|| format!("create_snapshot: no snapshot_name in {v}"))?;
        let id = v
            .get("head_block_id")
            .and_then(|x| x.as_str())
            .ok_or_else(|| format!("create_snapshot: no head_block_id in {v}"))?;
        // Antelope block ids carry the height in the first 4 bytes (BE).
        let num = v
            .get("head_block_num")
            .and_then(|x| x.as_u64())
            .or_else(|| u64::from_str_radix(id.get(0..8)?, 16).ok())
            .ok_or_else(|| format!("create_snapshot: cannot derive height from {v}"))?;
        Ok(SnapshotResult {
            snapshot_name: name.to_string(),
            head_block_num: num,
            head_block_id: id.to_string(),
        })
    }

    fn schedule_snapshot(&self, height: u64) -> Result<(), String> {
        self.post(
            &format!("{}/v1/producer/schedule_snapshot", self.producer_api),
            Some(json!({"start_block_num": height, "end_block_num": height})),
            None,
        )
        .map(|_| ())
    }

    fn target_info(&self) -> Result<Option<ChainInfo>, String> {
        let body = json!({"jsonrpc": "2.0", "method": "pulsevm.getInfo", "params": {}, "id": 1});
        match self.post(&self.target_rpc, Some(body), None) {
            Ok(v) => {
                let result = v.get("result").cloned().unwrap_or(v);
                match Self::parse_info(&result) {
                    Ok(info) => Ok(Some(info)),
                    // RPC answered but the chain isn't serving state yet.
                    Err(_) => Ok(None),
                }
            }
            // Unreachable/booting is a normal transient, not an error.
            Err(_) => Ok(None),
        }
    }

    fn public_info(&self, public_url: &str) -> Result<Option<ChainInfo>, String> {
        let url = format!("{}/v1/chain/get_info", public_url.trim_end_matches('/'));
        match self.post(&url, None, None) {
            Ok(v) => Ok(Self::parse_info(&v).ok()),
            Err(_) => Ok(None),
        }
    }

    fn ignite(&self) -> Result<String, String> {
        self.run_hook(&self.ignite_cmd)
    }

    fn run_hook(&self, cmd: &str) -> Result<String, String> {
        run_shell(cmd)
    }

    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    fn sleep_ms(&self, ms: u64) {
        std::thread::sleep(Duration::from_millis(ms));
    }
}
