// coordlib.mjs — pure helpers for control/coord.mjs (importable by tests: no CLI side effects).
//
// eventPayload(a, now): the signed `event` payload object from coord.mjs's --flags.
//   Fleet bindings (all optional, all covered by the signature, all checked by the agent in src/coord.rs):
//     --roster bp1,bp2,bp3          servers that MUST agree before ignition; "producer:instance_id" pins a
//                                   beacon instance (its 32-hex `beacon.instance` id). The agent's fleet gate counts only these, with fresh reports.
//     --quorum 5                    how many roster members must agree (default on the agent: all of them)
//     --release-sha256 <hex>        the PulseVM plugin build every agent must have installed (target.plugin_path)
//     --snapshot-sha256 <hex>       the expected cut snapshot hash, once known
//     --metal-network-id N          the Metal network the target runs on (a private rehearsal network); every agent's
//                                   target.metal_network_id (or the network its chain moves to) must equal it
//   Without them an event carries no roster, and the agent's fleet gate falls back to the legacy
//   "quorum of whoever reports" (or no gate at all), which is not fleet agreement.
export function parseRoster(spec) {
  if (spec === undefined) return undefined;
  const members = String(spec).split(',').map((s) => s.trim()).filter(Boolean).map((s) => {
    const [producer, instance_id] = s.split(':');
    if (!/^[a-z1-5.]{1,12}$/.test(producer)) throw new Error(`roster member "${producer}" is not an account name`);
    // A pinned instance is a beacon's `beacon.instance` id (32 hex, as /api/status shows it): anything else could
    // never match a report, and the member would silently never count.
    if (instance_id !== undefined && !/^[0-9a-f]{32}$/i.test(instance_id)) throw new Error(`roster member ${producer}: instance id "${instance_id}" is not a 32-hex beacon instance id`);
    return instance_id ? { producer, instance_id: instance_id.toLowerCase() } : { producer };
  });
  if (!members.length) throw new Error('--roster is empty');
  const seen = new Set();
  for (const m of members) {
    const k = `${m.producer}:${m.instance_id || ''}`;
    if (seen.has(k) || (!m.instance_id && members.filter((x) => x.producer === m.producer).length > 1)) {
      throw new Error(`roster lists ${m.producer} more than once (each member may count once toward the quorum)`);
    }
    seen.add(k);
  }
  return members;
}

const HEX64 = /^[0-9a-f]{64}$/i;

export function eventPayload(a, now = Date.now()) {
  const roster = parseRoster(a.roster);
  if (a.quorum !== undefined) {
    if (!roster) throw new Error('--quorum needs --roster (a quorum of whom?)');
    const q = Number(a.quorum);
    if (!Number.isInteger(q) || q < 1 || q > roster.length) throw new Error(`--quorum ${a.quorum} is not within 1..=${roster.length}`);
  }
  for (const f of ['release-sha256', 'snapshot-sha256']) {
    if (a[f] !== undefined && !HEX64.test(a[f])) throw new Error(`--${f} must be 64 hex characters`);
  }
  if (a['metal-network-id'] !== undefined && !/^[1-9]\d{0,9}$/.test(String(a['metal-network-id']))) throw new Error('--metal-network-id must be a positive integer');
  return {
    v: 1, type: 'event', network: a.net, event_id: a['event-id'] || `ev-${now.toString(36)}`, chain_id: a['chain-id'], h: +a.h,
    freeze_lead_blocks: +(a.lead || 24), ...(a['cpu-scale'] === 'none' ? {} : { import_cpu_scale: +(a['cpu-scale'] || 143) }),
    ...(roster ? { roster } : {}),
    ...(a.quorum !== undefined ? { quorum: Number(a.quorum) } : {}),
    ...(a['release-sha256'] ? { release_sha256: a['release-sha256'].toLowerCase() } : {}),
    ...(a['snapshot-sha256'] ? { snapshot_sha256: a['snapshot-sha256'].toLowerCase() } : {}),
    ...(a['metal-network-id'] !== undefined ? { metal_network_id: Number(a['metal-network-id']) } : {}),
    issued_at_ms: now,
  };
}
