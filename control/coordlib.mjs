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
//     --producer-gate yes           rc.33: bind every producer that could produce before the pause (needs `schedule`, the
//                                   source's get_producer_schedule answer, which coord.mjs reads from --source-rpc), with
//     [--standby a,b] [--cover name:member,…] [--min-agent-version 0.5.0-rc.33]   (see producerGate below)
//   Without them an event carries no roster, and the agent's fleet gate falls back to the legacy
//   "quorum of whoever reports" (or no gate at all), which is not fleet agreement.
import { createHash } from 'node:crypto';
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

export function eventPayload(a, now = Date.now(), { schedule } = {}) {
  // D-10: every event binds the chain it cuts and its H (an XPR mainnet agent refuses an event that does not).
  if (!HEX64.test(a['chain-id'] || '')) throw new Error('--chain-id must be the source chain_id (64 hex characters)');
  if (!/^[1-9]\d{0,11}$/.test(String(a.h ?? ''))) throw new Error('--h must be a positive block number');
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
  let gate = null;
  if (a['producer-gate'] === 'yes') {
    if (!roster) throw new Error('--producer-gate needs --roster (the agents that cover the schedule)');
    if (!schedule) throw new Error('--producer-gate needs the source schedule (--source-rpc <nodeos /v1 base>)');
    gate = producerGate(schedule, { roster, standby: String(a.standby || '').split(',').map((s) => s.trim()).filter(Boolean),
      cover: parseCover(a.cover), minAgentVersion: a['min-agent-version'] || PRODUCER_GATE_MIN_AGENT_VERSION });
  } else if (a['producer-gate'] !== undefined && a['producer-gate'] !== 'no') throw new Error('--producer-gate takes yes or no');
  return {
    v: 1, type: 'event', network: a.net, event_id: a['event-id'] || `ev-${now.toString(36)}`, chain_id: a['chain-id'].toLowerCase(), h: +a.h,
    freeze_lead_blocks: +(a.lead || 24), ...(a['cpu-scale'] === 'none' ? {} : { import_cpu_scale: +(a['cpu-scale'] || 143) }),
    ...(roster ? { roster } : {}),
    ...(a.quorum !== undefined ? { quorum: Number(a.quorum) } : {}),
    ...(a['release-sha256'] ? { release_sha256: a['release-sha256'].toLowerCase() } : {}),
    ...(a['snapshot-sha256'] ? { snapshot_sha256: a['snapshot-sha256'].toLowerCase() } : {}),
    ...(a['metal-network-id'] !== undefined ? { metal_network_id: Number(a['metal-network-id']) } : {}),
    // rc.31: the fleet-wide early-abort setting (off unless the coordinator signs it on).
    ...(a['early-abort'] === 'yes' ? { early_abort: true } : {}),
    // rc.33: the all-producers gate (src/producer_gate.rs).
    ...(gate ? { producer_gate: gate } : {}),
    issued_at_ms: now,
  };
}

// ---- rc.33: the all-producers gate (src/producer_gate.rs) -------------------------------------------------------
// `coord.mjs event --producer-gate yes --source-rpc <nodeos>` reads get_producer_schedule and binds every producer that
// could produce before the pause (active ∪ pending ∪ proposed ∪ --standby) into the signed event, with the roster member
// that runs each one's agent (itself, or --cover name:member for servers that sign for several names) and the minimum
// agent version. Mission control refuses to relay the arm until every one of them is ready, and every agent re-checks
// the schedule and the members' reports right before it freezes.

export const PRODUCER_GATE_MIN_AGENT_VERSION = '0.5.0-rc.33';
const NAME = /^[a-z1-5.]{1,12}$/;

/** One schedule of a get_producer_schedule answer → {version, producers: [names]} or null. */
export function scheduleOf(s) {
  if (!s || typeof s !== 'object' || !Number.isSafeInteger(s.version) || !Array.isArray(s.producers)) return null;
  return { version: s.version, producers: s.producers.map((p) => (typeof p === 'string' ? p : p?.producer_name)).filter((n) => typeof n === 'string') };
}

/** semver precedence for this project's tags (0.5.0-rc.33); null when either side does not parse. */
export function compareVersions(a, b) {
  const parse = (v) => {
    const m = String(v ?? '').trim().replace(/^v/, '').split('+')[0].match(/^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?$/);
    return m ? { core: [+m[1], +m[2], +m[3]], pre: m[4] ? m[4].split('.') : [] } : null;
  };
  const x = parse(a), y = parse(b);
  if (!x || !y) return null;
  for (let i = 0; i < 3; i++) if (x.core[i] !== y.core[i]) return x.core[i] < y.core[i] ? -1 : 1;
  if (!x.pre.length || !y.pre.length) return x.pre.length === y.pre.length ? 0 : (x.pre.length ? -1 : 1);
  for (let i = 0; i < Math.min(x.pre.length, y.pre.length); i++) {
    const p = x.pre[i], q = y.pre[i], pn = /^\d+$/.test(p), qn = /^\d+$/.test(q);
    if (p === q) continue;
    if (pn && qn) return +p < +q ? -1 : 1;
    if (pn !== qn) return pn ? -1 : 1;
    return p < q ? -1 : 1;
  }
  return Math.sign(x.pre.length - y.pre.length);
}
export const versionAtLeast = (v, min) => { const c = compareVersions(v, min); return c !== null && c >= 0; };

const memberOf = (g, name) => (g.cover && Object.hasOwn(g.cover, name) ? g.cover[name] : name);

/** The text the gate's sha256 covers; byte-for-byte equal to ProducerGate::canonical in src/producer_gate.rs. */
export function producerGateCanonical(g) {
  const sched = (s) => (s ? `${s.version}:${s.producers.join(',')}` : '-');
  return ['pulse-cutover producer gate v1',
    `required=${[...g.required].sort().map((n) => `${n}:${memberOf(g, n)}`).join(',')}`,
    `active=${sched(g.active)}`, `pending=${sched(g.pending)}`, `proposed=${sched(g.proposed)}`,
    `standby=${[...(g.standby || [])].sort().join(',')}`, `min_agent_version=${g.min_agent_version}`].join('\n');
}
export const producerGateSha = (g) => createHash('sha256').update(Buffer.from(producerGateCanonical(g), 'utf8')).digest('hex');

/** "name:member,…" → {name: member}. */
export function parseCover(spec) {
  const out = {};
  for (const s of String(spec || '').split(',').map((x) => x.trim()).filter(Boolean)) {
    const [name, member] = s.split(':');
    if (!NAME.test(name || '') || !NAME.test(member || '')) throw new Error(`--cover entry "${s}" is not name:member (two account names)`);
    if (Object.hasOwn(out, name)) throw new Error(`--cover maps ${name} twice`);
    out[name] = member;
  }
  return out;
}

/**
 * Build the signed event's producer_gate from a get_producer_schedule answer. Throws when a producer that could produce
 * before the pause has no roster member running its agent.
 */
export function producerGate(schedule, { roster, standby = [], cover = {}, minAgentVersion = PRODUCER_GATE_MIN_AGENT_VERSION } = {}) {
  const active = scheduleOf(schedule?.active);
  if (!active || !active.producers.length) throw new Error('get_producer_schedule has no active schedule: cannot derive the producer gate');
  const pending = scheduleOf(schedule?.pending), proposed = scheduleOf(schedule?.proposed);
  for (const n of standby) if (!NAME.test(n)) throw new Error(`--standby "${n}" is not an account name`);
  if (compareVersions(minAgentVersion, PRODUCER_GATE_MIN_AGENT_VERSION) === null || compareVersions(minAgentVersion, PRODUCER_GATE_MIN_AGENT_VERSION) < 0) {
    throw new Error(`--min-agent-version must be a version at least ${PRODUCER_GATE_MIN_AGENT_VERSION}`);
  }
  const required = [...new Set([...active.producers, ...(pending?.producers || []), ...(proposed?.producers || []), ...standby])].sort();
  for (const n of required) if (!NAME.test(n)) throw new Error(`schedule names "${n}", which is not an account name`);
  const coverOut = {};
  for (const [name, member] of Object.entries(cover)) {
    if (!required.includes(name)) throw new Error(`--cover maps ${name}, which is not in the schedule or --standby`);
    if (member !== name) coverOut[name] = member;
  }
  const g = { v: 1, required, cover: coverOut, active, pending, proposed, standby: [...new Set(standby)].sort(), min_agent_version: minAgentVersion };
  const members = new Set((roster || []).map((m) => m.producer));
  const uncovered = required.filter((n) => !members.has(memberOf(g, n))).map((n) => (memberOf(g, n) === n ? n : `${n} (covered by ${memberOf(g, n)})`));
  if (uncovered.length) throw new Error(`these producers can produce before the pause but no --roster member runs their agent: ${uncovered.join(', ')} (add them to --roster, or map them with --cover name:member)`);
  return { ...g, sha256: producerGateSha(g) };
}

/** Problems with a live get_producer_schedule answer against the bound gate (same rules as the agent). */
export function gateScheduleProblems(g, live) {
  const active = scheduleOf(live?.active), pending = scheduleOf(live?.pending), proposed = scheduleOf(live?.proposed);
  if (!active) return ['get_producer_schedule has no active schedule'];
  const out = [];
  const req = new Set(g.required);
  const outside = [];
  for (const [what, s] of [['active', active], ['pending', pending], ['proposed', proposed]]) for (const p of s?.producers || []) if (!req.has(p)) outside.push(`${p} (${what})`);
  if (outside.length) out.push(`producers in the live schedule that are not in the event's armed set: ${[...new Set(outside)].sort().join(', ')}`);
  const same = (a, b) => (!a && !b) || (!!a && !!b && a.version === b.version && a.producers.join(',') === b.producers.join(','));
  if (!same(active, g.active)) out.push(`the active schedule changed since the event (bound v${g.active.version}, now v${active.version})`);
  if (!same(pending, g.pending || null)) out.push(`the pending schedule changed since the event (bound ${g.pending ? `v${g.pending.version}` : 'none'}, now ${pending ? `v${pending.version}` : 'none'})`);
  return out;
}
