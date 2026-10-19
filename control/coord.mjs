#!/usr/bin/env node
// coord.mjs — coordinator tool: make a key, then publish SIGNED event / arm / abort messages.
//
//   node control/coord.mjs keygen --out coordinator.pem          # prints the public key (hex) for configs
//   node control/coord.mjs event --url https://control… --net rehearsal --chain-id <hex> --h 9000 \
//        --lead 24 --cpu-scale 143 --key coordinator.pem [--event-id ev-…]
//        (--cpu-scale none omits import_cpu_scale: use it for import_backend = "upstream", where it does nothing)
//        [--roster bp1,bp2,…|producer:instance_id,…] [--quorum N] [--release-sha256 <hex>] [--snapshot-sha256 <hex>] [--metal-network-id N]
//        [--early-abort yes]  (rc.31: agents stop early on a provably unreachable quorum; OFF unless signed on)
//        [--producer-gate yes --source-rpc <nodeos /v1 base> [--standby a,b,…] [--cover name:member,…] [--min-agent-version V]]
//        (rc.33: bind every producer that could produce before the pause — get_producer_schedule's active, pending and
//         proposed, plus the standbys — and the roster member running each one's agent; mission control refuses the arm
//         until all of them are armed with the admission lock, and every agent re-checks before it freezes)
//        (fleet bindings, signed with the event; see control/coordlib.mjs. Without --roster the agents' fleet gate
//         has no roster and falls back to counting whoever reports)
//   node control/coord.mjs arm   --url … --net rehearsal --event-id ev-… --key coordinator.pem --event-file ev-….event.json
//        [--source-rpc <nodeos /v1 base>]  (rc.33: with a producer gate, refuse to sign if the schedule changed since the event)
//   node control/coord.mjs abort --url … --net rehearsal --event-id ev-… --key coordinator.pem --event-file ev-….event.json
//   node control/coord.mjs complete --url … --net rehearsal --event-id ev-… --key coordinator.pem --event-file ev-….event.json
//        (closes an event that RAN — LIVE — so the next one can be published; never sign an abort for that:
//         an abort tells every agent "stop", and they record it as final)
//        abort [--after-creation yes]: the relay refuses an abort once any member is past chain creation (rc.31);
//         this flag signs an explicit acknowledgement, which the relay accepts and logs as an alarm
//
// Keep the private key offline/secure. Agents only act on messages signed by keys in THEIR config.
// arm/abort carry event_hash = sha256 over the UTF-8 bytes of the exact signed event `payload` string (no
// re-serialization; see control/README.md and control/test/fixtures/event-hash-vector.json), so a signature for one
// version of an event can never act on another. `event` saves that payload to <event_id>.event.json; arm/abort
// hash YOUR saved copy (--event-file) and refuse if the relay serves anything else. Without --event-file they
// refuse unless --trust-relay yes is given (then the relay's copy is hashed; the relay is not an authority).
// Event ids are single-use.
import { generateKeyPairSync, createPrivateKey, createPublicKey, sign, createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { eventPayload, gateScheduleProblems } from './coordlib.mjs';
const [cmd, ...rest] = process.argv.slice(2);
const a = {}; for (let i = 0; i < rest.length; i += 2) a[rest[i].replace(/^--/, '')] = rest[i + 1];
const rawPub = (k) => createPublicKey(k).export({ format: 'der', type: 'spki' }).subarray(-32).toString('hex');
if (cmd === 'keygen') {
  const { privateKey } = generateKeyPairSync('ed25519');
  writeFileSync(a.out, privateKey.export({ format: 'pem', type: 'pkcs8' }), { mode: 0o600 });
  console.log(rawPub(privateKey));
  process.exit(0);
}
if (!['event', 'arm', 'abort', 'complete'].includes(cmd)) { console.error('usage: keygen | event | arm | abort | complete (see header)'); process.exit(2); }
const key = createPrivateKey(readFileSync(a.key));
const base = a.url.replace(/\/$/, '');
export const eventHashOf = (payload) => createHash('sha256').update(Buffer.from(payload, 'utf8')).digest('hex');
let eventHash = null;
// rc.33: the source schedule (producer gate), read from the coordinator's own nodeos.
const readSchedule = async () => {
  const r = await fetch(`${String(a['source-rpc']).replace(/\/$/, '')}/v1/chain/get_producer_schedule`, { method: 'POST', body: '{}', signal: AbortSignal.timeout(8000) });
  if (!r.ok) throw new Error(`get_producer_schedule: HTTP ${r.status}`);
  return r.json();
};
let schedule = null;
if (cmd === 'event' && a['producer-gate'] === 'yes') {
  if (!a['source-rpc']) { console.error('--producer-gate yes needs --source-rpc <nodeos /v1 base> (the schedule is read from the source chain)'); process.exit(2); }
  try { schedule = await readSchedule(); } catch (e) { console.error(`cannot read the source schedule: ${e.message || e}`); process.exit(1); }
}
if (cmd !== 'event') {
  const cur = await (await fetch(`${base}/api/coord/${a.net}`)).json();
  const ev = cur?.event?.payload ? JSON.parse(cur.event.payload) : null;
  if (!ev || ev.event_id !== a['event-id']) { console.error(`the relay's current event is ${ev?.event_id || 'none'}, not ${a['event-id']}: refusing to sign`); process.exit(1); }
  const relayHash = eventHashOf(cur.event.payload);
  if (a['event-file']) {
    const mine = JSON.parse(readFileSync(a['event-file'], 'utf8')).payload;
    eventHash = eventHashOf(mine);
    if (eventHash !== relayHash) { console.error(`the relay serves a different payload for ${ev.event_id} (relay ${relayHash.slice(0, 16)}…, yours ${eventHash.slice(0, 16)}…): refusing to sign`); process.exit(1); }
  } else if (a['trust-relay'] === 'yes') {
    eventHash = relayHash;
    console.error('warning: no --event-file; signing the hash of the payload the relay serves (--trust-relay yes)');
  } else { console.error('pass --event-file <event_id>.event.json (saved when the event was published), or --trust-relay yes'); process.exit(2); }
  if (cmd === 'arm' && ev.producer_gate) {
    if (a['source-rpc']) {
      let live;
      try { live = await readSchedule(); } catch (e) { console.error(`cannot read the source schedule (${e.message || e}): refusing to sign the arm`); process.exit(1); }
      const probs = gateScheduleProblems(ev.producer_gate, live);
      if (probs.length) { console.error(`the source schedule is not the one event ${ev.event_id} binds: ${probs.join('; ')}. Refusing to sign: abort this event and publish a new one`); process.exit(1); }
      console.error(`producer gate: the source schedule is still the bound one (${ev.producer_gate.required.length} producers)`);
    } else console.error('warning: no --source-rpc: the schedule is not re-checked here (mission control and every agent still check it)');
  }
  console.error(`signing ${cmd} for ${ev.event_id} (H ${ev.h}, event_hash ${eventHash.slice(0, 16)}…)`);
}
let payloadObj;
try {
  payloadObj = cmd === 'event' ? eventPayload(a, Date.now(), { schedule })
    : { v: 1, type: cmd, network: a.net, event_id: a['event-id'], event_hash: eventHash, issued_at_ms: Date.now(),
      // rc.31: the relay refuses an abort for an event already past chain creation unless the signer acknowledges it.
      ...(cmd === 'abort' && a['after-creation'] === 'yes' ? { acknowledge_past_creation: true } : {}) };
} catch (e) { console.error(String(e.message || e)); process.exit(2); }
const payload = JSON.stringify(payloadObj);
const msg = { payload, sig: sign(null, Buffer.from(payload), key).toString('hex'), key: rawPub(key) };
if (cmd === 'event') { const f = `${JSON.parse(payload).event_id}.event.json`; writeFileSync(f, JSON.stringify(msg, null, 1)); console.error(`saved the signed event to ${f} (arm/abort need it)`); }
const r = await fetch(`${base}/api/coord/${a.net}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(msg) });
const text = await r.text();
console.log(r.status, text, JSON.parse(payload).event_id);
// rc.33: a refused arm names the producers that are not ready.
if (cmd === 'arm' && r.status === 409) { try { for (const m of JSON.parse(text).missing || []) console.error(`  not ready: ${m.producer}${m.member !== m.producer ? ` (agent ${m.member})` : ''}: ${m.why}`); } catch {} }
if (cmd === 'event' && r.ok) {   // the relay must report the same hash we computed over our own payload
  let got = null; try { got = JSON.parse(text).event_hash; } catch {}
  if (got !== eventHashOf(payload)) { console.error(`relay reported event_hash ${got}, expected ${eventHashOf(payload)}: do not arm`); process.exit(1); }
}
process.exit(r.ok ? 0 : 1);
