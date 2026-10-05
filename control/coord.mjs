#!/usr/bin/env node
// coord.mjs — coordinator tool: make a key, then publish SIGNED event / arm / abort messages.
//
//   node control/coord.mjs keygen --out coordinator.pem          # prints the public key (hex) for configs
//   node control/coord.mjs event --url https://control… --net rehearsal --chain-id <hex> --h 9000 \
//        --lead 24 --cpu-scale 143 --key coordinator.pem [--event-id ev-…]
//        (--cpu-scale none omits import_cpu_scale: use it for import_backend = "upstream", where it does nothing)
//   node control/coord.mjs arm   --url … --net rehearsal --event-id ev-… --key coordinator.pem --event-file ev-….event.json
//   node control/coord.mjs abort --url … --net rehearsal --event-id ev-… --key coordinator.pem --event-file ev-….event.json
//   node control/coord.mjs complete --url … --net rehearsal --event-id ev-… --key coordinator.pem --event-file ev-….event.json
//        (closes an event that RAN — LIVE — so the next one can be published; never sign an abort for that:
//         an abort tells every agent "stop", and they record it as final)
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
  console.error(`signing ${cmd} for ${ev.event_id} (H ${ev.h}, event_hash ${eventHash.slice(0, 16)}…)`);
}
const payload = JSON.stringify(cmd === 'event'
  ? { v: 1, type: 'event', network: a.net, event_id: a['event-id'] || `ev-${Date.now().toString(36)}`, chain_id: a['chain-id'], h: +a.h,
      freeze_lead_blocks: +(a.lead || 24), ...(a['cpu-scale'] === 'none' ? {} : { import_cpu_scale: +(a['cpu-scale'] || 143) }), issued_at_ms: Date.now() }
  : { v: 1, type: cmd, network: a.net, event_id: a['event-id'], event_hash: eventHash, issued_at_ms: Date.now() });
const msg = { payload, sig: sign(null, Buffer.from(payload), key).toString('hex'), key: rawPub(key) };
if (cmd === 'event') { const f = `${JSON.parse(payload).event_id}.event.json`; writeFileSync(f, JSON.stringify(msg, null, 1)); console.error(`saved the signed event to ${f} (arm/abort need it)`); }
const r = await fetch(`${base}/api/coord/${a.net}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(msg) });
const text = await r.text();
console.log(r.status, text, JSON.parse(payload).event_id);
if (cmd === 'event' && r.ok) {   // the relay must report the same hash we computed over our own payload
  let got = null; try { got = JSON.parse(text).event_hash; } catch {}
  if (got !== eventHashOf(payload)) { console.error(`relay reported event_hash ${got}, expected ${eventHashOf(payload)}: do not arm`); process.exit(1); }
}
process.exit(r.ok ? 0 : 1);
