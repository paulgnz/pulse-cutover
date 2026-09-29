#!/usr/bin/env node
// coord.mjs — coordinator tool: make a key, then publish SIGNED event / arm / abort messages.
//
//   node control/coord.mjs keygen --out coordinator.pem          # prints the public key (hex) for configs
//   node control/coord.mjs event --url https://control… --net rehearsal --chain-id <hex> --h 9000 \
//        --lead 24 --cpu-scale 143 --key coordinator.pem [--event-id ev-…]
//   node control/coord.mjs arm   --url … --net rehearsal --event-id ev-… --key coordinator.pem
//   node control/coord.mjs abort --url … --net rehearsal --event-id ev-… --key coordinator.pem
//
// Keep the private key offline/secure. Agents only act on messages signed by keys in THEIR config.
// arm/abort carry event_hash = sha256 of the published event payload (fetched from the relay and checked against
// --event-id), so a signature for one version of an event can never act on another. Event ids are single-use.
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
if (!['event', 'arm', 'abort'].includes(cmd)) { console.error('usage: keygen | event | arm | abort (see header)'); process.exit(2); }
const key = createPrivateKey(readFileSync(a.key));
const base = a.url.replace(/\/$/, '');
let eventHash = null;
if (cmd !== 'event') {
  const cur = await (await fetch(`${base}/api/coord/${a.net}`)).json();
  const ev = cur?.event?.payload ? JSON.parse(cur.event.payload) : null;
  if (!ev || ev.event_id !== a['event-id']) { console.error(`the relay's current event is ${ev?.event_id || 'none'}, not ${a['event-id']}: refusing to sign`); process.exit(1); }
  eventHash = createHash('sha256').update(cur.event.payload).digest('hex');
  console.error(`signing ${cmd} for ${ev.event_id} (H ${ev.h}, event_hash ${eventHash.slice(0, 16)}…)`);
}
const payload = JSON.stringify(cmd === 'event'
  ? { v: 1, type: 'event', network: a.net, event_id: a['event-id'] || `ev-${Date.now().toString(36)}`, chain_id: a['chain-id'], h: +a.h,
      freeze_lead_blocks: +(a.lead || 24), import_cpu_scale: +(a['cpu-scale'] || 143), issued_at_ms: Date.now() }
  : { v: 1, type: cmd, network: a.net, event_id: a['event-id'], event_hash: eventHash, issued_at_ms: Date.now() });
const msg = { payload, sig: sign(null, Buffer.from(payload), key).toString('hex'), key: rawPub(key) };
const r = await fetch(`${base}/api/coord/${a.net}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(msg) });
console.log(r.status, await r.text(), JSON.parse(payload).event_id);
process.exit(r.ok ? 0 : 1);
