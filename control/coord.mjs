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
import { generateKeyPairSync, createPrivateKey, createPublicKey, sign } from 'node:crypto';
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
const payload = JSON.stringify(cmd === 'event'
  ? { v: 1, type: 'event', network: a.net, event_id: a['event-id'] || `ev-${Date.now().toString(36)}`, chain_id: a['chain-id'], h: +a.h,
      freeze_lead_blocks: +(a.lead || 24), import_cpu_scale: +(a['cpu-scale'] || 143), issued_at_ms: Date.now() }
  : { v: 1, type: cmd, network: a.net, event_id: a['event-id'], issued_at_ms: Date.now() });
const msg = { payload, sig: sign(null, Buffer.from(payload), key).toString('hex'), key: rawPub(key) };
const r = await fetch(`${a.url.replace(/\/$/, '')}/api/coord/${a.net}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(msg) });
console.log(r.status, await r.text(), JSON.parse(payload).event_id);
process.exit(r.ok ? 0 : 1);
