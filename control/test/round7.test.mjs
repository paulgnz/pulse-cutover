// node --test control/test/*.test.mjs
// Round 7: producer-API exposure probe (/api/exposure). It may only ever dial the caller's own address.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { probeProducerApi, resolvePublic } from '../lib.mjs';

const IP = '103.96.110.52';
// Stand-in for safeRequest: resolves the host through the probe's lookup exactly as safeRequest does, records
// where it would connect, and answers from a table.
function fake(answers) {
  const dialed = [];
  const request = async (url, opts) => {
    assert.equal(opts.maxRedirects, 0);
    assert.equal(opts.method, 'POST');
    const u = new URL(url);
    const ip = await resolvePublic(u.hostname, opts.lookup);
    dialed.push(ip);
    const a = answers[`${u.protocol}//${u.host}`];
    if (!a) throw Object.assign(new Error('connect ECONNREFUSED'), { code: 'ECONNREFUSED' });
    return { status: a[0], body: Buffer.from(a[1]), headers: new Map() };
  };
  return { request, dialed };
}
const dnsTable = { 'tn1.example': [IP], 'other.example': ['8.8.8.8'], 'multi.example': ['8.8.4.4', IP] };
const lookup = async (h) => (dnsTable[h] || []).map((address) => ({ address, family: 4 }));

test('open producer API on :8888 is reported; a 403 or a non-boolean 200 is not', async () => {
  const { request, dialed } = fake({ [`http://${IP}:8888`]: [201, 'false'], [`http://${IP}`]: [403, '<html>'], 'https://tn1.example': [200, '{"a":1}'] });
  const r = await probeProducerApi(IP, ['https://tn1.example'], { request, lookup });
  assert.deepEqual(r, { exposed: true, open: [`http://${IP}:8888`], checked: 3 });
  assert.ok(dialed.every((d) => d === IP));
});

test('guarded server: nothing open', async () => {
  const { request } = fake({ [`http://${IP}:8888`]: [403, 'no'], [`http://${IP}`]: [404, '{"code":404}'], 'https://tn1.example': [403, 'no'] });
  assert.deepEqual(await probeProducerApi(IP, ['https://tn1.example'], { request, lookup }), { exposed: false, open: [], checked: 3 });
});

test('bp.json endpoints that resolve elsewhere, or IP literals of other hosts, are never dialed', async () => {
  const { request, dialed } = fake({ 'https://other.example': [200, 'true'], 'http://8.8.8.8:8888': [200, 'true'] });
  const r = await probeProducerApi(IP, ['https://other.example', 'http://8.8.8.8:8888', 'http://10.0.0.1:8888'], { request, lookup });
  assert.equal(r.exposed, false);
  assert.ok(dialed.every((d) => d === IP), `dialed ${dialed}`);
  assert.equal(r.checked, 2); // only the caller's own :8888 and :80 (both refused)
});

test('a hostname with several addresses is pinned to the caller\'s one', async () => {
  const { request, dialed } = fake({ 'https://multi.example': [200, 'true'] });
  const r = await probeProducerApi(IP, ['https://multi.example'], { request, lookup });
  assert.deepEqual(r.open, ['https://multi.example']);
  assert.ok(dialed.every((d) => d === IP));
});
