// createLocal.cjs — per-iteration subnet+chain on the LOCAL metalgo network
// (network-id 12345, sybil protection off — the tmpnet pattern; no P-chain
// paperwork, infinite resets by wiping the local data dir). Signs with the
// well-known local-genesis ewoq key. Prints SUBNET=… and BID=….
const fs = require('fs');
const { Metal } = require('@metalblockchain/metaljs');
const EWOQ = 'PrivateKey-ewoqjP7PxY4yr3iLTpLisriqt94hdyDFNgchSxGGztUrTXtNN';
const VMID_INPUT = 'sMoBoVBs4qGMgvThg2qDvMLoNQWi3MUBxrkSTt6N6GqUHNX4a'; // metaljs maps this to on-chain vmID snFL9jZ… (same as the Tahoe chain)
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
(async () => {
  const metal = new Metal('127.0.0.1', 9655, 'http', 12345);
  const p = metal.PChain();
  const kc = p.keyChain(); kc.importKey(EWOQ);
  const addrs = kc.getAddressStrings();
  async function waitTx(txid) {
    for (let i = 0; i < 60; i++) {
      const st = await p.getTxStatus(txid);
      const s = st.status || st;
      if (s === 'Committed') return;
      if (s === 'Dropped' || s === 'Aborted') throw new Error(`tx ${txid} ${s}: ${JSON.stringify(st)}`);
      await sleep(500);
    }
    throw new Error(`tx ${txid} not committed in time`);
  }
  let { utxos } = await p.getUTXOs(addrs);
  const sub = await p.buildCreateSubnetTx(utxos, addrs, addrs, addrs, 1);
  const subnetID = await p.issueTx(sub.sign(kc));
  await waitTx(subnetID);
  ({ utxos } = await p.getUTXOs(addrs));
  const genesis = fs.readFileSync('/root/api-cutover/loop/genesis.json', 'utf8');
  const subnetAuth = [[0, kc.getAddresses()[0]]];
  const ch = await p.buildCreateChainTx(utxos, addrs, addrs, subnetID, 'pulse loop', VMID_INPUT, [], genesis, undefined, undefined, subnetAuth);
  const bid = await p.issueTx(ch.sign(kc));
  await waitTx(bid);
  console.log('SUBNET=' + subnetID);
  console.log('BID=' + bid);
})().catch((e) => { console.error('ERR:', e.message || e); process.exit(1); });
