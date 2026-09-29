# Connect your node to Cutover Mission Control

**For block producers. Takes about 2 minutes. Safe on a live producing node.**

This installs a small **read-only** reporter (the *beacon*) that tells
[Cutover Mission Control](https://control-rehearsal.protonnz.com) whether your node is ready for a future
PulseVM cutover. It **does not** touch nodeos, its config, your keys or block production, and it opens no ports.

---

## 1. Run one command on your node

Log in to the server that runs your **producer** nodeos, then run:

```bash
curl -fsSL https://raw.githubusercontent.com/paulgnz/pulse-cutover/main/tools/beacon-install.sh | sudo bash
```

It works out everything itself: mainnet or testnet (from your chain), your producer account (from
`producer-name` in your nodeos `config.ini`), and your nodeos API address.

> Want to look before you install? Add `-s -- --dry-run` to the end. It shows what it found and your readiness
> checklist, and changes nothing:
> ```bash
> curl -fsSL https://raw.githubusercontent.com/paulgnz/pulse-cutover/main/tools/beacon-install.sh | sudo bash -s -- --dry-run
> ```

## 2. Send one line

The last thing it prints looks like this:

```
  ✓ Done. Last step: send this ONE line to the mission-control operator (it is only a hash):

      network=testnet producer=youraccount token_sha256=3f9c…e21a
```

Send that line to the operator (Telegram or email). It is only a **hash**: your secret token never leaves
your server.

## 3. Watch your node

Once you're approved, your node appears on the dashboard with its readiness checks:
`https://control-rehearsal.protonnz.com/testnet/youraccount`

A few red items are expected today (for example "validator not running" until the Metal node is set up).
That list **is** your to-do list for the cutover.

## 4. Set up your Metal node

One more command installs metalgo and prints your NodeID, BLS key and proof of possession:
[METAL-QUICKSTART.md](METAL-QUICKSTART.md).

---

## If something goes wrong

| You see | Do this |
|---|---|
| `could not find producer-name` | This box isn't your producer, or nodeos uses a custom config path. Add your account: `… \| sudo bash -s -- --producer youraccount` |
| `could not reach nodeos chain API` | Your nodeos listens somewhere else. Add it: `… \| sudo bash -s -- --api http://127.0.0.1:8888` |
| `unknown chain` | Not XPR mainnet or testnet. Add `--network <name>` |
| `checksum mismatch` | Stop. Don't install. Tell the operator. |
| Anything else | Run with `--dry-run` and send the output to the operator. |

**Remove it any time:**

```bash
curl -fsSL https://raw.githubusercontent.com/paulgnz/pulse-cutover/main/tools/beacon-install.sh | sudo bash -s -- --uninstall
```

---

<details>
<summary>What exactly does it install?</summary>

- `/usr/local/bin/pulse-cutover`: the prebuilt binary from the
  [GitHub release](https://github.com/paulgnz/pulse-cutover/releases), checked against its sha256.
- `/etc/pulse-cutover/beacon.toml`: a read-only config. No ceremony can run from it (no cut height, no hooks,
  no target chain).
- `/etc/pulse-cutover/beacon.token`: a random token made on your server, readable only by root.
- `pulse-beacon.service`: a systemd service with a read-only filesystem, reporting every 10 seconds.

What the beacon reads: your nodeos `get_info`, whether the producer API answers locally, whether a Metal
validator service is running, and free disk. What it sends (the dashboard is public, so it is kept minimal):
pass/fail for each check with a short verdict (e.g. "83 GB free", "running"), head/LIB, your account, a node
label you choose (default: its role), and the pulse-cutover version. **Never** sent: keys, tokens, IP addresses,
hostnames, file paths, config contents or unit names.
</details>
