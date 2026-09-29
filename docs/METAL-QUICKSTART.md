# Set up your Metal node (and get your NodeID)

**For block producers. One command, about 3 minutes plus sync time.** Run it on the server that will be your
PulseVM validator. That can be the same box as your producer nodeos if it has room (8 GB+ RAM, 100 GB+ free).

```bash
curl -fsSL https://raw.githubusercontent.com/paulgnz/pulse-cutover/main/tools/metal-install.sh | sudo bash
```

It works out mainnet or testnet from your nodeos and installs the metalgo version that
[the network manifest](#the-network-manifest) pins. **Everything is downloaded and verified before anything on
the box changes** (sha256, the binary's reported version and plugin protocol). Then it swaps the binary in, starts
the `metalgo` service and checks the running node's version, network and identity. If an upgrade fails at any
point, the previous binary and config are put back and the previous node is started again.
It **fails closed**: if the manifest can't be fetched, doesn't match your chain, or has no checksum for your
platform, it stops instead of installing an unpinned binary.

The result is a **prepared Metal node**. Being admitted as a validator (registered, funded) and being ready for a
cutover are separate steps with their own checks.

Then it prints:

```
  ── Your validator identity (PUBLIC: share these to register) ──
  NodeID               NodeID-…
  BLS public key       0x…
  Proof of possession  0x…
  Advertised address   203.0.113.10:9651

  ── BACK UP YOUR KEYS NOW (PRIVATE: never share, never commit) ──
  …/staking/staker.key + staker.crt   → your NodeID
  …/staking/signer.key                → your BLS key
  All three are in:  /root/metalgo-identity-NodeID-…-<timestamp>.tar.gz   (PRIVATE KEYS)
```

Then a numbered **NEXT STEPS** list tells you exactly what to do:
1. whether port 9651 is reachable from the internet (mission control dials back to your server's IP on 9651
   only, via `/api/reach`) and how to fix it if not;
2. the exact `scp` command to copy your key archive off the server. The archive is verified against the live
   key files, and every run makes a new timestamped one (never overwritten). If you ran it with `sudo`, a second
   copy is put in your home directory, readable only by you, so you can `scp` it as yourself. **Both copies
   contain private keys**: delete the home-directory copy once it is stored safely;
3. how to check sync;
4. **one line to send to the operator** (`metal network=… producer=… node_id=… bls=… pop=…`, public values only);
5. what you'll need later to register.

Everything is also saved in `/etc/metalgo/identity.txt`, and machine-readable for scripts and agents in
`/etc/metalgo/identity.json` (schema `metal-identity-v1`, public values only).

> Want to look first? Add `-s -- --dry-run`. No nodeos on this box? Add `-s -- --metal tahoe` (testnet) or
> `-s -- --metal mainnet`.

## The three things you must not miss

1. **Back up the identity archive off the server.** It holds three files:

   | File | What it is | If you lose it |
   |---|---|---|
   | `staker.key` + `staker.crt` | your TLS identity, which *is* your NodeID | you get a new NodeID and must register again |
   | `signer.key` | your BLS signing key (what the proof of possession proves) | you must register a new BLS key + PoP |

   Chain data doesn't need backing up: it re-syncs. **Never run two nodes with the same keys at the same time.**
2. **Open 9651/tcp to the internet.** Peers reach you there. The installer tests it from outside (mission
   control dials back to your server's IP on 9651 only) and, if it's closed, tells you **which** firewall blocks it
   and the exact fix:

   | Blocked by | Fix it prints |
   |---|---|
   | ufw on this server | `sudo ufw allow 9651/tcp` |
   | firewalld | `sudo firewall-cmd --permanent --add-port=9651/tcp && sudo firewall-cmd --reload` |
   | iptables (default-drop) | `sudo iptables -I INPUT -p tcp --dport 9651 -j ACCEPT` + how to keep it after reboot |
   | nftables (policy drop) | add `tcp dport 9651 accept` to the input chain |
   | nothing local: your provider's firewall | allow inbound TCP 9651 in the provider's panel (Vultr firewall group, Hetzner Firewalls, AWS security group, …) |
   | metalgo not listening / localhost only | restart metalgo / remove the listen override |

   These are **likely** causes: the test dials the address mission control sees your request come from, and the
   local firewall inspection is a heuristic (NAT, IPv6 and multi-homed hosts can differ).
   Re-check any time without changing anything: `… | sudo bash -s -- --check` (it queries the running node's live
   identity). The installer never opens a firewall port on its own: add `--open-port` to have it add the rule to
   this server's own firewall (ufw, firewalld or iptables; it can't change your provider's).
   The HTTP API (9650) stays on 127.0.0.1.
3. **Stay on the pinned version.** Tahoe activated Granite on 2026-09-21. Nodes older than `v1.14.2-tahoe`
   fall off the network. To upgrade, re-run the same command: it keeps your keys.

## What you'll need to register (later, when the event is announced)

- The NodeID, BLS public key and proof of possession printed above.
- A Metal **P-Chain address** you control (`P-tahoe1…` on testnet, `P-metal1…` on mainnet). It receives any
  unused validator balance and can disable the validator. Use a key you already back up, not one on this server.
- **METAL on the P-Chain** to prepay the validator's continuous fee (the amount is announced with the event).

The script never stakes, registers, funds or spends anything, and never touches nodeos.

## Ubuntu 20.04

Supported natively, without Docker. Metal's official binaries need glibc 2.34 (Ubuntu 22.04+), so on 20.04 the
installer uses the same metalgo source tag compiled on Ubuntu 20.04
([release](https://github.com/paulgnz/pulse-cutover/releases/tag/metalgo-glibc2.31-1), checksum pinned in the
manifest, reproducible with `tools/build-metalgo-glibc231.sh`). It still runs as the normal `metalgo` service.
You'll see `install method: compat`. Upgrading to 22.04/24.04 is still a good idea (20.04 is past standard support),
and after an upgrade re-running the installer switches you to Metal's official binary.

## The network manifest

Nobody should copy chain IDs by hand. Mission control publishes which Metal network, subnet, blockchain and VM
each XPR network maps to, plus the pinned metalgo version and checksums:

- all networks: <https://control-rehearsal.protonnz.com/api/manifest>
- one network: `https://control-rehearsal.protonnz.com/api/manifest/testnet` (or `mainnet`, `rehearsal`)

| Field | Meaning |
|---|---|
| `network`, `network_id`, `hrp` | Metal network (Tahoe = 5, mainnet = 1) and address prefix |
| `metalgo_version`, `sha256_linux_{amd64,arm64}`, `docker_image` | what the installer installs and verifies |
| `compat_glibc231_url`, `compat_glibc231_sha256` | the Ubuntu 20.04 build of the same version |
| `rpcchainvm_protocol` | plugin protocol that metalgo speaks; the PulseVM plugin must match |
| `subnet_id`, `blockchain_id`, `vm_id` | the PulseVM chain to track (`null` until it exists) |
| `pulsevm_version`, `plugin_sha256` | the PulseVM plugin build to run |
| `status`, `upgrades_note` | plain-language state and deadlines |

When a PulseVM chain is created for your network, the manifest gets its IDs. Re-running the installer then
makes your node track it.

## Troubleshooting

| You see | Do this |
|---|---|
| `glibc … needs 2.34+` | Only on ARM or very old systems: upgrade to Ubuntu 22.04/24.04 |
| `port 9650 is already in use` | Another metalgo/avalanchego runs here. Stop it, or use that one |
| `checksum mismatch` | Stop. Don't install. Tell the operator |
| `metalgo did not answer` | `journalctl -u metalgo -n 50` and send it to the operator |
| Advertised address is wrong | Re-run as `… \| sudo METAL_PUBLIC_IP=<your IPv4> bash` |

Remove the service (keeps your keys and data): `… | sudo bash -s -- --uninstall`. It only removes a service this
script installed.

**Already running metalgo some other way?** The installer refuses to touch a `metalgo` service it did not install.
`--adopt` takes it over, but only when it uses the standard `/var/lib/metalgo` data dir; a custom layout is refused
rather than risk the node identity.

**Mission control unreachable?** The installer stops: it does not install without the published manifest. On
testnet only, `--allow-unpinned` falls back to the checksum pins built into the script. The Docker method needs a
digest-pinned image in the manifest, and `--build` checks out the exact pinned commit and verifies the Go
toolchain's checksum.
