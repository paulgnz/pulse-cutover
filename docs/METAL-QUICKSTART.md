# Set up your Metal node (and get your NodeID)

**For block producers. One script, about 3 minutes plus sync time.** Run it on the server that will be your
PulseVM validator. That can be the same box as your producer nodeos if it has room (8 GB+ RAM, 100 GB+ free).

Get the script from a **release tag**, read it, run it:

```bash
V=v0.5.0-rc.28
git clone --branch "$V" --depth 1 https://github.com/paulgnz/pulse-cutover && cd pulse-cutover
less tools/metal-install.sh        # you are about to run this as root: read it (the pins are near the top)
sudo bash tools/metal-install.sh
```

Why a tag and not `main`: the metalgo version, the checksums of Metal's release tarballs, the Ubuntu 20.04 build,
the Docker image digest and the source commit are **pinned inside the script**, so what you install is what was
reviewed when that tag was cut, verified by git against the tag. (From v0.5.0-rc.29 each release also attaches the
script and lists it in `sha256sums.txt`, so a plain download can be checked with
`sha256sum -c sha256sums.txt --ignore-missing`.) The old `curl … main … | sudo bash` form took the checksums from
mission control's unsigned manifest; it is gone.

What you trust by running it: GitHub serving the tag; the official
[MetalBlockchain/metalgo releases](https://github.com/MetalBlockchain/metalgo/releases) (downloaded and checked
against the pinned sha256 before anything changes); and mission control for exactly two things that are not
checksums: which Metal network your XPR chain maps to, and the PulseVM subnet/chain/VM ids once they exist. A
manifest that names another metalgo version or checksum is **refused**, not followed (see below).

It works out mainnet or testnet from your nodeos. **Everything is downloaded and verified before anything on
the box changes** (sha256, the binary's reported version and plugin protocol). Then it swaps the binary in, starts
the `metalgo` service and checks the running node's version, network and identity. If an upgrade fails at any
point, the previous binary and config are put back and the previous node is started again.
It **fails closed**: if the manifest can't be fetched, doesn't match your chain, or disagrees with the script's
pins, it stops instead of installing something else.

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
   key files and restore-tested (the archived certificate must derive the NodeID the node runs as), and every
   run makes a new timestamped one (never overwritten). At a terminal it then asks you to confirm you copied it
   off the server (`--i-have-backed-up` skips the question for automation); the answer is recorded as `custody`
   in `identity.json`, and `--check` reminds you while it is unconfirmed. If you ran it with `sudo`, a second
   copy is put in your home directory, readable only by you, so you can `scp` it as yourself. **Both copies
   contain private keys**: delete the home-directory copy once it is stored safely;
3. how to check sync;
4. **one line to send to the operator** (`metal network=… producer=… node_id=… bls=… pop=…`, public values only);
5. what you'll need later to register.

Everything is also saved in `/etc/metalgo/identity.txt`, and machine-readable for scripts and agents in
`/etc/metalgo/identity.json` (schema `metal-identity-v1`, public values only; `pin_source` records whether the
script's pins or a command-line override chose the version).

> Want to look first? `sudo bash tools/metal-install.sh --dry-run` installs and changes nothing (it does fetch
> the manifest, to validate it and compare it with the pins). No nodeos on this box? Add `--metal tahoe`
> (testnet) or `--metal mainnet`.

## The three things you must not miss

1. **Back up the identity archive off the server.** It holds three files:

   | File | What it is | If you lose it |
   |---|---|---|
   | `staker.key` + `staker.crt` | your TLS identity, which *is* your NodeID | you get a new NodeID and must register again |
   | `signer.key` | your BLS signing key (what the proof of possession proves) | you must register a new BLS key + PoP |

   Chain data doesn't need backing up: it re-syncs. **Never run two nodes with the same keys at the same time.**
2. **Open 9651/tcp to the internet.** Peers reach you there. The installer tests it from outside (mission
   control dials back to your server's IP on 9651 only) and, if it's closed, names the **likely** firewall layer
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
   Re-check any time without changing anything: `sudo bash tools/metal-install.sh --check` (it queries the running
   node's live identity). The installer never opens a firewall port on its own: add `--open-port` to have it add
   the rule to this server's own firewall (ufw, firewalld or iptables; it can't change your provider's).
   The HTTP API (9650) stays on 127.0.0.1.
3. **Stay on the pinned version.** Tahoe activated Granite on 2026-09-21. Nodes older than `v1.14.2-tahoe`
   fall off the network. To upgrade, check out the newer pulse-cutover release tag that pins the new metalgo and
   re-run the same script: it keeps your keys.

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
script, reproducible with `tools/build-metalgo-glibc231.sh`). It still runs as the normal `metalgo` service.
You'll see `install method: compat`. Upgrading to 22.04/24.04 is still a good idea (20.04 is past standard support),
and after an upgrade re-running the installer switches you to Metal's official binary.

## The pins, and the network manifest

The script's trust root is the `PIN_*` block near its top (today: `v1.14.2-tahoe`, its amd64/arm64 release
tarball checksums, the 20.04 build, the Docker image digest, the tag's commit, plugin protocol 45). Changing a pin
is a reviewed commit that ships in a release tag; `tools/test/run.sh` checks the block is complete and well-formed.
Metal **mainnet has no pins yet** (no mainnet cutover is scheduled): a mainnet install is refused until a tag pins
it, or until you take responsibility with the override below.

Mission control publishes which Metal network, subnet, blockchain and VM each XPR network maps to:

- all networks: <https://control-rehearsal.protonnz.com/api/manifest>
- one network: `https://control-rehearsal.protonnz.com/api/manifest/testnet` (or `mainnet`, `rehearsal`)

| Field | What the installer does with it |
|---|---|
| `network`, `network_id`, `xpr_network`, `xpr_chain_id` | checked against this box's XPR chain and `--metal`; a mismatch refuses |
| `subnet_id`, `blockchain_id`, `vm_id` | the PulseVM chain to track (`null` until it exists); re-running the installer once they appear makes your node track it |
| `metalgo_version`, `sha256_linux_{amd64,arm64}`, `compat_glibc231_*`, `docker_image_digest`, `metalgo_commit`, `rpcchainvm_protocol` | **compared with the script's pins only**; any disagreement refuses the install with the exact field. They are never a source of checksums |
| `pulsevm_version`, `plugin_sha256` | not used by this script (the PulseVM plugin is staged by `install.sh` from the ceremony manifest) |
| `status`, `upgrades_note` | printed |

The manifest is unsigned preparation metadata, which is why it cannot choose what runs on your box. When the network
really has moved to a newer metalgo, the fix is a newer pulse-cutover tag; if you must install the manifest's
version before one exists, type the checksum yourself:

```bash
sudo bash tools/metal-install.sh --metalgo-version v1.14.3-tahoe --metalgo-sha256 <sha256 of metalgo-linux-amd64-v1.14.3-tahoe.tar.gz>
```

That override installs the official release binary only (no 20.04 build, Docker image or source build), skips the
pin comparison, and is recorded as `pin_source=override` in `identity.json` and the install marker.

## Troubleshooting

| You see | Do this |
|---|---|
| `the network manifest disagrees with the pins in this script` | The network moved on, or the manifest is wrong. Check out the newer release tag and re-run; or, if the operator confirms the version, use `--metalgo-version` + `--metalgo-sha256` |
| `this script pins no metalgo release for Metal mainnet` | Expected today. Wait for a tag that pins it, or override with the announced version and checksum |
| `glibc … needs 2.34+` | Only on ARM or very old systems: upgrade to Ubuntu 22.04/24.04 |
| `port 9650 is already in use` | Another metalgo/avalanchego runs here. Stop it, or use that one |
| `checksum mismatch` | Stop. Don't install. Tell the operator |
| `metalgo did not answer` | `journalctl -u metalgo -n 50` and send it to the operator |
| Advertised address is wrong | Re-run as `sudo METAL_PUBLIC_IP=<your IPv4> bash tools/metal-install.sh` |

Remove the service (keeps your keys and data): `sudo bash tools/metal-install.sh --uninstall`. It only removes a
service this script installed.

**Already running metalgo some other way?** The installer refuses to touch a `metalgo` service it did not install.
`--adopt` takes it over, but only when it uses the standard `/var/lib/metalgo` data dir; a custom layout is refused
rather than risk the node identity.

**Mission control unreachable?** The installer stops: without the manifest it does not know which PulseVM chain to
track. On testnet only, `--no-manifest` installs the script's pinned metalgo without the subnet ids (the node syncs
the Tahoe primary network; re-run once mission control is back). The Docker method uses the digest pinned in the
script, and `--build` checks out exactly the pinned commit and verifies the Go toolchain's checksum (a tag alone is
never trusted, since tags can move).

**Upgrades are transactions.** Everything is downloaded and verified first. From the moment the old metalgo is
stopped until the new one is verified (same NodeID **and** same BLS key, expected version and network), any
failure puts the previous binary, config and unit back, starts the previous node and checks it answers with its
old NodeID and BLS key. `--adopt` also refuses a node whose key files are configured anywhere other than
`/var/lib/metalgo/staking/`, or that uses ephemeral or inline keys, since this script's config would change its
identity.
