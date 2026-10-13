# Security policy

pulse-cutover runs as root on block producers' servers and stages a chain migration, so a
vulnerability in it, its installers or Cutover Mission Control can reach every node that uses
it. Please report problems privately first.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting for this repository:
<https://github.com/paulgnz/pulse-cutover/security/advisories/new>. It reaches the maintainer
only; nothing is published until a fix exists.

Please include what you can of: the version or commit, the component (`pulse-cutover` binary,
`install.sh`, `cutover.sh`, `tools/*.sh`, `control/`, `gateway/`, `federator/`), steps to
reproduce, and what an attacker gains. A report bundle (`pulse-cutover report`) is welcome; it
redacts keys and tokens before anything is written.

What to expect: an acknowledgement within 7 days, a fix or mitigation in the next release
candidate for anything that affects a running node or a ceremony, and credit in the release
notes unless you prefer otherwise.

## Scope

In scope: anything that lets a third party change what a node installs or runs (installer and
release integrity, the pinned checksums, the ceremony manifest handling), start, alter or
abort a ceremony on a node it does not own, read private keys or beacon tokens, or mislead the
fleet verdict on mission control.

Out of scope: the PulseVM node itself and metalgo (report those to
[MetalBlockchain](https://github.com/MetalBlockchain)), and findings that need root on the
node already.

## Supported versions

Only the newest release candidate on the
[releases page](https://github.com/paulgnz/pulse-cutover/releases) is supported; a rehearsal
fleet must run one version (`docs/OPERATOR-QUICKSTART.md`).
