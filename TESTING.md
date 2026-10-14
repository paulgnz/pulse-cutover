# Testing pulse-cutover — the rehearsal program

We are preparing for a real Antelope → PulseVM migration event by rehearsing
the cutover on as many *different* operator setups as possible: docker vs
native nodeos, every nginx and haproxy layout, legacy Hyperion stacks, odd disks, odd
kernels. The tool detects instead of assumes — but it can only detect what
someone has shown it. That someone is you.

## What a test run involves

The [Start here walkthrough](README.md#start-here--the-operator-walkthrough)
in the README, end to end:

1. a spare Ubuntu 20.04/22.04/24.04 box (or any box you're happy to experiment on)
   with a synced testnet nodeos;
2. `pulse-cutover doctor` — the read-only survey (safe to run anywhere,
   including production; that alone is a useful data point for us);
3. `./install.sh` with a rehearsal manifest — start from `examples/ceremony.json`
   (every field documented, the public values filled in) and take the
   coordinator's values (the target chain ids, the pinned plugin and metalgo
   binaries, the shared rehearsal producer identity) from the current rehearsal
   bundle in the Telegram group;
4. `./cutover.sh` — run the ceremony and watch it go LIVE (or ABORT — an
   abort with a journal is just as valuable);
5. `pulse-cutover report` — one command, one sanitized bundle to share.

Budget about 40 minutes for a first run.

## Model checking the ceremony (for contributors)

Besides the scenario tests, `tests/model.rs` drives the real state machine through seeded random scenarios (crashes at
any operation, including right after a pause or a resume took effect; resume answers lost; someone else pausing or
resuming the producer; aborts, re-runs, plain and forced rollbacks, `unhalt`), half of them with `schedule_at_h`, the
mainnet freeze strategy. Invariants are checked at the moment of every action: I1 no pause the agent made is left
behind after a rollback; I2 writes never reopen into a paused producer; I3 someone else's pause is never resumed unless
a release was journaled after that pause began (each release authorizes one resume); I4 nothing resumes the old chain
past the point of no return; I5 the journal always replays; I6 every step terminates and a plain re-run of a STRANDED
journal never strands again; I7 a rollback that provably finished leaves writes open. `r30r10_join_model_*` in
`tests/machine.rs` does the same for `join` on the upstream backend, including the join's own pause windows (the run
asserts it reached them).

CI runs 300 + 60 scenarios. Long runs: `MODEL_SCENARIOS=20000 cargo test --test model -- --nocapture`,
`JOIN_MODEL_SCENARIOS=1000 cargo test --test machine r30r10_join_model -- --nocapture`; replay one with
`MODEL_SEED=<seed>` / `JOIN_MODEL_SEED=<seed>`. The classes found earlier (A, B, R10-1, R10-2, R11-A) are fixed and
kept as regression tests in `tests/model.rs`.

Round 13 tightened the oracles to the code's exact rules: I3 is run-local (a release authorizes one resume attempt,
successful or not, by the run that journaled it, for a pause that began before it) and I7 also fails when a rollback
completes over a paused producer in a ceremony that froze writes. New generators: someone else un-pausing the agent's
own pause, producer-API outages right after a resume, configs without `on_abort`, journals written by pre-round-6
agents, and the operator-abort sequence (a kill, then a plain rollback). Three classes it found are still open in the
source and listed in `OPEN_CLASSES` (F1-A, R13-A, R13-B, each with an `#[ignore]`d regression test): the default run
reports them but fails only on anything else; `MODEL_STRICT=1` fails on every violation.

## It never touches production

- `doctor`, `status`, `scan-contracts` and `report` are strictly read-only.
- `install.sh` installs tools and stages services **on the box you run it
  on**; it never touches your running nodeos and never changes public
  traffic.
- The ceremony's only user-visible action (the URL flip) happens on the
  rehearsal box's own web edge (nginx or haproxy) — and an abort reverts it automatically.
- Your production BP infrastructure is never part of a rehearsal unless you
  deliberately point a manifest at it. Don't — rehearse beside it instead.

## What to share, and where

Run `pulse-cutover report` after every rehearsal — LIVE or ABORTED. It packs
the doctor survey, the ceremony journal and service logs into one tar.gz,
with private keys/tokens/passwords automatically redacted (`[REDACTED-…]`);
it prints the redaction summary and full file list so you can review before
sharing. Then:

- **Telegram** — the cutover testing group:
  <https://t.me/+N1mAvoUDbtVmNTBh> (fastest feedback)
- **GitHub** — a [rehearsal-feedback issue](https://github.com/paulgnz/pulse-cutover/issues/new?template=rehearsal-feedback.md)
  (attach the bundle, quote the printed sha256)

If `install.sh` or `doctor` refused your setup with UNSUPPORTED — that's the
report we want *most*. A bundle from a setup we can't drive yet is exactly
how it becomes a setup we can.

## What testers get

- **Operational familiarity before any real event.** If a real cutover is
  ever scheduled, the operators who rehearsed will already know their literal
  Tuesday: which commands, in which order, what the output looks like, what
  an abort feels like.
- **Your setup, supported.** Report bundles turn directly into detection and
  templating code for your layout — verified on your evidence.
- **Credit.** Every operator whose rehearsal bundle improves the tool gets
  named in the release notes (tell us if you'd rather not be).

## Agent-driven testing is welcome

Pointing an AI coding agent (Claude Code or similar) at this repo and saying
"rehearse a cutover on this box" is a supported path — see
[AGENTS.md](AGENTS.md) for the machine-readable surfaces and the safety
rails an agent must follow. Report bundles from agent-driven runs are just
as useful as hand-driven ones; mention it was agent-driven in the issue.

## Telegram pinned message (copy-paste)

> **Help us test the XPR → PulseVM cutover tooling.**
> pulse-cutover rehearses the whole migration on YOUR setup — same URL, same
> chain_id, zero read downtime — without touching production. What it takes:
> a spare Ubuntu 20.04/22.04/24.04 box with a testnet nodeos, ~40 minutes, and the
> walkthrough at https://github.com/paulgnz/pulse-cutover#start-here--the-operator-walkthrough
> 1. `pulse-cutover doctor` — read-only survey, tells you if your box is READY
> 2. `./install.sh --mode api --manifest ceremony.json` — stages everything (start from examples/ceremony.json; ask here for the rehearsal bundle's values)
> 3. `./cutover.sh` — run the ceremony to LIVE (aborts are safe AND useful)
> 4. `pulse-cutover report` — post the sanitized bundle here (keys auto-redacted)
> Every bundle — LIVE, aborted, or "UNSUPPORTED setup" — makes the real event
> safer. Testers get first-hand operational familiarity + credit in the
> release notes.
