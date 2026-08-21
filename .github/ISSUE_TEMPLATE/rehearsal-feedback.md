---
name: Rehearsal feedback
about: "Testing a cutover rehearsal with us? Run `pulse-cutover report` and attach the bundle."
title: "[rehearsal] <your operator name / box role>"
labels: rehearsal-feedback
---

<!-- Prefer chat? The same bundle is welcome in the testing Telegram group:
     https://t.me/+N1mAvoUDbtVmNTBh — this issue template is the on-the-record
     version. What testing involves: TESTING.md in the repo. -->

## What happened

<!-- One or two sentences: which mode (bp / api / hyperion), how far the
     ceremony got (last state printed), and what surprised you.
     "doctor said UNSUPPORTED and I never got further" is a complete answer
     — that's the exact feedback we want. -->

## The bundle

Run this on the box and attach the resulting `.tar.gz` here:

```sh
pulse-cutover report            # add --paranoid to placeholder hostnames/IPs too
```

The command prints exactly what was redacted (private keys, tokens,
passwords come out as `[REDACTED-<type>]`) and the full file list — review it
before attaching. Paste the printed **sha256** below so we know the bundle
arrived intact:

```
sha256:
```

<!-- If `pulse-cutover report` itself failed, paste its output instead, plus
     `pulse-cutover doctor` output — that is precisely the kind of setup we
     want to learn about. -->

## Your setup, in one line

<!-- e.g. "docker nodeos behind nginx with TLS domains, legacy Hyperion on
     pm2" — doctor's verdict section says the same thing, this is just the
     human summary. -->

## What you expected

<!-- Especially for UNSUPPORTED verdicts: what would "supported" look like
     on your box? -->
