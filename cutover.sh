#!/usr/bin/env bash
# cutover.sh — the day-of command. install.sh prepared the box; this runs the ceremony.
#
#   ./cutover.sh --manifest ceremony.json    # validate + run to LIVE (exit 0) or ABORT (exit 1)
#   ./cutover.sh status                      # current journaled state
#   ./cutover.sh abort                       # stop the agent + roll back (refused unless the journal proves
#                                            #   ignition has not started; see `pulse-cutover help rollback`)
#   ./cutover.sh abort --force-after-ignite  # roll back after IGNITED (coordinator-confirmed only)
#   ./cutover.sh abort --no-journal-i-know   # roll back a box that never ran a ceremony (no journal)
#
# Plain-language streaming: every state transition the agent journals is echoed
# as one operator-readable line. The full evidence is always in the JSONL journal.
set -euo pipefail

CONFIG=${PULSE_CUTOVER_CONFIG:-/etc/pulse-cutover/ceremony.toml}
CMD="${1:-}"

# Read a key from the staged toml; empty (not a failure) when absent — this
# runs under set -euo pipefail, and an optional key must not kill the script.
tomlget(){ { grep -E "^$1 *= *" "$CONFIG" || true; } | head -1 | sed -E 's/^[^=]+= *"?([^"]*)"?.*/\1/'; }

case "$CMD" in
  status)
    exec pulse-cutover status --config "$CONFIG"
    ;;
  abort)
    echo "aborting: stopping the agent (the ratchet means nothing user-visible ran unless FLIPPED/LIVE printed)"
    pkill -f 'pulse-cutover run' || echo "  (no running agent)"
    # The decision is made by `pulse-cutover rollback`, which takes the journal's exclusive lock
    # (so it cannot race an agent that is still shutting down), replays it, and rolls back ONLY when
    # the journal positively shows ignition has not started. It refuses (exit 3) on a missing,
    # corrupt or unreadable journal, and once ignition may have started: after that point resuming
    # the source or reverting routing could create a second writable history, which is a human,
    # fleet-wide decision (--force-after-ignite).
    shift
    set +e
    pulse-cutover rollback --config "$CONFIG" --wait "${PULSE_CUTOVER_ABORT_WAIT:-30}" "$@"
    RC=$?
    set -e
    if [ "$RC" = 3 ]; then
      echo "  NOT rolled back. The agent is stopped; the source was NOT resumed and public routing was NOT reverted."
      echo "  Only if the coordinator has confirmed a fleet-wide rollback, run:"
      echo "    ./cutover.sh abort --force-after-ignite"
    elif [ "$RC" = 0 ] && [ "$(tomlget mode)" = "api" ]; then
      echo "  api mode: if you stopped the source yourself, restart it: $(tomlget start_cmd)"
    fi
    echo "journal: $(tomlget journal_path)"
    exit "$RC"
    ;;
  ""|--manifest)
    ;;
  *)
    echo "usage: ./cutover.sh [--manifest ceremony.json]   run the ceremony (exit 0 = LIVE)"
    echo "       ./cutover.sh status                       show how far the ceremony got"
    echo "       ./cutover.sh abort                        stop safely + undo any public change (before IGNITED)"
    echo "       ./cutover.sh abort --force-after-ignite   roll back after IGNITED (coordinator-confirmed only)"
    exit 2;;
esac

MANIFEST="ceremony.json"
[ "$CMD" = "--manifest" ] && MANIFEST="${2:-ceremony.json}"
[ -f "$CONFIG" ] || { echo "NOT starting: $CONFIG is missing — this box was never staged. Run ./install.sh first (see the README walkthrough, Step 3)."; exit 1; }
command -v jq >/dev/null || { echo "NOT starting: jq is not installed. Fix: apt-get install -y jq"; exit 1; }
command -v pulse-cutover >/dev/null || { echo "NOT starting: the pulse-cutover binary is not installed. Run ./install.sh first."; exit 1; }

# ---------- validate the manifest against the live world ----------
problems=()
if [ -f "$MANIFEST" ]; then
  M_CHAIN=$(jq -r '.ceremony.chain_id' "$MANIFEST")
  SRC_RPC=$(jq -r '.source.rpc_url' "$MANIFEST")
  MODE=$(jq -r '.mode' "$MANIFEST")
  FH=$(jq -r '.ceremony.freeze_height' "$MANIFEST")
  FM=$(jq -r '.ceremony.freeze_margin // 0' "$MANIFEST")
  INFO=$(curl -s -m5 "$SRC_RPC/v1/chain/get_info" || true)
  HEAD=$(echo "$INFO" | jq -r '.head_block_num // 0')
  GOT=$(echo "$INFO" | jq -r '.chain_id // empty')
  [ "$GOT" = "$M_CHAIN" ] || problems+=("source serves chain_id ${GOT:-none}, manifest expects $M_CHAIN")
  if [ "$FH" -gt 0 ] 2>/dev/null; then
    [ "$HEAD" -lt "$FH" ] || problems+=("freeze_height $FH is not in the future (head $HEAD)")
  else
    [ "$FM" -gt 0 ] 2>/dev/null || problems+=("neither freeze_height nor freeze_margin declared")
  fi
  if [ "$MODE" = "api" ]; then
    STOP=$(jq -r '.source.stop_cmd // empty' "$MANIFEST")
    [ -n "$STOP" ] || problems+=("api mode without source.stop_cmd")
    [ -x /opt/pulse-cutover/flip-to-pulsevm.sh ] || problems+=("flip script missing — re-run install.sh")
  fi
else
  echo "note: no $MANIFEST beside me; trusting $CONFIG as staged by install.sh"
fi
STAGED=$(tomlget staged_path)
[ -e "$STAGED" ] && problems+=("staged snapshot $STAGED already exists (R12) — remove it and re-create the target chain if it initialized from it")
GOLDENS=$(tomlget golden_roots)
[ -n "$GOLDENS" ] && [ ! -f "$GOLDENS" ] && problems+=("golden_roots $GOLDENS missing")
if [ ${#problems[@]} -gt 0 ]; then
  echo "NOT starting the ceremony — nothing has run. Problems found:"
  for p in "${problems[@]}"; do echo "  - $p"; done
  echo "Fix the above and run this again. Unsure? 'pulse-cutover doctor' surveys the box;"
  echo "'pulse-cutover report' builds a bundle you can share with us."
  exit 1
fi

MODE=$(tomlget mode); MODE=${MODE:-producer}
JOURNAL=$(tomlget journal_path)
echo "ceremony starting — journal: $JOURNAL"
echo "(^C + './cutover.sh abort' rolls back safely before IGNITED; after IGNITED a failure SEALS — nothing is resumed without a human)"

# ---------- run, translating journal lines to plain language ----------
if [ "$MODE" = "api" ]; then
  FROZEN_MSG="the freeze height is final on the source chain — taking the state snapshot next."
else
  FROZEN_MSG="freeze height reached — production paused, writes are over (reads keep serving)."
fi
set +e
pulse-cutover run --config "$CONFIG" 2>&1 | while IFS= read -r line; do
  case "$line" in
    *"-> ARMED"*)       echo "[ARMED]       watching the source chain; preflight passed." ;;
    *"-> FROZEN"*)      echo "[FROZEN]      $FROZEN_MSG" ;;
    *"-> SNAPSHOTTED"*) echo "[SNAPSHOTTED] state snapshot cut + pinned to one exact block." ;;
    *"-> VERIFIED"*)    echo "[VERIFIED]    snapshot hash + state fingerprints check out; staged for PulseVM." ;;
    *"-> IGNITED"*)     echo "[IGNITED]     PulseVM is up, serving the SAME chain_id, continuing at the cut block." ;;
    *"-> FLIPPED"*)     echo "[FLIPPED]     public /v1 now answered by PulseVM — this was the only user-visible change." ;;
    *"-> LIVE"*)        echo "[LIVE]        ceremony complete. Source retired. Same URL, same chain, new engine." ;;
    *"-> ABORTED"*)     echo "[ABORTED]     ceremony stopped before ignition and rolled back — the source chain is still the real one. The journal has the reason." ;;
    *"-> HALTED"*)      echo "[HALTED]      SEALED: ignition may have started, so nothing was rolled back. The coordinator decides for the fleet." ;;
    *) echo "  $line" ;;
  esac
done
RC=${PIPESTATUS[0]}
set -e
if [ "$RC" = 0 ]; then
  echo ""
  echo "LIVE. Evidence journal: $JOURNAL"
else
  echo ""
  echo "Ceremony did NOT reach LIVE (exit $RC)."
  echo "  - If ABORTED printed: it stopped before ignition started and rolled back; the source chain is the real one."
  echo "  - If HALTED printed: it is SEALED. Ignition may have started on this node, so nothing was"
  echo "    resumed or reverted, and re-running will not continue until an operator clears it. Do not"
  echo "    restart the source on your own: the coordinator decides for the whole fleet, then either"
  echo "    'pulse-cutover unhalt --config $CONFIG --i-understand' (carry on) or"
  echo "    './cutover.sh abort --force-after-ignite' (fleet-wide rollback)."
  echo "Full evidence: $JOURNAL"
  echo "Next: run 'pulse-cutover report' — it builds a sanitized bundle (journal + doctor survey +"
  echo "service logs, keys auto-redacted) to attach to a GitHub issue or post in the Telegram group."
fi
exit "$RC"
