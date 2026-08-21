#!/usr/bin/env bash
# cutover.sh — the day-of command. install.sh prepared the box; this runs the ceremony.
#
#   ./cutover.sh --manifest ceremony.json    # validate + run to LIVE (exit 0) or ABORT (exit 1)
#   ./cutover.sh status                      # current journaled state
#   ./cutover.sh abort                       # stop the agent + roll the public edge back
#
# Plain-language streaming: every state transition the agent journals is echoed
# as one operator-readable line. The full evidence is always in the JSONL journal.
set -euo pipefail

CONFIG=/etc/pulse-cutover/ceremony.toml
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
    MODE=$(tomlget mode); MODE=${MODE:-producer}
    if [ "$MODE" = "api" ]; then
      # Revert EVERY staged flip ([flip].revert_cmd and, in hyperion mode,
      # [hyperion].revert_cmd — the /v2 swap back to the pre-cut upstream).
      { grep -E '^revert_cmd *= *' "$CONFIG" || true; } | sed -E 's/^[^=]+= *"?([^"]*)"?.*/\1/' | while IFS= read -r REVERT; do
        [ -n "$REVERT" ] && { echo "  reverting: $REVERT"; sh -c "$REVERT" || true; }
      done
      echo "  source nodeos was never paused by the agent; if your stop command already ran, restart it:"
      echo "    $(tomlget start_cmd)"
    else
      PROD=$(tomlget producer_api_url)
      echo "  resuming source producer at $PROD"
      curl -s -X POST "$PROD/v1/producer/resume" >/dev/null || true
    fi
    echo "journal: $(tomlget journal_path)"
    exit 0
    ;;
  ""|--manifest)
    ;;
  *)
    echo "usage: ./cutover.sh [--manifest ceremony.json]   run the ceremony (exit 0 = LIVE)"
    echo "       ./cutover.sh status                       show how far the ceremony got"
    echo "       ./cutover.sh abort                        stop safely + undo any public change"
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
echo "(the source chain stays authoritative until the last step; ^C + './cutover.sh abort' is always safe before FLIPPED)"

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
    *"-> ABORTED"*)     echo "[ABORTED]     ceremony stopped safely and rolled back — the source chain is still the real one. The journal has the reason." ;;
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
  echo "Ceremony did NOT reach LIVE (exit $RC) — it stopped safely; your source chain is untouched"
  echo "unless FLIPPED printed above (and an abort at FLIPPED reverts the swap automatically)."
  echo "Full evidence: $JOURNAL"
  echo "Next: run 'pulse-cutover report' — it builds a sanitized bundle (journal + doctor survey +"
  echo "service logs, keys auto-redacted) to attach to a GitHub issue or post in the Telegram group."
fi
exit "$RC"
