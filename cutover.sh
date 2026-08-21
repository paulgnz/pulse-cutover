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

tomlget(){ grep -E "^$1 *= *" "$CONFIG" | head -1 | sed -E 's/^[^=]+= *"?([^"]*)"?.*/\1/'; }

case "$CMD" in
  status)
    exec pulse-cutover status --config "$CONFIG"
    ;;
  abort)
    echo "aborting: stopping the agent (the ratchet means nothing user-visible ran unless FLIPPED/LIVE printed)"
    pkill -f 'pulse-cutover run' || echo "  (no running agent)"
    MODE=$(tomlget mode); MODE=${MODE:-producer}
    if [ "$MODE" = "api" ]; then
      REVERT=$(tomlget revert_cmd)
      if [ -n "$REVERT" ]; then echo "  reverting public /v1 to nodeos: $REVERT"; sh -c "$REVERT" || true; fi
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
    echo "usage: ./cutover.sh [--manifest ceremony.json] | status | abort"; exit 2;;
esac

MANIFEST="ceremony.json"
[ "$CMD" = "--manifest" ] && MANIFEST="${2:-ceremony.json}"
[ -f "$CONFIG" ] || { echo "ABORT: $CONFIG missing — run ./install.sh first"; exit 1; }
command -v jq >/dev/null || { echo "ABORT: jq missing"; exit 1; }
command -v pulse-cutover >/dev/null || { echo "ABORT: pulse-cutover not installed — run ./install.sh"; exit 1; }

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
  echo "NOT starting the ceremony:"
  for p in "${problems[@]}"; do echo "  - $p"; done
  exit 1
fi

JOURNAL=$(tomlget journal_path)
echo "ceremony starting — journal: $JOURNAL"
echo "(the source chain stays authoritative until the last step; ^C + './cutover.sh abort' is always safe before FLIPPED)"

# ---------- run, translating journal lines to plain language ----------
set +e
pulse-cutover run --config "$CONFIG" 2>&1 | while IFS= read -r line; do
  case "$line" in
    *"-> ARMED"*)       echo "[ARMED]       watching the source chain; preflight passed. $line" ;;
    *"-> FROZEN"*)      echo "[FROZEN]      H reached — writes rejected at the API edge (source keeps serving reads)." ;;
    *"-> SNAPSHOTTED"*) echo "[SNAPSHOTTED] state snapshot cut + pinned by block id." ;;
    *"-> VERIFIED"*)    echo "[VERIFIED]    sha256 + dual-import fingerprints check out; snapshot staged for PulseVM." ;;
    *"-> IGNITED"*)     echo "[IGNITED]     PulseVM is up, serving the SAME chain_id at the cut height." ;;
    *"-> FLIPPED"*)     echo "[FLIPPED]     public /v1 now answered by PulseVM — this was the only user-visible change." ;;
    *"-> LIVE"*)        echo "[LIVE]        ceremony complete. Source retired. Same URL, same chain, new engine." ;;
    *"-> ABORTED"*)     echo "[ABORTED]     ceremony rolled back — source chain remains authoritative. See journal." ;;
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
  echo "Ceremony did NOT reach LIVE (exit $RC). Journal with full evidence: $JOURNAL"
fi
exit "$RC"
