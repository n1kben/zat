#!/bin/bash
# ralph-afk.sh — AFK (unattended) Ralph for ZatDB
# Runs in a Docker sandbox with streaming output.
# Usage: ./ralph-afk.sh [iterations] [model]
set -e

ITERATIONS=${1:-5}
MODEL=${2:-claude-opus-4-6}
LOGDIR="logs"
mkdir -p "$LOGDIR"

PROMPT=$(cat PROMPT.md)

for ((i=1; i<=ITERATIONS; i++)); do
  TIMESTAMP=$(date +%Y%m%d-%H%M%S)
  LOGFILE="$LOGDIR/ralph-${TIMESTAMP}-iter${i}.log"
  tmpfile=$(mktemp)

  echo "=== Ralph AFK iteration $i/$ITERATIONS ==="

  docker sandbox run claude -- \
    --model "$MODEL" \
    --verbose \
    --output-format stream-json \
    -p "$PROMPT" \
    2>&1 \
    | grep --line-buffered '^{' \
    | tee "$tmpfile" \
    | jq --unbuffered -rj '
        select(.type == "assistant")
        | .message.content[]
        | select(.type == "text")
        | .text
        | gsub("\n"; "\r\n")
      ' 2>/dev/null

  # Save full log
  cp "$tmpfile" "$LOGFILE"

  # Check for completion
  if jq -e 'select(.type == "result") | .result // "" | test("<promise>COMPLETE</promise>")' "$tmpfile" >/dev/null 2>&1; then
    echo ""
    echo "All tasks complete!"
    rm -f "$tmpfile"
    exit 0
  fi

  rm -f "$tmpfile"
done

echo "Completed $ITERATIONS iterations. Check TASKS.md for progress."
