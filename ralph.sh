#!/bin/bash
# ralph.sh — Ralph Wiggum harness for ZatDB
# Usage: ./ralph.sh [iterations] [model]
set -e

ITERATIONS=${1:-1}
MODEL=${2:-claude-opus-4-6}
LOGDIR="logs"
mkdir -p "$LOGDIR"

PROMPT="Read CLAUDE.md, TASKS.md, IMPLEMENTAITON.md, and progress.txt.
Then:
1. Review progress.txt to understand what's been done.
2. Pick the next unfinished task from TASKS.md (respect dependencies).
3. Read the relevant section of IMPLEMENTAITON.md for detailed specs.
4. Implement the task. Write clean, idiomatic Zig code.
5. Run 'zig build' — fix any compilation errors.
6. Run 'zig build test' — fix any test failures.
7. Update the task status in TASKS.md to [x].
8. Append your progress to progress.txt (task, files changed, test results, decisions).
9. Git commit with a descriptive message.
ONLY WORK ON A SINGLE TASK PER SESSION.
If all tasks in TASKS.md are [x], output <promise>COMPLETE</promise>."

for ((i=1; i<=ITERATIONS; i++)); do
  TIMESTAMP=$(date +%Y%m%d-%H%M%S)
  LOGFILE="$LOGDIR/ralph-${TIMESTAMP}-iter${i}.log"

  echo "=== Ralph iteration $i/$ITERATIONS ==="

  if [[ $ITERATIONS -eq 1 ]]; then
    PROMPT_ARGS=()
  else
    PROMPT_ARGS=(-p "$PROMPT")
  fi

  result=$(docker sandbox run claude \
    --model "$MODEL" \
    "${PROMPT_ARGS[@]}" \
    2>&1 | tee "$LOGFILE")

  if [[ "$result" == *"<promise>COMPLETE</promise>"* ]]; then
    echo "All tasks complete!"
    exit 0
  fi
done

echo "Completed $ITERATIONS iterations. Check TASKS.md for progress."
