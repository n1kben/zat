#!/bin/bash
# ralph.sh — HITL (Human-in-the-Loop) Ralph for ZatDB
# Runs a single interactive session in the foreground.
# Usage: ./ralph.sh [model]
claude \
  --permission-mode acceptEdits \
  "@PROMPT.md @progress.txt"
