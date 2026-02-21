#!/bin/bash
# init.sh — Verify the ZatDB development environment is ready.
# Run this at the start of a session to confirm baseline is green.
set -e

echo "=== ZatDB Environment Check ==="

# Check Zig is available
if ! command -v zig &>/dev/null; then
  echo "Zig not found. Attempting install..."
  if command -v apt-get &>/dev/null; then
    apt-get update -qq && apt-get install -y -qq wget xz-utils
    ZIG_VERSION="0.15.2"
    wget -q "https://ziglang.org/builds/zig-linux-x86_64-${ZIG_VERSION}.tar.xz" -O /tmp/zig.tar.xz
    tar -xf /tmp/zig.tar.xz -C /usr/local
    ln -sf /usr/local/zig-linux-x86_64-${ZIG_VERSION}/zig /usr/local/bin/zig
    rm /tmp/zig.tar.xz
  else
    echo "ERROR: Cannot auto-install Zig on this platform. Install manually."
    exit 1
  fi
fi

echo "Zig version: $(zig version)"

# Check git
if ! command -v git &>/dev/null; then
  echo "ERROR: git not found"
  exit 1
fi
echo "Git: OK"

# Verify build
echo "Running zig build..."
zig build
echo "Build: OK"

# Verify tests
echo "Running zig build test..."
zig build test
echo "Tests: OK"

echo "=== Environment ready ==="
