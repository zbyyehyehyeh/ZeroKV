#!/bin/bash
# 【关键修改】优先使用 CMake 注入的绝对路径，回退到默认名称
BIN="${ZEROKV_ROBUSTNESS_BIN:-./zerokv_robustness_tests}"
DIR=/tmp/zerokv_crash

# CI 环境下默认跑 10 轮即可（防止 CI 超时），本地手动跑可传参覆盖（如 ./script.sh 50）
ROUNDS=${1:-10}

for r in $(seq 1 $ROUNDS); do
  rm -rf "$DIR"; mkdir -p "$DIR"
  N=$((2000 + RANDOM % 8000))
  "$BIN" writer "$DIR" "$N" &
  PID=$!
  sleep 0.$((RANDOM % 9))
  kill -9 "$PID" 2>/dev/null
  wait "$PID" 2>/dev/null
  if ! "$BIN" checker "$DIR"; then echo "❌ ROUND $r FAILED"; exit 1; fi
  echo "✅ round $r ok"
done
echo "🎉 ALL $ROUNDS ROUNDS PASSED"