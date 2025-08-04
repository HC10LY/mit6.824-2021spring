#!/bin/bash

# 测试项列表
tests=("2A" "2B" "2C")
tests2c=("2C")

# 循环次数
repeat=30

# 统计结果字典（bash里用关联数组）
declare -A success_count
declare -A fail_count

# 初始化计数
for test in "${tests[@]}"; do
  success_count[$test]=0
  fail_count[$test]=0
done

# 创建日志目录
mkdir -p logs

for test in "${tests2c[@]}"; do
  echo "=== Running test $test for $repeat times ==="
  for ((i=1; i<=repeat; i++)); do
    echo "Run #$i for test $test"
    log_file="logs/test_${test}_run${i}.log"  # 每次运行都记录日志
    go test -run "$test" -race > "$log_file" 2>&1
    if [ $? -eq 0 ]; then
      ((success_count[$test]++))
      echo "  Success"
      rm "$log_file"  # 如果成功可以删掉日志（可选）
    else
      ((fail_count[$test]++))
      echo "  Fail (see $log_file)"
    fi
  done
  echo ""
done

# 打印统计结果
echo "=== Summary ==="
for test in "${tests[@]}"; do
  echo "Test $test: Success=${success_count[$test]}, Fail=${fail_count[$test]}"
done