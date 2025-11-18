#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""并行运行六个数据抓取脚本"""

import argparse
import subprocess
import sys
import time
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

DEFAULT_SCRIPTS = [
    ("fetch_kline_em.py", "EM K线"),
    ("fetch_kline_ths.py", "THS K线"),
    ("fetch_kline_qq.py", "QQ K线"),
    ("fetch_kline_bd.py", "BD K线"),
    ("fetch_timesharing_qq.py", "QQ 分时"),
    ("fetch_timesharing_ths.py", "THS 分时"),
]


def run_script(script_path, base_args, workers, extra_env=None):
    cmd = [sys.executable, script_path] + base_args
    env = dict(**extra_env) if extra_env else None
    start = time.time()
    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
            check=False
        )
        duration = time.time() - start
        return {
            "script": script_path,
            "returncode": completed.returncode,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
            "duration": duration,
        }
    except Exception as exc:  # noqa: BLE001
        duration = time.time() - start
        return {
            "script": script_path,
            "returncode": -1,
            "stdout": "",
            "stderr": f"运行失败: {exc}",
            "duration": duration,
        }


def main():
    parser = argparse.ArgumentParser(description="并行运行全部抓取脚本")
    parser.add_argument("--date", type=str, help="单个日期，格式 YYYY-MM-DD")
    parser.add_argument("--start", type=str, help="开始日期，格式 YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="结束日期，格式 YYYY-MM-DD")
    parser.add_argument("--codes", type=str, help="股票代码列表")
    parser.add_argument("--markets", type=str, help="市场代码列表")
    parser.add_argument("--workers", type=int, help="覆盖脚本内部并发设置")
    parser.add_argument(
        "--max-consecutive-failures",
        type=int,
        help="仅对东方财富脚本有效，设置连续失败阈值"
    )
    parser.add_argument(
        "--scripts",
        default=json.dumps([name for name, _ in DEFAULT_SCRIPTS]),
        help="自定义脚本列表(JSON 数组)"
    )

    args = parser.parse_args()
    
    if not args.date and not (args.start and args.end):
        today_str = datetime.now().strftime("%Y-%m-%d")
        args.date = today_str
        print(f"未指定日期参数，默认使用今天: {today_str}")

    try:
        scripts = json.loads(args.scripts)
    except json.JSONDecodeError:
        print("错误: --scripts 参数必须是 JSON 数组")
        sys.exit(1)

    base_args = []
    if args.date:
        base_args += ["--date", args.date]
    if args.start:
        base_args += ["--start", args.start]
    if args.end:
        base_args += ["--end", args.end]
    if args.codes:
        base_args += ["--codes", args.codes]
    if args.markets:
        base_args += ["--markets", args.markets]
    if args.workers:
        base_args += ["--workers", str(max(1, args.workers))]

    if args.max_consecutive_failures:
        base_args_em = base_args + ["--max-consecutive-failures", str(max(1, args.max_consecutive_failures))]
    else:
        base_args_em = base_args.copy()

    script_args_map = {}
    for script in scripts:
        if script == "fetch_kline_em.py":
            script_args_map[script] = base_args_em
        else:
            script_args_map[script] = base_args

    results = []
    start_all = time.time()
    with ThreadPoolExecutor(max_workers=len(scripts)) as executor:
        future_to_script = {
            executor.submit(run_script, script, script_args_map[script], args.workers, None): script
            for script in scripts
        }
        for future in as_completed(future_to_script):
            res = future.result()
            results.append(res)
            status = "成功" if res["returncode"] == 0 else "失败"
            print(f"[{status}] {res['script']} | 用时 {res['duration']:.2f}s")
            if res["stdout"]:
                print(res["stdout"].strip())
            if res["stderr"]:
                print(res["stderr"].strip())
            print("-" * 60)

    total = time.time() - start_all
    success = sum(1 for r in results if r["returncode"] == 0)
    print(f"所有任务完成: 成功 {success}/{len(results)} 个, 总耗时 {total:.2f}s")


if __name__ == "__main__":
    main()
