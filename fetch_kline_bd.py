#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
百度股市通K线数据抓取脚本
支持抓取指定日期或日期段内的每日K线数据，保存为CSV文件
"""

import argparse
import csv
import os
import random
import time
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional


class BDKlineFetcher:
    """百度股市通K线数据抓取类"""

    def __init__(self, max_workers: int = 10, history_limit: int = 640):
        self.base_url = "https://finance.pae.baidu.com/selfselect/getstockquotation"
        self.output_dir = os.path.join("data", "bd")
        self.history_limit = history_limit
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/119.0.0.0 Safari/537.36"
                ),
                "Referer": "https://gushitong.baidu.com/",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "zh-CN,zh;q=0.9",
            }
        )
        self.ths_fetcher = None

    def _get_ths_fetcher(self):
        """延迟导入同花顺fetcher以复用股票代码获取方法"""
        if self.ths_fetcher is None:
            try:
                from fetch_kline_ths import THSKlineFetcher

                self.ths_fetcher = THSKlineFetcher()
            except ImportError:
                print("警告: 无法导入同花顺模块，将使用默认方法获取股票代码")
        return self.ths_fetcher

    def get_all_stock_codes(self) -> List[Dict[str, str]]:
        """获取全部A股股票代码列表（复用同花顺接口）"""
        ths_fetcher = self._get_ths_fetcher()
        if ths_fetcher:
            return ths_fetcher.get_all_stock_codes()
        print("错误: 无法获取股票代码列表")
        return []

    def convert_market_code(self, ths_market: str, stock_code: Optional[str] = None) -> str:
        """将同花顺市场代码转换为百度市场代码"""
        market_map = {
            "17": "ab",  # 上海A股
            "33": "ab",  # 深圳A股
            "1": "index",  # 上证指数
            "16": "index",
            "32": "index",
        }
        market = market_map.get(ths_market)

        if market is None and stock_code:
            if stock_code.startswith(("6", "5")):
                market = "ab"
            elif stock_code.startswith(("0", "3")):
                market = "ab"
            elif stock_code.startswith("9"):
                market = "global"
            else:
                market = "ab"
        return market or "ab"

    def get_trade_days(self, start_date: str, end_date: str) -> List[str]:
        """获取交易日列表（简化版：跳过周末）"""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        days = []
        current = start
        while current <= end:
            if current.weekday() < 5:
                days.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
        return days

    def _calculate_start_time(self, trade_date: str) -> str:
        date_obj = datetime.strptime(trade_date, "%Y-%m-%d")
        lookback_days = max(self.history_limit * 2, 400)
        start_date = date_obj - timedelta(days=lookback_days)
        return start_date.strftime("%Y-%m-%d")

    def _build_params(self, stock_code: str, market: str, trade_date: str) -> Dict[str, str]:
        params = {
            "all": "1",
            "code": stock_code,
            "isIndex": "0",
            "isBk": "0",
            "isBlock": "0",
            "isFutures": "0",
            "isStock": "1",
            "newFormat": "1",
            "market_type": market,
            "group": f"quotation_kline_{market}",
            "ktype": "1",
            "start_time": self._calculate_start_time(trade_date),
            "finClientType": "pc",
            "pointType": "string",
            "srcid": "5353",
        }
        return params

    def fetch_kline_data(self, stock_code: str, market: str, trade_date: str) -> Optional[List[Dict]]:
        """获取指定日期的K线数据"""
        params = self._build_params(stock_code, market, trade_date)

        try:
            response = self.session.get(self.base_url, params=params, timeout=30)
        except requests.RequestException as exc:
            print(f"请求失败: {stock_code} - {trade_date} - {exc}")
            return None

        if response.status_code != 200:
            print(f"请求失败: HTTP {response.status_code}")
            return None

        try:
            data = response.json()
        except ValueError:
            print(f"响应不是有效的JSON: {response.text[:200]}")
            return None

        if str(data.get("ResultCode")) != "0":
            print(f"接口返回错误: {data.get('ResultCode')} - {data.get('Result')}")
            return None

        market_data = data.get("Result", {}).get("newMarketData", {})
        raw_data = market_data.get("marketData")
        keys = market_data.get("keys", [])
        if not raw_data or not keys:
            print(f"无K线数据: {stock_code} - {trade_date}")
            return None

        records = self._parse_market_data(raw_data, keys)
        if not records:
            return None

        filtered = self._limit_history(records, trade_date)
        return filtered or None

    @staticmethod
    def _parse_market_data(raw_data: str, keys: List[str]) -> List[Dict]:
        def convert_value(key: str, value: str):
            if value in ("", "--", None):
                return None
            if key == "time":
                return value
            if key == "timestamp":
                try:
                    return int(value)
                except ValueError:
                    return None
            numeric_fields = {
                "open",
                "close",
                "high",
                "low",
                "volume",
                "amount",
                "range",
                "ratio",
                "turnoverratio",
                "preClose",
                "ma5avgprice",
                "ma5volume",
                "ma10avgprice",
                "ma10volume",
                "ma20avgprice",
                "ma20volume",
            }
            if key in numeric_fields:
                try:
                    return float(value)
                except ValueError:
                    return None
            return value

        records = []
        for row in raw_data.split(";"):
            row = row.strip()
            if not row:
                continue
            parts = row.split(",")
            if len(parts) != len(keys):
                continue
            item = {}
            for key, value in zip(keys, parts):
                item[key] = convert_value(key, value)
            records.append(item)
        return records

    def _limit_history(self, records: List[Dict], trade_date: str) -> List[Dict]:
        """筛选出截止到 trade_date 的最近 history_limit 条记录"""
        valid_records = [
            item for item in records
            if item.get("time")
        ]
        valid_records.sort(key=lambda x: x["time"])
        target_records = [
            item for item in valid_records
            if item["time"] <= trade_date
        ]
        if not target_records:
            return []
        return target_records[-self.history_limit:]

    def _fetch_single_stock(self, stock: Dict, trade_date: str) -> Optional[List[Dict]]:
        code = stock["code"]
        market = self.convert_market_code(stock["market"], code)
        records = self.fetch_kline_data(code, market, trade_date)
        if not records:
            return None

        for item in records:
            item["code"] = code
            item["market"] = market
        return records

    def fetch_by_date_range(self, stock_codes: List[Dict], start_date: str, end_date: str):
        trade_days = self.get_trade_days(start_date, end_date)

        print(f"开始抓取数据: {start_date} 至 {end_date}")
        print(f"交易日数量: {len(trade_days)}")
        print(f"股票数量: {len(stock_codes)}")

        for trade_date in trade_days:
            print(f"\n处理日期: {trade_date}")
            all_data = []
            success = 0
            failures = 0

            for stock in stock_codes:
                code = stock["code"]
                name = stock.get("name", "")
                try:
                    result = self._fetch_single_stock(stock, trade_date)
                except Exception as exc:  # noqa: BLE001
                    failures += 1
                    print(f"  {name}({code}) 异常: {exc}")
                    time.sleep(random.uniform(2.0, 4.0))
                    continue

                if result:
                    all_data.extend(result)
                    success += 1
                    print(f"  {name}({code}) 成功 ({len(result)} 条)")
                else:
                    failures += 1
                    print(f"  {name}({code}) 无数据")

                time.sleep(random.uniform(2.0, 4.0))

            print(f"  抓取结果: 成功 {success} 只，失败 {failures} 只")

            if all_data:
                filename = f"kline_{trade_date}.csv"
                self.save_to_csv(all_data, filename)
            else:
                print(f"  日期 {trade_date} 无数据")

    def fetch_by_single_date(self, stock_codes: List[Dict], date: str):
        """按单个日期抓取数据"""
        self.fetch_by_date_range(stock_codes, date, date)

    def save_to_csv(self, data: List[Dict], filename: str):
        """保存数据到CSV文件"""
        os.makedirs(self.output_dir, exist_ok=True)
        filepath = os.path.join(self.output_dir, filename)

        preferred_order = [
            "time",
            "code",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "range",
            "ratio",
            "turnoverratio",
            "ma5avgprice",
            "ma10avgprice",
            "ma20avgprice",
            "market",
            "timestamp",
        ]

        keys = set()
        for item in data:
            keys.update(item.keys())

        fieldnames = []
        for field in preferred_order:
            if field in keys:
                fieldnames.append(field)
        for field in sorted(keys):
            if field not in fieldnames:
                fieldnames.append(field)

        with open(filepath, "w", newline="", encoding="utf-8-sig") as fp:
            writer = csv.DictWriter(fp, fieldnames=fieldnames)
            writer.writeheader()
            if data:
                writer.writerows(data)
            else:
                writer.writerow({"time": "获取到0条数据"})

        print(f"已保存: {filepath} ({len(data)} 条记录)")


def _resolve_date_args(args):
    """统一处理日期参数，支持默认日期"""
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    if args.date:
        if args.start or args.end:
            raise ValueError("--date 和 --start/--end 不能同时使用")
        return "single", args.date, None, None
    
    if args.start or args.end:
        if not (args.start and args.end):
            raise ValueError("--start 和 --end 需要同时指定，或直接使用 --date")
        return "range", None, args.start, args.end
    
    print(f"未指定日期，默认使用今天: {today_str}")
    return "single", today_str, None, None


def main():
    parser = argparse.ArgumentParser(description="百度股市通K线数据抓取工具")
    parser.add_argument("--date", type=str, help="单个日期，格式: YYYY-MM-DD（缺省为今天）")
    parser.add_argument("--start", type=str, help="开始日期，格式: YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="结束日期，格式: YYYY-MM-DD")
    parser.add_argument("--codes", type=str, help="股票代码列表，逗号分隔，格式: 代码1,代码2 (需要配合--markets使用)")
    parser.add_argument("--markets", type=str, help="市场代码列表，逗号分隔，格式: 市场1,市场2 (需要配合--codes使用)")
    parser.add_argument("--workers", type=int, default=10, help="并发线程数量，默认10")

    args = parser.parse_args()

    try:
        mode, resolved_date, start_date, end_date = _resolve_date_args(args)
    except ValueError as exc:
        print(f"错误: {exc}")
        return

    fetcher = BDKlineFetcher(max_workers=max(1, args.workers or 10))

    if args.codes and args.markets:
        codes = [c.strip() for c in args.codes.split(",")]
        markets = [m.strip() for m in args.markets.split(",")]
        if len(codes) != len(markets):
            print("错误: 股票代码数量和市场代码数量不匹配")
            return
        stock_codes = [
            {"code": code, "market": market, "name": ""}
            for code, market in zip(codes, markets)
        ]
        print(f"使用指定的 {len(stock_codes)} 只股票")
    else:
        print("获取全部A股股票代码...")
        stock_codes = fetcher.get_all_stock_codes()
        if not stock_codes:
            print("获取股票代码失败")
            return

    if mode == "single":
        fetcher.fetch_by_single_date(stock_codes, resolved_date)
    else:
        fetcher.fetch_by_date_range(stock_codes, start_date, end_date)

    print("\n抓取完成！")


if __name__ == "__main__":
    main()

