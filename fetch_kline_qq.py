#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
腾讯K线数据抓取脚本
支持抓取指定日期或日期段内的每日K线数据，保存为CSV文件
"""

import requests
import json
import csv
import os
import re
import random
from datetime import datetime, timedelta
import argparse
import time
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed


class QQKlineFetcher:
    """腾讯K线数据抓取类"""
    
    def __init__(self, max_workers: int = 10):
        self.base_url = "http://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "http://quote.eastmoney.com/",
            "Accept": "*/*"
        }
        self.output_dir = os.path.join("data", "qq")
        self.ths_fetcher = None
        self.max_workers = max_workers
        
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
        else:
            print("错误: 无法获取股票代码列表")
            return []
    
    def convert_market_code(self, ths_market: str, stock_code: str = None) -> str:
        """将同花顺市场代码转换为腾讯市场代码"""
        # 17=上海A股 -> sh, 33=深圳A股 -> sz
        market_map = {
            '17': 'sh',  # 上海
            '33': 'sz'   # 深圳
        }
        
        qq_market = market_map.get(ths_market)
        
        if qq_market is None and stock_code:
            if stock_code.startswith('6'):
                qq_market = 'sh'
            elif stock_code.startswith(('0', '3')):
                qq_market = 'sz'
            else:
                qq_market = 'sh'
        
        return qq_market or 'sh'
    
    def get_trade_days(self, start_date: str, end_date: str) -> List[str]:
        """获取交易日列表（简化版：跳过周末）"""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        dates = []
        current = start
        while current <= end:
            if current.weekday() < 5:
                dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
        return dates
    
    def fetch_kline_data(self, stock_code: str, market: str, trade_date: str) -> Optional[List[Dict]]:
        """获取指定日期的K线数据"""
        try:
            target_date = datetime.strptime(trade_date, "%Y-%m-%d")
            current_date = datetime.now()
            if target_date > current_date:
                print(f"错误: 日期 {trade_date} 是未来日期")
                return None
            
            qq_market = self.convert_market_code(market, stock_code)
            qq_code = f"{qq_market}{stock_code}"
            
            days_to_fetch = 640
            
            param = f"{qq_code},day,,,{days_to_fetch},qfq"
            
            params = {
                "param": param
            }
            
            if os.getenv('DEBUG'):
                print(f"  请求参数: {params}")
            
            response = requests.get(
                self.base_url,
                headers=self.headers,
                params=params,
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"请求失败: HTTP {response.status_code}")
                try:
                    error_text = response.text[:500]
                    print(f"错误详情: {error_text}")
                except:
                    pass
                return None
            
            try:
                text = response.text
                
                if os.getenv('DEBUG'):
                    print(f"  响应数据: {text[:500]}")
                
                match = re.search(r'\{.*\}', text, re.DOTALL)
                if not match:
                    print(f"响应格式错误: {text[:200]}")
                    return None
                
                data = json.loads(match.group(0))
            except (json.JSONDecodeError, AttributeError) as e:
                print(f"响应不是有效的JSON: {response.text[:200]}")
                if os.getenv('DEBUG'):
                    print(f"原始响应: {response.text[:500]}")
                return None
            
            if 'data' not in data:
                print(f"响应中无data字段: {json.dumps(data, ensure_ascii=False)[:300]}")
                return None
            
            data_obj = data['data']
            if not data_obj:
                print(f"data字段为空")
                return None
            
            if qq_code not in data_obj:
                print(f"响应中无股票代码 {qq_code}")
                return None
            
            stock_data = data_obj[qq_code]
            if 'qfqday' not in stock_data or not stock_data['qfqday']:
                print(f"无K线数据")
                return None
            
            klines = stock_data['qfqday']
            if not klines:
                print(f"无K线数据")
                return None
            
            kline_list = []
            for kline_row in klines:
                if not kline_row:
                    continue
                
                if not isinstance(kline_row, list) or len(kline_row) < 6:
                    continue
                
                try:
                    kline_date = str(kline_row[0]) if kline_row[0] else None
                    if not kline_date:
                        continue
                    
                    open_price = float(kline_row[1]) if kline_row[1] and isinstance(kline_row[1], (str, int, float)) else None
                    close_price = float(kline_row[2]) if kline_row[2] and isinstance(kline_row[2], (str, int, float)) else None
                    high_price = float(kline_row[3]) if kline_row[3] and isinstance(kline_row[3], (str, int, float)) else None
                    low_price = float(kline_row[4]) if kline_row[4] and isinstance(kline_row[4], (str, int, float)) else None
                    volume = float(kline_row[5]) if kline_row[5] and isinstance(kline_row[5], (str, int, float)) else None
                    
                    kline_dict = {
                        'date': kline_date,
                        'open': open_price,
                        'close': close_price,
                        'high': high_price,
                        'low': low_price,
                        'volume': volume
                    }
                    
                    if len(kline_row) >= 7 and kline_row[6] and isinstance(kline_row[6], (str, int, float)):
                        try:
                            amount = float(kline_row[6])
                            kline_dict['amount'] = amount
                        except (ValueError, TypeError):
                            pass
                    
                    kline_list.append(kline_dict)
                except (ValueError, TypeError, IndexError) as e:
                    if os.getenv('DEBUG'):
                        print(f"解析K线数据失败: {kline_row}, 错误: {e}")
                    continue
            
            if kline_list:
                return kline_list
            
            print(f"无有效K线数据")
            return None
            
        except Exception as e:
            print(f"获取K线数据失败 ({stock_code}, {trade_date}): {e}")
            import traceback
            if os.getenv('DEBUG'):
                traceback.print_exc()
            return None
    
    def save_to_csv(self, data: List[Dict], filename: str):
        """保存数据到CSV文件"""
        if not data:
            print(f"无数据可保存: {filename}")
            return
        
        os.makedirs(self.output_dir, exist_ok=True)
        filepath = os.path.join(self.output_dir, filename)
        
        preferred_order = [
            'date', 'code', 'open', 'high', 'low', 'close',
            'volume', 'amount', 'zf', 'zdf', 'zde',
            'turnover', 'outstanding_share', 'market', 'timestamp'
        ]
        
        all_keys = set()
        for item in data:
            all_keys.update(item.keys())
        
        fieldnames = []
        for field in preferred_order:
            if field in all_keys:
                fieldnames.append(field)
        for field in sorted(all_keys):
            if field not in fieldnames:
                fieldnames.append(field)
        
        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        
        print(f"已保存: {filepath} ({len(data)} 条记录)")
    
    def _fetch_single_stock(self, stock: Dict, trade_date: str) -> Optional[List[Dict]]:
        """线程池任务：抓取单支股票对应日期的数据"""
        code = stock['code']
        market = stock['market']
        kline_data = self.fetch_kline_data(code, market, trade_date)
        if kline_data:
            for record in kline_data:
                record['code'] = code
                record['market'] = market
        time.sleep(random.uniform(0.05, 0.15))
        return kline_data
    
    def fetch_by_date_range(self, stock_codes: List[Dict], start_date: str, end_date: str):
        """按日期段抓取数据"""
        trade_days = self.get_trade_days(start_date, end_date)
        
        print(f"开始抓取数据: {start_date} 至 {end_date}")
        print(f"交易日数量: {len(trade_days)}")
        print(f"股票数量: {len(stock_codes)}")
        
        for trade_date in trade_days:
            print(f"\n处理日期: {trade_date}")
            
            all_data = []
            success = 0
            failures = 0
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_stock = {
                    executor.submit(self._fetch_single_stock, stock, trade_date): stock
                    for stock in stock_codes
                }
                
                for future in as_completed(future_to_stock):
                    stock = future_to_stock[future]
                    code = stock['code']
                    name = stock.get('name', '')
                    try:
                        records = future.result()
                        if records:
                            all_data.extend(records)
                            success += 1
                            print(f"  {name}({code}) 成功 ({len(records)} 条)")
                        else:
                            failures += 1
                            print(f"  {name}({code}) 无数据")
                    except Exception as exc:
                        failures += 1
                        print(f"  {name}({code}) 失败: {exc}")
            
            print(f"  并发结果: 成功 {success} 只，失败 {failures} 只")
            if all_data:
                filename = f"kline_{trade_date}.csv"
                self.save_to_csv(all_data, filename)
            else:
                print(f"  日期 {trade_date} 无数据")
    
    def fetch_by_single_date(self, stock_codes: List[Dict], date: str):
        """按单个日期抓取数据"""
        self.fetch_by_date_range(stock_codes, date, date)


def _resolve_date_args(args):
    """统一处理日期参数，支持默认取当天"""
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
    TEST_LIMIT = None
    
    parser = argparse.ArgumentParser(description='腾讯K线数据抓取工具')
    parser.add_argument('--date', type=str, help='单个日期，格式: YYYY-MM-DD（缺省为今天）')
    parser.add_argument('--start', type=str, help='开始日期，格式: YYYY-MM-DD')
    parser.add_argument('--end', type=str, help='结束日期，格式: YYYY-MM-DD')
    parser.add_argument('--codes', type=str, help='股票代码列表，逗号分隔，格式: 代码1,代码2 (需要配合--markets使用)')
    parser.add_argument('--markets', type=str, help='市场代码列表，逗号分隔，格式: 市场1,市场2 (需要配合--codes使用)')
    parser.add_argument('--workers', type=int, default=10, help='并发线程数量，默认10')
    
    args = parser.parse_args()
    
    try:
        mode, resolved_date, start_date, end_date = _resolve_date_args(args)
    except ValueError as exc:
        print(f"错误: {exc}")
        return
    
    fetcher = QQKlineFetcher(max_workers=max(1, args.workers or 10))
    
    if args.codes and args.markets:
        codes = [c.strip() for c in args.codes.split(',')]
        markets = [m.strip() for m in args.markets.split(',')]
        if len(codes) != len(markets):
            print("错误: 股票代码数量和市场代码数量不匹配")
            return
        stock_codes = [
            {'code': code, 'market': market, 'name': ''}
            for code, market in zip(codes, markets)
        ]
        print(f"使用指定的 {len(stock_codes)} 只股票")
    else:
        print("获取全部A股股票代码...")
        stock_codes = fetcher.get_all_stock_codes()
        if not stock_codes:
            print("获取股票代码失败")
            return
    
    if TEST_LIMIT and TEST_LIMIT > 0:
        original_count = len(stock_codes)
        stock_codes = stock_codes[:TEST_LIMIT]
        print(f"⚠️  测试模式：限制抓取数量为 {TEST_LIMIT} 只（原始数量: {original_count} 只）")
    
    if mode == "single":
        fetcher.fetch_by_single_date(stock_codes, resolved_date)
    else:
        fetcher.fetch_by_date_range(stock_codes, start_date, end_date)
    
    print("\n抓取完成！")


if __name__ == "__main__":
    main()

