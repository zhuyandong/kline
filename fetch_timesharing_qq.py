#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
腾讯分时数据抓取脚本（支持历史数据）
使用腾讯分钟K线接口获取历史分时数据
"""

import requests
import json
import csv
import os
from datetime import datetime, timedelta
import argparse
import time
from typing import List, Dict, Optional


class QQTimesharingFetcher:
    """腾讯分时数据抓取类（支持历史数据）"""
    
    def __init__(self):
        self.base_url = "https://ifzq.gtimg.cn/appstock/app/kline/mkline"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "http://quote.eastmoney.com/",
            "Accept": "*/*"
        }
        self.output_dir = "qq_timesharing"
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
        else:
            print("错误: 无法获取股票代码列表")
            return []
    
    def convert_market_code(self, ths_market: str, stock_code: str = None) -> str:
        """将同花顺市场代码转换为腾讯市场代码"""
        market_map = {
            '17': 'sh',
            '33': 'sz'
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
    
    def fetch_timesharing_data(self, stock_code: str, market: str, trade_date: str) -> Optional[List[Dict]]:
        """获取指定日期的分时数据（通过分钟K线接口）"""
        try:
            target_date = datetime.strptime(trade_date, "%Y-%m-%d")
            current_date = datetime.now()
            
            if target_date.date() > current_date.date():
                print(f"错误: 日期 {trade_date} 是未来日期")
                return None
            
            qq_market = self.convert_market_code(market, stock_code)
            qq_code = f"{qq_market}{stock_code}"
            
            # 获取足够多的分钟数据（约5天的数据，240条/天）
            data_count = 1200
            
            param = f"{qq_code},m1,,{data_count}"
            
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
                return None
            
            try:
                data = response.json()
            except json.JSONDecodeError:
                print(f"响应不是有效的JSON: {response.text[:200]}")
                if os.getenv('DEBUG'):
                    print(f"原始响应: {response.text[:500]}")
                return None
            
            if data.get('code') != 0:
                print(f"接口返回错误: code={data.get('code')}, msg={data.get('msg', '未知错误')}")
                return None
            
            if 'data' not in data or qq_code not in data['data']:
                print(f"响应中无数据")
                return None
            
            stock_data = data['data'][qq_code]
            if 'm1' not in stock_data:
                print(f"响应中无m1分钟数据")
                return None
            
            m1_data = stock_data['m1']
            if not m1_data:
                print(f"无分钟数据")
                return None
            
            # 筛选指定日期的数据
            target_date_str = trade_date.replace("-", "")
            timesharing_list = []
            
            for item in m1_data:
                if not item or not isinstance(item, list) or len(item) < 6:
                    continue
                
                try:
                    time_str = str(item[0])
                    if len(time_str) < 8:
                        continue
                    
                    item_date = time_str[:8]
                    if item_date != target_date_str:
                        continue
                    
                    if len(time_str) == 12:
                        date_time_str = f"{time_str[:4]}-{time_str[4:6]}-{time_str[6:8]} {time_str[8:10]}:{time_str[10:12]}:00"
                    else:
                        date_time_str = f"{trade_date} {time_str[:2]}:{time_str[2:4]}:00"
                    
                    open_price = float(item[1]) if len(item) > 1 and item[1] else None
                    high_price = float(item[2]) if len(item) > 2 and item[2] else None
                    low_price = float(item[3]) if len(item) > 3 and item[3] else None
                    close_price = float(item[4]) if len(item) > 4 and item[4] else None
                    volume = float(item[5]) if len(item) > 5 and item[5] else None
                    
                    if close_price is None:
                        continue
                    
                    timesharing_dict = {
                        'date': date_time_str,
                        'price': close_price,
                        'open': open_price,
                        'high': high_price,
                        'low': low_price
                    }
                    
                    if volume is not None:
                        timesharing_dict['volume'] = volume
                    
                    timesharing_list.append(timesharing_dict)
                except (ValueError, TypeError, IndexError) as e:
                    if os.getenv('DEBUG'):
                        print(f"解析分钟数据失败: {item}, 错误: {e}")
                    continue
            
            if timesharing_list:
                return timesharing_list
            
            print(f"无指定日期({trade_date})的分时数据")
            return None
            
        except Exception as e:
            print(f"获取分时数据失败 ({stock_code}, {trade_date}): {e}")
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
            'date', 'code', 'price', 'open', 'high', 'low',
            'volume', 'pre_close', 'change', 'change_percent', 'market'
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
    
    def fetch_by_date(self, stock_codes: List[Dict], date: str):
        """按单个日期抓取数据"""
        self.fetch_by_date_range(stock_codes, date, date)
    
    def fetch_by_date_range(self, stock_codes: List[Dict], start_date: str, end_date: str):
        """按日期区间抓取数据"""
        trade_days = self.get_trade_days(start_date, end_date)
        
        print(f"开始抓取分时数据: {start_date} 至 {end_date}")
        print(f"交易日数量: {len(trade_days)}")
        print(f"股票数量: {len(stock_codes)}")
        
        for trade_date in trade_days:
            print(f"\n处理日期: {trade_date}")
            
            all_data = []
            for stock in stock_codes:
                code = stock['code']
                market = stock['market']
                name = stock.get('name', '')
                
                print(f"  抓取 {name}({code})...", end=' ')
                timesharing_data = self.fetch_timesharing_data(code, market, trade_date)
                
                if timesharing_data:
                    for record in timesharing_data:
                        record['code'] = code
                        record['market'] = market
                    all_data.extend(timesharing_data)
                    print(f"成功 ({len(timesharing_data)} 条)")
                else:
                    print("失败")
                
                time.sleep(0.5)
            
            if all_data:
                filename = f"timesharing_{trade_date}.csv"
                self.save_to_csv(all_data, filename)
            else:
                print(f"  日期 {trade_date} 无数据")


def main():
    TEST_LIMIT = None
    
    parser = argparse.ArgumentParser(description='腾讯分时数据抓取工具（支持历史数据）')
    parser.add_argument('--date', type=str, help='单个日期，格式: YYYY-MM-DD')
    parser.add_argument('--start', type=str, help='开始日期，格式: YYYY-MM-DD')
    parser.add_argument('--end', type=str, help='结束日期，格式: YYYY-MM-DD')
    parser.add_argument('--codes', type=str, help='股票代码列表，逗号分隔，格式: 代码1,代码2 (需要配合--markets使用)')
    parser.add_argument('--markets', type=str, help='市场代码列表，逗号分隔，格式: 市场1,市场2 (需要配合--codes使用)')
    
    args = parser.parse_args()
    
    if args.date:
        if args.start or args.end:
            print("错误: --date 和 --start/--end 不能同时使用")
            return
    elif not (args.start and args.end):
        print("错误: 请指定 --date 或 --start/--end")
        return
    
    fetcher = QQTimesharingFetcher()
    
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
    
    if args.date:
        fetcher.fetch_by_date(stock_codes, args.date)
    else:
        fetcher.fetch_by_date_range(stock_codes, args.start, args.end)
    
    print("\n抓取完成！")


if __name__ == "__main__":
    main()



