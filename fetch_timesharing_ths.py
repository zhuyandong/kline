#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
同花顺分时数据抓取脚本
支持抓取指定日期的分时数据，保存为CSV文件
"""

import requests
import json
import csv
import os
from datetime import datetime, timedelta
import argparse
import time
from typing import List, Dict, Optional


class THSTimesharingFetcher:
    """同花顺分时数据抓取类"""
    
    def __init__(self):
        self.base_url = "https://dq.10jqka.com.cn/interval_calculation/block_info/v1/get_time_sharing"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://www.10jqka.com.cn/",
            "Accept": "*/*"
        }
        self.output_dir = os.path.join("data", "ths_timesharing")
        
    def get_all_stock_codes(self) -> List[Dict[str, str]]:
        """获取全部A股股票代码列表（排除指数）"""
        try:
            url = "https://ozone.10jqka.com.cn/tg_templates/doubleone/datacenter/data/all_codes.txt"
            response = requests.get(url, timeout=10)
            response.encoding = 'utf-8'
            data = json.loads(response.text)
            
            stock_list = []
            a_stock_markets = ['17', '33']
            
            for market_code, stocks in data.items():
                if market_code in a_stock_markets:
                    for stock in stocks:
                        if len(stock) >= 2:
                            stock_list.append({
                                'code': stock[0],
                                'market': market_code,
                                'name': stock[1] if len(stock) > 1 else ''
                            })
            
            print(f"获取到 {len(stock_list)} 只A股股票")
            return stock_list
        except Exception as e:
            print(f"获取股票代码列表失败: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def fetch_timesharing_data(self, stock_code: str, market: str, trade_date: str) -> Optional[List[Dict]]:
        """获取指定日期的分时数据"""
        try:
            target_date = datetime.strptime(trade_date, "%Y-%m-%d")
            current_date = datetime.now()
            
            if target_date.date() > current_date.date():
                print(f"错误: 日期 {trade_date} 是未来日期，分时数据只能获取当日或历史数据")
                return None
            
            params = {
                'code': stock_code,
                'market': market,
                'type': '1'
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
            
            if data.get('status_code') != 0:
                print(f"接口返回错误: status_code={data.get('status_code')}, status_msg={data.get('status_msg', '未知错误')}")
                return None
            
            if 'data' not in data or not data['data']:
                print(f"响应中无data字段或data为空")
                return None
            
            data_info = data['data']
            base_price = float(data_info.get('base_price', 0)) if data_info.get('base_price') else None
            data_list = data_info.get('data_list', [])
            
            if not data_list:
                print(f"无分时数据（可能是非交易日）")
                return None
            
            # 检查返回数据的日期是否与请求日期匹配
            if data_list:
                first_item = data_list[0]
                first_date_str = first_item.get('date', '')
                if first_date_str and len(first_date_str) >= 8:
                    returned_date = f"{first_date_str[:4]}-{first_date_str[4:6]}-{first_date_str[6:8]}"
                    if returned_date != trade_date:
                        print(f"返回数据日期({returned_date})与请求日期({trade_date})不匹配，可能是非交易日")
                        return None
            
            # 检查数据量是否合理（正常交易日应该有较多分时数据，比如100条以上）
            if len(data_list) < 10:
                print(f"数据量过少({len(data_list)}条)，可能是非交易日")
                return None
            
            timesharing_list = []
            for item in data_list:
                if not item or not isinstance(item, dict):
                    continue
                
                try:
                    date_str = item.get('date', '')
                    value_str = item.get('value', '')
                    
                    if not date_str or not value_str:
                        continue
                    
                    # 再次检查日期是否匹配
                    if len(date_str) >= 8:
                        item_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                        if item_date != trade_date:
                            continue
                    
                    if len(date_str) == 14:
                        date_time_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]} {date_str[8:10]}:{date_str[10:12]}:{date_str[12:14]}"
                    elif len(date_str) == 8:
                        date_time_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]} 00:00:00"
                    else:
                        date_time_str = f"{trade_date} 00:00:00"
                    
                    price = float(value_str) if value_str else None
                    
                    if price is None:
                        continue
                    
                    change = None
                    change_percent = None
                    if base_price and base_price > 0:
                        change = price - base_price
                        change_percent = (change / base_price) * 100
                    
                    timesharing_dict = {
                        'date': date_time_str,
                        'price': price,
                        'pre_close': base_price
                    }
                    
                    if change is not None:
                        timesharing_dict['change'] = change
                    if change_percent is not None:
                        timesharing_dict['change_percent'] = change_percent
                    
                    timesharing_list.append(timesharing_dict)
                except (ValueError, TypeError, KeyError) as e:
                    if os.getenv('DEBUG'):
                        print(f"解析分时数据失败: {item}, 错误: {e}")
                    continue
            
            if timesharing_list:
                return timesharing_list
            
            print(f"无有效分时数据")
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
            'date', 'code', 'price', 'pre_close',
            'change', 'change_percent', 'market'
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
    
    parser = argparse.ArgumentParser(description='同花顺分时数据抓取工具')
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
    
    fetcher = THSTimesharingFetcher()
    
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

