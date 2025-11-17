#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
同花顺K线数据抓取脚本
支持抓取指定日期或日期段内的每日K线数据，保存为CSV文件
"""

import requests
import json
import csv
import os
from datetime import datetime, timedelta
import argparse
import time
from typing import List, Dict, Optional


class THSKlineFetcher:
    """同花顺K线数据抓取类"""
    
    def __init__(self):
        self.base_url = "https://quota-h.10jqka.com.cn/fuyao/common_hq_aggr_cache/quote/v1/single_kline"
        self.headers = {
            "Host": "quota-h.10jqka.com.cn",
            "Content-Type": "application/json",
            "x-fuyao-auth": "basecomponent",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        self.output_dir = "ths"
        
    def get_all_stock_codes(self) -> List[Dict[str, str]]:
        """获取全部A股股票代码列表（排除指数）"""
        try:
            url = "https://ozone.10jqka.com.cn/tg_templates/doubleone/datacenter/data/all_codes.txt"
            response = requests.get(url, timeout=10)
            response.encoding = 'utf-8'
            data = json.loads(response.text)
            
            stock_list = []
            a_stock_markets = ['17', '33']  # 17=上海A股，33=深圳A股
            
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
    
    def date_to_timestamp(self, date_str: str) -> int:
        """将日期字符串转换为时间戳（秒）"""
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            return int(dt.timestamp())
        except ValueError:
            raise ValueError(f"日期格式错误，应为 YYYY-MM-DD: {date_str}")
    
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
            
            target_timestamp = self.date_to_timestamp(trade_date)
            current_timestamp = int(time.time())
            days_diff = (current_timestamp - target_timestamp) // 86400
            
            if days_diff < 0:
                print(f"错误: 日期 {trade_date} 是未来日期")
                return None
            
            if days_diff > 365:
                print(f"警告: 日期 {trade_date} 距离今天超过1年，可能无法获取数据")
            
            days_to_fetch = 100
            
            payload = {
                "code_list": [
                    {
                        "market": market,
                        "codes": [stock_code]
                    }
                ],
                "trade_class": "intraday",
                "time_period": "day_1",
                "trade_date": -1,
                "begin_time": -days_to_fetch,
                "end_time": 0,
                "adjust_type": "forward",
                "gpid": 0
            }
            
            if os.getenv('DEBUG'):
                print(f"  请求参数: {json.dumps(payload, ensure_ascii=False, indent=2)}")
            
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json=payload,
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"请求失败: HTTP {response.status_code}")
                try:
                    error_text = response.text[:500]
                    print(f"错误详情: {error_text}")
                except:
                    pass
                print(f"请求参数: {json.dumps(payload, ensure_ascii=False)}")
                return None
            
            try:
                data = response.json()
            except json.JSONDecodeError:
                print(f"响应不是有效的JSON: {response.text[:200]}")
                return None
            
            if os.getenv('DEBUG'):
                print(f"  响应数据: {json.dumps(data, ensure_ascii=False, indent=2)[:500]}")
            
            if 'error' in data or 'message' in data:
                print(f"接口返回错误: {data.get('error', data.get('message', '未知错误'))}")
                return None
            
            if 'data' in data:
                quote_data = data['data']
                if isinstance(quote_data, dict) and 'quote_data' in quote_data:
                    quote_list = quote_data['quote_data']
                elif isinstance(quote_data, list):
                    quote_list = quote_data
                else:
                    quote_list = []
                
                kline_list = []
                for item in quote_list:
                    if 'value' in item and item['value']:
                        data_fields = item.get('data_fields', [])
                        for kline_row in item['value']:
                            if not kline_row or len(kline_row) < 2:
                                continue
                            
                            timestamp_ms = kline_row[0]
                            try:
                                kline_date = datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d')
                            except (ValueError, OSError):
                                continue
                            
                            kline_dict = {
                                'timestamp': timestamp_ms,
                                'date': kline_date
                            }
                            
                            field_mapping = {
                                '1': 'open',
                                '7': 'high', 
                                '8': 'low',
                                '9': 'close',
                                '11': 'volume',
                                '13': 'amount'
                            }
                            
                            for idx, field_id in enumerate(data_fields):
                                if idx + 1 < len(kline_row):
                                    field_name = field_mapping.get(field_id, f'field_{field_id}')
                                    kline_dict[field_name] = kline_row[idx + 1]
                            
                            kline_list.append(kline_dict)
                    
                    elif 'kline' in item and item['kline']:
                        for kline in item['kline']:
                            kline_list.append(kline)
                
                if kline_list:
                    return kline_list
            
            print(f"无数据返回，响应内容: {json.dumps(data, ensure_ascii=False)[:300]}")
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
            'volume', 'amount', 'turnover', 'outstanding_share',
            'market', 'timestamp'
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
    
    def fetch_by_date_range(self, stock_codes: List[Dict], start_date: str, end_date: str):
        """按日期段抓取数据"""
        trade_days = self.get_trade_days(start_date, end_date)
        
        print(f"开始抓取数据: {start_date} 至 {end_date}")
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
                kline_data = self.fetch_kline_data(code, market, trade_date)
                
                if kline_data:
                    for record in kline_data:
                        record['code'] = code
                        record['market'] = market
                    all_data.extend(kline_data)
                    print(f"成功 ({len(kline_data)} 条)")
                else:
                    print("失败")
                
                time.sleep(0.5)
            
            if all_data:
                filename = f"kline_{trade_date}.csv"
                self.save_to_csv(all_data, filename)
            else:
                print(f"  日期 {trade_date} 无数据")
    
    def fetch_by_single_date(self, stock_codes: List[Dict], date: str):
        """按单个日期抓取数据"""
        self.fetch_by_date_range(stock_codes, date, date)


def main():
    TEST_LIMIT = None  # 测试时限制抓取数量，正式运行时改为None或0
    
    parser = argparse.ArgumentParser(description='同花顺K线数据抓取工具')
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
    
    fetcher = THSKlineFetcher()
    
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
        fetcher.fetch_by_single_date(stock_codes, args.date)
    else:
        fetcher.fetch_by_date_range(stock_codes, args.start, args.end)
    
    print("\n抓取完成！")


if __name__ == "__main__":
    main()

