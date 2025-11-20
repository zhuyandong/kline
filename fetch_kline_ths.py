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
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


class THSKlineFetcher:
    """同花顺K线数据抓取类"""
    
    def __init__(self, workers: int = 10):
        self.base_url = "https://quota-h.10jqka.com.cn/fuyao/common_hq_aggr_cache/quote/v1/single_kline"
        self.headers = {
            "Host": "quota-h.10jqka.com.cn",
            "Content-Type": "application/json",
            "x-fuyao-auth": "basecomponent",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        self.output_dir = os.path.join("data", "ths")
        self.workers = workers
        self.print_lock = threading.Lock()  # 线程安全的打印锁
        
    def get_all_stock_codes(self) -> List[Dict[str, str]]:
        """获取全部股票代码列表（包括指数和特殊分类）"""
        url = "https://ozone.10jqka.com.cn/tg_templates/doubleone/datacenter/data/all_codes.txt"
        headers = {
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/118.0.0.0 Safari/537.36"),
            "Referer": "https://www.10jqka.com.cn/",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive"
        }
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            response.encoding = 'utf-8'
            data = json.loads(response.text)
        except Exception as e:
            print(f"获取股票代码列表失败: {e}")
            import traceback
            traceback.print_exc()
            return []
        
        stock_list = []
        for market_code, stocks in data.items():
            for stock in stocks:
                if len(stock) >= 2:
                    stock_list.append({
                        'code': stock[0],
                        'market': market_code,
                        'name': stock[1] if len(stock) > 1 else ''
                    })
        
        print(f"获取到 {len(stock_list)} 只股票（包括指数和特殊分类）")
        return stock_list
    
    def date_to_timestamp(self, date_str: str) -> int:
        """将日期字符串转换为时间戳（秒）"""
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            return int(dt.timestamp())
        except ValueError:
            raise ValueError(f"日期格式错误，应为 YYYY-MM-DD: {date_str}")
    
    def fetch_kline_data(self, stock_code: str, market: str, trade_date: str, stock_name: str = "") -> Optional[List[Dict]]:
        """获取指定日期的K线数据"""
        try:
            target_date = datetime.strptime(trade_date, "%Y-%m-%d")
            current_date = datetime.now()
            if target_date > current_date:
                with self.print_lock:
                    print(f"错误: 日期 {trade_date} 是未来日期")
                return None
            
            target_timestamp = self.date_to_timestamp(trade_date)
            current_timestamp = int(time.time())
            days_diff = (current_timestamp - target_timestamp) // 86400
            
            if days_diff < 0:
                with self.print_lock:
                    print(f"错误: 日期 {trade_date} 是未来日期")
                return None
            
            days_to_fetch = 640
            
            # API 的 end_time 必须 >= 0，所以：
            # 如果 trade_date 是今天，end_time = 0，begin_time = -640
            # 如果 trade_date 是过去的日期，end_time = 0，begin_time 需要确保能获取到 trade_date 的数据
            # 如果 trade_date 距离今天超过 640 天，需要调整 begin_time
            end_time = 0
            if days_diff > days_to_fetch:
                # trade_date 距离今天超过 640 天，需要获取更多数据
                begin_time = -(days_diff + days_to_fetch)
            else:
                # trade_date 距离今天在 640 天内，使用默认的 640 天
                begin_time = -days_to_fetch
            
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
                "begin_time": begin_time,
                "end_time": end_time,
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
                with self.print_lock:
                    print(f"请求失败 ({stock_code}): HTTP {response.status_code}")
                    try:
                        error_text = response.text[:500]
                        print(f"错误详情: {error_text}")
                    except:
                        pass
                return None
            
            try:
                data = response.json()
            except json.JSONDecodeError:
                with self.print_lock:
                    print(f"响应不是有效的JSON ({stock_code}): {response.text[:200]}")
                return None
            
            if os.getenv('DEBUG'):
                with self.print_lock:
                    print(f"  响应数据: {json.dumps(data, ensure_ascii=False, indent=2)[:500]}")
            
            if 'error' in data or 'message' in data:
                error_msg = data.get('error', data.get('message', '未知错误'))
                with self.print_lock:
                    print(f"接口返回错误 ({stock_code}): {error_msg}")
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
            
            with self.print_lock:
                print(f"无数据返回 ({stock_code}): {json.dumps(data, ensure_ascii=False)[:300]}")
            return None
            
        except Exception as e:
            with self.print_lock:
                print(f"获取K线数据失败 ({stock_code}, {trade_date}): {e}")
            import traceback
            if os.getenv('DEBUG'):
                with self.print_lock:
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
    
    def _fetch_single_stock(self, stock: Dict, trade_date: str) -> tuple:
        """单个股票抓取任务（用于并发）"""
        code = stock['code']
        market = stock['market']
        name = stock.get('name', '')
        
        kline_data = self.fetch_kline_data(code, market, trade_date, name)
        
        if kline_data:
            for record in kline_data:
                record['code'] = code
                record['market'] = market
            with self.print_lock:
                print(f"  ✓ {name}({code}): 成功 ({len(kline_data)} 条)")
            return (code, kline_data, True)
        else:
            with self.print_lock:
                print(f"  ✗ {name}({code}): 失败")
            return (code, [], False)
    
    def fetch_by_single_date(self, stock_codes: List[Dict], date: str):
        """按单个日期抓取数据"""
        print(f"开始抓取数据: {date}")
        print(f"股票数量: {len(stock_codes)}")
        print(f"并发数: {self.workers}")
        
        start_time = time.time()
        
        all_data = []
        success_count = 0
        fail_count = 0
        
        # 使用线程池并发抓取
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            # 提交所有任务
            future_to_stock = {
                executor.submit(self._fetch_single_stock, stock, date): stock
                for stock in stock_codes
            }
            
            # 收集结果
            for future in as_completed(future_to_stock):
                try:
                    code, kline_data, success = future.result()
                    if success:
                        all_data.extend(kline_data)
                        success_count += 1
                    else:
                        fail_count += 1
                except Exception as e:
                    stock = future_to_stock[future]
                    with self.print_lock:
                        print(f"  ✗ {stock.get('name', '')}({stock['code']}): 异常 - {e}")
                    fail_count += 1
        
        elapsed_time = time.time() - start_time
        print(f"\n完成: 成功 {success_count}, 失败 {fail_count}, 耗时 {elapsed_time:.2f}秒")
        
        if all_data:
            filename = f"kline_{date}.csv"
            self.save_to_csv(all_data, filename)
        else:
            print(f"  日期 {date} 无数据")


def main():
    TEST_LIMIT = None  # 测试时限制抓取数量，正式运行时改为None或0
    
    parser = argparse.ArgumentParser(description='同花顺K线数据抓取工具')
    parser.add_argument('--date', type=str, help='日期，格式: YYYY-MM-DD（不传则使用今天）')
    parser.add_argument('--codes', type=str, help='股票代码列表，逗号分隔，格式: 代码1,代码2 (需要配合--markets使用)')
    parser.add_argument('--markets', type=str, help='市场代码列表，逗号分隔，格式: 市场1,市场2 (需要配合--codes使用)。可用市场代码: 16=上证指数, 17=上海A股, 22=上海特殊股票, 32=深证指数, 33=深圳A股。示例: --codes 000001,600000 --markets 33,17')
    parser.add_argument('--workers', type=int, default=10, help='并发线程数，默认10（测试反爬时可调高，如50或100）')
    
    args = parser.parse_args()
    
    # 如果 --date 不传，默认使用今天的日期
    if not args.date:
        args.date = datetime.now().strftime("%Y-%m-%d")
        print(f"未指定日期，默认使用今天: {args.date}")
    
    fetcher = THSKlineFetcher(workers=args.workers)
    
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
        print("获取全部股票代码（包括指数和特殊分类）...")
        stock_codes = fetcher.get_all_stock_codes()
        if not stock_codes:
            print("获取股票代码失败")
            return
    
    if TEST_LIMIT and TEST_LIMIT > 0:
        original_count = len(stock_codes)
        stock_codes = stock_codes[:TEST_LIMIT]
        print(f"⚠️  测试模式：限制抓取数量为 {TEST_LIMIT} 只（原始数量: {original_count} 只）")
    
    fetcher.fetch_by_single_date(stock_codes, args.date)
    
    print("\n抓取完成！")


if __name__ == "__main__":
    main()

