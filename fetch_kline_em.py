#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
东方财富K线数据抓取脚本
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


class EMKlineFetcher:
    """东方财富K线数据抓取类"""
    
    def __init__(self, use_proxy=False, proxy_list=None):
        self.base_url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        self.output_dir = "em"
        self.ths_fetcher = None
        self.use_proxy = use_proxy
        self.proxy_list = proxy_list or []
        self.session = requests.Session()
        self._update_headers()
        
    def _update_headers(self):
        """更新请求头，模拟真实浏览器（保持简洁，避免过度特征）"""
        user_agents = [
            ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36", '"macOS"'),
            ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36", '"Windows"'),
            ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36", '"Linux"')
        ]
        
        user_agent, platform = random.choice(user_agents)
        
        self.headers = {
            "User-Agent": user_agent,
            "Referer": "https://quote.eastmoney.com/",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site"
        }
        self.session.headers.update(self.headers)
        
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
        """将同花顺市场代码转换为东方财富市场代码"""
        # 同花顺市场代码映射
        # 17=上海A股 -> 1=上海, 33=深圳A股 -> 0=深圳
        # 注意：北交所的市场代码需要根据实际情况调整
        market_map = {
            '17': '1',  # 上海
            '33': '0'   # 深圳
        }
        
        em_market = market_map.get(ths_market)
        
        # 如果市场代码转换失败，根据股票代码判断（备用逻辑）
        if em_market is None and stock_code:
            # 先检查北交所代码（更特殊，需要优先判断）
            if stock_code.startswith(('43', '83', '87', '88', '920')):
                # 北交所股票代码识别：
                # - 43、83、87、88：旧代码体系
                # - 920：新代码体系（2024年4月起新上市，2025年10月9日起全面启用）
                # 北交所的市场代码为 '0'，例如：secid=0.920726
                em_market = '0'  # 北交所
            elif stock_code.startswith('6'):
                em_market = '1'  # 沪市
            elif stock_code.startswith(('0', '3')):
                em_market = '0'  # 深市
            else:
                em_market = '1'  # 默认沪市
        
        return em_market or '1'
    
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
            
            em_market = self.convert_market_code(market, stock_code)
            secid = f"{em_market}.{stock_code}"
            
            end_date_str = trade_date.replace("-", "")
            lmt = 100
            
            params = {
                "secid": secid,
                "ut": "fa5fd1943c7b386f172d6893dbfba10b",
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                "klt": "101",
                "fqt": "1",
                "end": end_date_str,
                "lmt": str(lmt),
                "cb": "quote_jp1"
            }
            
            if os.getenv('DEBUG'):
                print(f"  请求参数: {params}")
            
            max_retries = 3
            proxies = None
            if self.use_proxy and self.proxy_list:
                proxy = random.choice(self.proxy_list)
                proxies = {'http': proxy, 'https': proxy}
            
            for attempt in range(max_retries):
                try:
                    if attempt > 0:
                        self._update_headers()
                    
                    response = self.session.get(
                        self.base_url,
                        params=params,
                        proxies=proxies,
                        timeout=30
                    )
                    break
                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, 
                        requests.exceptions.ChunkedEncodingError, requests.exceptions.ProxyError) as e:
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 2 + random.uniform(0, 1)
                        if os.getenv('DEBUG'):
                            print(f"  重试 {attempt + 1}/{max_retries}, 等待 {wait_time:.2f} 秒...")
                        time.sleep(wait_time)
                        if self.use_proxy and self.proxy_list:
                            proxy = random.choice(self.proxy_list)
                            proxies = {'http': proxy, 'https': proxy}
                        continue
                    else:
                        raise
            
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
                # 处理JSONP响应格式：quote_jp1({...})
                if text.startswith('quote_jp1(') and text.endswith(')'):
                    text = text[10:-1]  # 移除 quote_jp1( 和 )
                elif text.startswith('(') and text.endswith(')'):
                    text = text[1:-1]
                elif 'callback(' in text or 'jQuery' in text or 'quote_jp' in text:
                    match = re.search(r'\{.*\}', text, re.DOTALL)
                    if match:
                        text = match.group(0)
                
                data = json.loads(text)
            except json.JSONDecodeError as e:
                print(f"响应不是有效的JSON: {response.text[:200]}")
                if os.getenv('DEBUG'):
                    print(f"原始响应: {response.text[:500]}")
                return None
            
            if os.getenv('DEBUG'):
                print(f"  响应数据: {json.dumps(data, ensure_ascii=False, indent=2)[:500]}")
            
            if data.get('rc') != 0:
                print(f"接口返回错误: rc={data.get('rc')}, msg={data.get('msg', '未知错误')}")
                return None
            
            if 'data' not in data:
                print(f"响应中无data字段: {json.dumps(data, ensure_ascii=False)[:300]}")
                return None
            
            data_obj = data['data']
            if not data_obj:
                print(f"data字段为空")
                return None
            
            klines = data_obj.get('klines', [])
            if not klines:
                print(f"无K线数据")
                return None
            
            kline_list = []
            prev_close = None
            for kline_str in klines:
                if not kline_str:
                    continue
                
                parts = kline_str.split(',')
                if len(parts) < 7:
                    continue
                
                try:
                    kline_date = parts[0]
                    
                    open_price = float(parts[1]) if parts[1] else None
                    close_price = float(parts[2]) if parts[2] else None
                    high_price = float(parts[3]) if parts[3] else None
                    low_price = float(parts[4]) if parts[4] else None
                    volume = float(parts[5]) if parts[5] else None
                    amount = float(parts[6]) if parts[6] else None
                    
                    kline_dict = {
                        'date': kline_date,
                        'open': open_price,
                        'close': close_price,
                        'high': high_price,
                        'low': low_price,
                        'volume': volume,
                        'amount': amount
                    }
                    
                    if len(parts) >= 8 and parts[7]:
                        try:
                            amplitude = float(parts[7])
                            kline_dict['zf'] = amplitude
                        except ValueError:
                            pass
                    
                    if len(parts) >= 9 and parts[8]:
                        try:
                            change_pct = float(parts[8])
                            kline_dict['zdf'] = change_pct
                        except ValueError:
                            pass
                    
                    if len(parts) >= 10 and parts[9]:
                        try:
                            change_amount = float(parts[9])
                            kline_dict['zde'] = change_amount
                        except ValueError:
                            pass
                    
                    if len(parts) >= 11 and parts[10]:
                        try:
                            turnover = float(parts[10])
                            kline_dict['turnover'] = turnover
                        except ValueError:
                            pass
                    
                    if close_price is not None:
                        prev_close = close_price
                    
                    kline_list.append(kline_dict)
                except (ValueError, IndexError) as e:
                    if os.getenv('DEBUG'):
                        print(f"解析K线数据失败: {kline_str}, 错误: {e}")
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
                
                delay = random.uniform(1.0, 2.5)
                time.sleep(delay)
            
            if all_data:
                filename = f"kline_{trade_date}.csv"
                self.save_to_csv(all_data, filename)
            else:
                print(f"  日期 {trade_date} 无数据")
    
    def fetch_by_single_date(self, stock_codes: List[Dict], date: str):
        """按单个日期抓取数据"""
        self.fetch_by_date_range(stock_codes, date, date)


def main():
    TEST_LIMIT = 10
    
    parser = argparse.ArgumentParser(description='东方财富K线数据抓取工具')
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
    
    fetcher = EMKlineFetcher()
    
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

