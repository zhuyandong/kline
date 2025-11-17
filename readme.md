## 项目简介
Kline 目录包含多个 Python 抓取脚本，可从同花顺、东方财富、百度、腾讯等接口获取 A 股日 K 线与分时数据。仓库自带 GitHub Actions 工作流，每天北京时间 18:00 自动执行所有脚本并将结果打包发布到 Releases，方便其他用户直接下载每日数据。

## 快速开始（本地运行）
1. 安装 Python 3.11（或兼容版本）以及 `requests` 依赖。
   ```bash
   pip install -r requirements.txt  # 如无该文件，可手动 pip install requests
   ```
2. 运行任一脚本，示例（当日日期）：
   ```bash
   python fetch_kline_ths.py --date 2025-11-17
   ```
   常用参数：
   - `--date YYYY-MM-DD`：单日数据。
   - `--start / --end`：日期区间。
   - `--codes`、`--markets`：限定股票与市场。

脚本输出位于根目录下的 `qq/`、`qq_timesharing/`、`ths/`、`ths_timesharing/`、`em/` 等文件夹。

## GitHub Actions & 数据下载
工作流 `Fetch Daily Data` 每日 18:00（UTC+8）自动运行，流程：
1. 依次执行全部抓取脚本，统一以当天日期为参数。
2. 将上述输出目录压缩为 `daily-data-YYYY-MM-DD.zip`。
3. 创建标签 `data-YYYY-MM-DD` 并生成同名 Release，上传压缩包。

其他用户如需下载每日数据：
- 打开仓库 `Releases` 页面，找到对应日期（`Daily data YYYY-MM-DD`），点击附件 `daily-data-YYYY-MM-DD.zip` 获取所有 CSV。
- 如需重新触发当日采集，可在 GitHub `Actions` 页面手动运行 `Fetch Daily Data`，完成后同样会生成 Release 与下载包。

## 数据字典
【日K线】
date - 交易日  
code - 股票代码  
open - 开盘价  
high - 最高价  
low - 最低价  
close - 收盘价  
volume - 成交量  
amount - 成交额  
zf - 振幅  
zdf - 涨跌幅  
zde - 涨跌额  
turnover - 换手率  
outstanding_share - 流动股本  
market - 市场代码  
timestamp - 时间戳

【分时】
