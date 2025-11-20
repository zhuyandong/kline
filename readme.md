## 如何下载每日数据
仓库每天北京时间 18:00 自动抓取同花顺、百度、腾讯等来源的数据，并在 GitHub Releases 发布压缩包：
1. 打开仓库主页 → 进入 `Releases`。
2. 找到日期形如 `Daily data YYYY-MM-DD` 的最新发布。
3. 下载附件 `daily-data-YYYY-MM-DD.zip`，解压即可获得 `data/qq/`、`data/ths/`、`data/bd/` 等目录内的 CSV。
如需重新生成某天的数据，可在 `Actions` 中手动运行 `Fetch Daily Data` 工作流，完成后会自动生成新的 Release。

## 数据字段说明

### 日K线数据

#### 同花顺 (THS) - `data/ths/kline_*.csv`
- `date` - 交易日
- `code` - 股票代码
- `open` - 开盘价
- `high` - 最高价
- `low` - 最低价
- `close` - 收盘价
- `volume` - 成交量
- `amount` - 成交额
- `turnover` - 换手率
- `outstanding_share` - 流动股本
- `market` - 市场代码
- `timestamp` - 时间戳（毫秒）

#### 腾讯 (QQ) - `data/qq/kline_*.csv`
- `date` - 交易日
- `code` - 股票代码
- `open` - 开盘价
- `high` - 最高价
- `low` - 最低价
- `close` - 收盘价
- `volume` - 成交量
- `amount` - 成交额
- `zf` - 振幅
- `zdf` - 涨跌幅
- `zde` - 涨跌额
- `turnover` - 换手率
- `outstanding_share` - 流动股本
- `market` - 市场代码
- `timestamp` - 时间戳

#### 百度 (BD) - `data/bd/kline_*.csv`
- `time` - 交易日
- `code` - 股票代码
- `open` - 开盘价
- `high` - 最高价
- `low` - 最低价
- `close` - 收盘价
- `volume` - 成交量
- `amount` - 成交额
- `range` - 振幅
- `ratio` - 涨跌幅
- `turnoverratio` - 换手率
- `preClose` - 前收盘价
- `ma5avgprice` - 5日均价
- `ma5volume` - 5日均量
- `ma10avgprice` - 10日均价
- `ma10volume` - 10日均量
- `ma20avgprice` - 20日均价
- `ma20volume` - 20日均量
- `market` - 市场代码
- `timestamp` - 时间戳（秒）
