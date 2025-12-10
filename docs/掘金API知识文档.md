# 掘金API知识文档

## 从参考代码中提取的关键信息

### 1. API导入和基础设置
```python
from gm.api import *
```

### 2. 认证方式
使用token进行认证，在run函数中传入：
```python
run(strategy_id='c11e30fc-6d85-11ef-8ac0-80fa5b89feba',
    filename='main.py',
    mode=MODE_BACKTEST,
    token='479feb80f2d2bd55461465e2cfac0be64eba0e98',
    ...)
```

### 3. 股票代码格式
- **格式**: `SZSE.000001` (深交所) 或 `SHSE.600000` (上交所)
- **示例**:
  - 深交所: `SZSE.000001`, `SZSE.300001` (创业板)
  - 上交所: `SHSE.600000`, `SHSE.688000` (科创板)
- **代码提取**: 可以从完整代码中提取6位数字，如从 `sh600616` 提取 `600616`

### 4. 核心数据获取函数

#### 4.1 获取股票列表
```python
stocks = get_symbols(
    sec_type1=1010,        # 股票类型
    sec_type2=101001,      # A股
    symbols=None,
    trade_date=now_str,    # 交易日期 'YYYY-MM-DD'
    df=True                # 返回DataFrame
)['symbol'].tolist()
```

#### 4.2 获取历史行情数据
```python
close_prices = history_n(
    symbol=symbol,          # 股票代码
    frequency='1d',         # 频率：日线
    count=ma2,             # 获取数据条数
    end_time=now_str,      # 结束时间
    fields='close',        # 字段：'close', 'open', 'high', 'low', 'volume'等
    adjust=ADJUST_PREV,    # 复权方式：前复权
    adjust_end_time='',    # 复权基准时间
    df=False               # 返回格式：False=list, True=DataFrame
)
```

#### 4.3 获取估值数据
```python
valuation_data = stk_get_daily_valuation_pt(
    symbols=stocks,        # 股票代码列表
    fields='dy_lfy',       # 字段：股息率(近12个月)
    trade_date=now_str,    # 交易日期
    df=True               # 返回DataFrame
)
```

#### 4.4 获取市值数据
```python
mktvalue_data = stk_get_daily_mktvalue_pt(
    symbols=stocks,        # 股票代码列表
    fields='tot_mv',       # 字段：总市值
    trade_date=now_str,    # 交易日期
    df=True               # 返回DataFrame
)
```

### 5. 重要数据字段

#### 5.1 基础行情字段
- `open`: 开盘价
- `high`: 最高价
- `low`: 最低价
- `close`: 收盘价
- `volume`: 成交量
- `turnover`: 成交额

#### 5.2 估值字段
- `dy_lfy`: 股息率(近12个月)

#### 5.3 市值字段
- `tot_mv`: 总市值

### 6. 数据存储模式
参考代码使用pickle进行本地缓存：
```python
# 加载本地数据
def load_data(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'rb') as f:
            return pickle.load(f)
    return {}

# 保存数据到本地
def save_data(file_path, data):
    with open(file_path, 'wb') as f:
        pickle.dump(data, f)
```

### 7. 错误处理和优化
- 使用本地缓存避免重复API调用
- 按年份缓存股票列表减少查询次数
- 数据合并使用pandas的merge功能

### 8. 回测模式参数
```python
MODE_BACKTEST           # 回测模式
ADJUST_PREV            # 前复权
backtest_start_time    # 回测开始时间
backtest_end_time      # 回测结束时间
backtest_initial_cash  # 初始资金
backtest_commission_ratio  # 手续费率
backtest_slippage_ratio   # 滑点率
```

### 9. 适合DWAD系统的关键函数

#### 9.1 必需函数
- `get_symbols()`: 获取股票列表
- `history_n()`: 获取历史行情数据
- `stk_get_daily_mktvalue_pt()`: 获取市值数据

#### 9.2 可选函数
- `stk_get_daily_valuation_pt()`: 获取估值数据（用于更丰富的分析）

### 10. 数据格式示例

#### 10.1 历史行情数据格式
```python
[
    {'symbol': 'SHSE.600000', 'eob': datetime, 'close': 10.50},
    {'symbol': 'SHSE.600000', 'eob': datetime, 'close': 10.60},
    ...
]
```

#### 10.2 DataFrame格式
```python
   symbol     dy_lfy  tot_mv
0  SHSE.600000   3.2   5000000000
1  SZSE.000001   2.8   4500000000
...
```

### 11. 远程终端连接 (Linux环境)

掘金终端仅支持 Windows 系统，但可以通过远程连接方式在 Linux 环境中使用掘金 API。

#### 11.1 架构说明
```
┌─────────────────┐         ┌─────────────────┐
│  Linux 服务器    │  网络   │  Windows 终端    │
│  (运行Python)   │ ──────> │  (掘金终端)      │
│  set_serv_addr  │         │  监听端口        │
└─────────────────┘         └─────────────────┘
```

#### 11.2 核心函数：set_serv_addr
```python
from gm.api import set_token, set_serv_addr

# 设置远程终端地址（必须在 set_token 之前调用）
set_serv_addr("192.168.1.100:7001")  # Windows终端的IP和端口

# 然后设置token
set_token("your_token_here")
```

#### 11.3 配置文件方式
在 `config/config.yaml` 中配置：
```yaml
goldminer:
  token: "YOUR_TOKEN"
  serv_addr: "192.168.1.100:7001"  # Windows终端地址
```

#### 11.4 代码实现示例
```python
from gm.api import set_token, set_serv_addr, get_symbols

def init_goldminer(token: str, serv_addr: str = None):
    """初始化掘金API，支持远程终端连接"""
    # Linux环境需要先设置终端地址
    if serv_addr:
        set_serv_addr(serv_addr)
        print(f"掘金终端地址设置为: {serv_addr}")
    
    # 设置token
    set_token(token)
    
    # 验证连接
    try:
        symbols = get_symbols(sec_type1=1010, df=True)
        print(f"连接成功，获取到 {len(symbols)} 只股票")
        return True
    except Exception as e:
        print(f"连接失败: {e}")
        return False
```

#### 11.5 Windows 终端配置
1. 在 Windows 上安装并启动掘金终端
2. 确保终端监听端口（默认 7001）对外开放
3. 配置防火墙允许 Linux 服务器访问该端口
4. 确保 Windows 和 Linux 在同一网络或可互通

#### 11.6 常见问题
- **连接超时**: 检查防火墙设置和网络连通性
- **认证失败**: 确认 token 正确且终端已登录
- **调用顺序错误**: `set_serv_addr` 必须在 `set_token` 之前调用

### 12. 依赖包
需要安装掘金官方SDK：
```bash
pip install gm3
```