# DWAD - Do-Win Analysis & Decision

股票投机分析决策系统

## 项目简介

DWAD是一个基于掘金量化平台的股票投机分析决策系统，主要功能包括：

- 批量下载A股历史行情数据
- 股池管理和板块指数制作
- 概念强弱分析和可视化

## 快速开始

### 1. 环境准备

确保已安装Python 3.12和相关依赖：

```bash
pip install -r requirements.txt
pip install gm3  # 掘金SDK
```

### 2. 配置设置

复制配置文件模板：

```bash
cp config/config.yaml.example config/config.yaml
```

编辑 `config/config.yaml`，设置您的掘金API token：

```yaml
goldminer:
  token: "your_goldminer_token_here"
```

### 3. 下载数据

运行数据下载工具：

```bash
python scripts/download_data.py
```

程序会自动：
- **首次运行**：下载所有A股从2020年至今的历史数据
- **后续运行**：全量刷新数据（删除旧数据，重新下载全部历史数据）

> **注意**：为保证前复权数据的连续性，系统采用全量刷新策略。
> 每次更新需要10-30分钟，建议在收盘后自动执行。详见 [数据更新策略说明](docs/数据更新策略说明.md)

## 配置说明

### 主要配置项

```yaml
# 数据获取配置
data_fetcher:
  mode: auto                        # 运行模式：auto/initial/update/refresh
  default_start_date: "2020-01-01"  # 初始下载起始日期
  batch_size: 50                    # 批次大小
  resume_download: true             # 是否启用断点续传（仅首次下载有效）

# 数据存储配置
data_storage:
  base_path: "./data"               # 数据存储路径
```

### 数据存储结构

```
data/
├── stocks/          # 个股数据 (Parquet格式)
│   ├── SHSE_600000.parquet
│   └── SZSE_000001.parquet
├── indices/         # 指数数据
└── metadata/        # 元数据
    ├── stock_info.parquet
    └── update_log.json
```

## 使用说明

### 数据下载

程序会根据当前数据状态和配置自动选择操作模式：

- **`auto`（默认）**：自动判断
  - 首次运行：下载所有A股历史数据
  - 已有数据：全量刷新（删除旧数据并重新下载）
- **`initial`**：强制初始下载所有股票
- **`update/refresh`**：强制全量刷新

**数据更新策略**：

由于使用前复权数据，为保证数据连续性，系统采用**全量刷新**策略：
1. 删除所有现有股票数据
2. 重新下载从起始日期到今天的全部历史数据
3. 确保所有数据使用统一的复权基准

详细说明请参阅：[数据更新策略说明](docs/数据更新策略说明.md)

### 进度监控

下载过程中会显示：
- 当前处理进度（进度条）
- 正在下载的股票代码
- API访问限制提示
- 成功/失败统计

### 自动化执行

建议设置定时任务，每天收盘后自动更新：

**Windows任务计划程序**：
```
触发器：每天 17:00
操作：python scripts/download_data.py
起始位置：D:\projects\DWAD
```

**Linux/Mac crontab**：
```bash
# 每天17:00执行
0 17 * * 1-5 cd /path/to/DWAD && python scripts/download_data.py >> logs/daily_update.log 2>&1
```

### 错误处理

如遇到问题：
1. 检查掘金API token是否正确
2. 确认网络连接正常
3. 查看日志文件：`logs/dwad.log`
4. 检查磁盘空间：需要 >2GB 可用空间
5. 验证数据完整性（参见文档）

## 注意事项

1. **API访问限制**：掘金平台有访问频率限制，请求等待时间较长时请耐心等待
2. **存储空间**：全量A股数据约需要500MB-1GB存储空间，建议预留 >2GB
3. **网络环境**：建议在网络稳定的环境下运行，全量更新需要10-30分钟
4. **数据连续性**：每次更新会删除旧数据重新下载，以保证前复权数据的连续性

## 项目结构

```
DWAD/
├── src/dwad/                    # 源代码
│   ├── data_fetcher/           # 数据获取模块
│   ├── data_storage/           # 数据存储模块
│   ├── tools/                  # 工具模块
│   └── utils/                  # 工具函数
├── config/                     # 配置文件
├── data/                       # 数据存储目录
├── logs/                       # 日志文件
├── download_data.py            # 数据下载启动脚本
└── requirements.txt            # 依赖包列表
```

## 下一步开发

- [ ] 股池管理功能
- [ ] 指数计算引擎
- [ ] 概念强弱分析
- [ ] 可视化图表生成

## 许可证

[License information]