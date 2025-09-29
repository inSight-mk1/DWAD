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
python download_data.py
```

程序会自动：
- 首次运行：下载所有A股从2020年至今的历史数据
- 后续运行：更新数据到最新交易日

## 配置说明

### 主要配置项

```yaml
# 数据获取配置
data_fetcher:
  default_start_date: "2020-01-01"  # 初始下载起始日期
  batch_size: 1                     # 批次大小
  resume_download: true             # 是否启用断点续传

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

程序会根据当前数据状态自动选择操作模式：

- **首次运行**：检测到无数据时，会下载所有A股历史数据
- **更新模式**：检测到已有数据时，会更新到最新交易日

### 进度监控

下载过程中会显示：
- 当前处理进度
- API访问限制提示
- 成功/失败统计

### 错误处理

如遇到问题：
1. 检查掘金API token是否正确
2. 确认网络连接正常
3. 查看日志文件 `logs/dwad.log`

## 注意事项

1. **API访问限制**：掘金平台有访问频率限制，请求等待时间较长时请耐心等待
2. **存储空间**：全量A股数据约需要几GB存储空间
3. **网络环境**：建议在网络稳定的环境下运行

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