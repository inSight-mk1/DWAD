# 中证指数股池提取脚本使用说明

## 脚本功能

`extract_csi_index_pools.py` 用于从 `pool_xls/zzzs` 目录中的 XLS 文件提取中证指数数据，并生成 YAML 配置文件。

## 使用方法

### 1. 确保依赖已安装

```bash
pip install pandas pyyaml xlrd openpyxl loguru
```

或者安装项目所有依赖：

```bash
pip install -r requirements.txt
```

### 2. 运行提取脚本

```bash
python scripts/extract_csi_index_pools.py
```

### 3. 输出文件

脚本会生成 `config/stock_pools_csi_indices.yaml` 文件，包含所有中证指数及其成分券。

## 数据格式

### 输入格式（XLS文件）

XLS 文件需包含以下列：
- **Index Name**: 指数名称（如"沪深300"）
- **Constituent Name**: 成分券名称（如"贵州茅台"）

### 输出格式（YAML文件）

```yaml
stock_pools:
  大概念股池:
    指数名称1:
      - "成分券1"
      - "成分券2"
      - ...
    指数名称2:
      - "成分券1"
      - "成分券2"
      - ...
```

## 脚本特点

1. **自动提取**: 自动读取目录中所有 XLS/XLSX 文件
2. **去重处理**: 自动去除重复的成分券名称
3. **日志记录**: 提供详细的处理日志和统计信息
4. **错误处理**: 对缺少必需列的文件会给出明确提示

## 扩展性

此脚本专门用于中证指数（CSI）的提取。如需提取其他类型的指数（如申万指数、同花顺指数等），可参考此脚本创建类似的提取脚本，如：
- `extract_sw_index_pools.py` - 申万指数提取
- `extract_ths_index_pools.py` - 同花顺指数提取

## 注意事项

1. 确保 `pool_xls/zzzs` 目录存在且包含 XLS 文件
2. XLS 文件必须包含 "Index Name" 和 "Constituent Name" 列
3. 生成的 YAML 文件会覆盖已存在的同名文件
