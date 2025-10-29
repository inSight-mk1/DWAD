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

#### 覆盖模式（默认）

从指定目录提取指数，生成独立的配置文件：

```bash
python scripts/extract_csi_index_pools.py --source zzzs
```

- 默认从 `pool_xls/zzzs` 目录读取
- 输出到 `config/stock_pools_csi_indices.yaml`
- 会覆盖已存在的同名文件

#### 追加模式

将新指数追加到现有股池配置文件：

```bash
python scripts/extract_csi_index_pools.py --source zzzs_25102001 --append
```

- `--source`: 指定数据源目录名（位于 `pool_xls/` 下）
- `--append`: 启用追加模式，将数据追加到 `config/stock_pools.yaml`
- 如果指数已存在会更新成分券，不存在则新增
- 适合定期添加新指数到现有配置

#### 使用 conda 环境

```bash
conda run -n dwad python scripts/extract_csi_index_pools.py --source zzzs_25102001 --append
```

### 3. 输出文件

- **覆盖模式**: 生成 `config/stock_pools_csi_indices.yaml`
- **追加模式**: 更新 `config/stock_pools.yaml`

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
3. **双模式支持**: 
   - 覆盖模式：生成独立配置文件
   - 追加模式：更新现有配置文件
4. **智能更新**: 追加模式下，已存在的指数会更新成分券，新指数会被添加
5. **日志记录**: 提供详细的处理日志和统计信息
6. **错误处理**: 对缺少必需列的文件会给出明确提示

## 扩展性

此脚本专门用于中证指数（CSI）的提取。如需提取其他类型的指数（如申万指数、同花顺指数等），可参考此脚本创建类似的提取脚本，如：
- `extract_sw_index_pools.py` - 申万指数提取
- `extract_ths_index_pools.py` - 同花顺指数提取

## 注意事项

1. 确保指定的数据源目录存在且包含 XLS 文件（如 `pool_xls/zzzs`、`pool_xls/zzzs_25102001` 等）
2. XLS 文件必须包含 "指数名称 Index Name" 和 "成份券名称Constituent Name" 列
3. **覆盖模式**: 生成的 YAML 文件会覆盖已存在的同名文件
4. **追加模式**: 需要确保 `config/stock_pools.yaml` 文件已存在
5. 追加模式会自动处理重复指数，更新其成分券列表

## 常见使用场景

### 场景1: 首次提取所有指数

```bash
python scripts/extract_csi_index_pools.py --source zzzs
```

### 场景2: 添加新指数到现有配置

当你在 `pool_xls` 下新建了目录（如 `zzzs_25102001`）并放入新的指数 XLS 文件：

```bash
# 追加到现有配置
conda run -n dwad python scripts/extract_csi_index_pools.py --source zzzs_25102001 --append
```

### 场景3: 更新指数成分券

如果某个指数的成分券发生变化，使用追加模式会自动更新：

```bash
conda run -n dwad python scripts/extract_csi_index_pools.py --source zzzs --append
```
