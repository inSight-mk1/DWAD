# 配置文件说明

本目录包含DWAD系统的配置文件。为了保护敏感信息（如API token）和个人配置，实际使用的配置文件不会上传到git仓库。

## 配置文件列表

### 1. config.yaml（必需）
系统主配置文件，包含：
- 掘金API token（敏感信息）
- 数据存储路径
- 数据获取参数
- 指数计算参数
- 日志配置

**首次使用：**
```bash
cp config_example.yaml config.yaml
```
然后编辑 `config.yaml`，填入您的掘金API token。

### 2. stock_pools.yaml（必需）
股池配置文件，定义所有股票池及其成分股。

**首次使用：**
```bash
cp stock_pools_example.yaml stock_pools.yaml
```
然后根据需要配置您的股票池。

### 3. index_comparison.yaml（可选）
指数比较配置文件，用于配置需要比较的指数列表和可视化参数。

**首次使用：**
```bash
cp index_comparison_example.yaml index_comparison.yaml
```
然后配置您需要比较的指数。

## 文件说明

- `*_example.yaml` - 示例配置文件，会上传到git仓库
- `*.yaml`（除example外） - 实际使用的配置文件，不会上传到git仓库

## 注意事项

1. **不要直接修改 `*_example.yaml` 文件**，这些文件仅作为模板使用
2. **不要将包含敏感信息的配置文件上传到git**（已在 `.gitignore` 中配置）
3. 如果需要更新示例配置，请修改 `*_example.yaml` 文件并提交
4. 团队成员克隆仓库后，需要手动复制并配置自己的配置文件
