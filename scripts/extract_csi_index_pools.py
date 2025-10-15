"""
中证指数股池提取脚本
从pool_xls/zzzs目录中的XLS文件中提取指数名称和成分券名称
将数据写入YAML配置文件
"""

import os
import sys
import argparse
from pathlib import Path
import pandas as pd
import yaml
from loguru import logger

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def extract_index_data_from_xls(xls_file_path: str) -> tuple[str, list[str]]:
    """
    从XLS文件中提取指数名称和成分券名称
    
    Args:
        xls_file_path: XLS文件路径
        
    Returns:
        (指数名称, 成分券名称列表)
    """
    try:
        # 读取XLS文件
        df = pd.read_excel(xls_file_path)
        
        # 检查必需的列是否存在（使用中英文混合的列名）
        index_name_col = '指数名称 Index Name'
        constituent_name_col = '成份券名称Constituent Name'
        
        if index_name_col not in df.columns:
            logger.error(f"文件 {xls_file_path} 缺少必需列: {index_name_col}")
            logger.info(f"实际列名: {df.columns.tolist()}")
            return None, []
        
        if constituent_name_col not in df.columns:
            logger.error(f"文件 {xls_file_path} 缺少必需列: {constituent_name_col}")
            logger.info(f"实际列名: {df.columns.tolist()}")
            return None, []
        
        # 提取指数名称（取第一行的指数名称，因为同一个文件中的指数名称应该相同）
        index_name = df[index_name_col].iloc[0]
        
        # 提取所有成分券名称，去除空值和重复项
        constituent_names = df[constituent_name_col].dropna().unique().tolist()
        
        logger.info(f"从文件 {Path(xls_file_path).name} 提取到指数: {index_name}, 成分券数量: {len(constituent_names)}")
        
        return index_name, constituent_names
        
    except Exception as e:
        logger.error(f"处理文件 {xls_file_path} 时出错: {e}")
        return None, []


def extract_all_csi_indices(xls_dir: str) -> dict:
    """
    从指定目录提取所有中证指数数据
    
    Args:
        xls_dir: XLS文件所在目录
        
    Returns:
        指数数据字典，格式为 {指数名称: [成分券列表]}
    """
    xls_dir_path = Path(xls_dir)
    
    if not xls_dir_path.exists():
        logger.error(f"目录不存在: {xls_dir}")
        return {}
    
    # 查找所有XLS文件
    xls_files = list(xls_dir_path.glob("*.xls")) + list(xls_dir_path.glob("*.xlsx"))
    
    if not xls_files:
        logger.warning(f"目录 {xls_dir} 中没有找到XLS文件")
        return {}
    
    logger.info(f"找到 {len(xls_files)} 个XLS文件")
    
    # 提取所有指数数据
    indices_data = {}
    for xls_file in xls_files:
        index_name, constituents = extract_index_data_from_xls(str(xls_file))
        if index_name and constituents:
            indices_data[index_name] = constituents
    
    return indices_data


def write_to_yaml(indices_data: dict, output_file: str, pool_category: str = "大概念股池"):
    """
    将指数数据写入YAML文件（覆盖模式）
    
    Args:
        indices_data: 指数数据字典
        output_file: 输出文件路径
        pool_category: 股池类别，默认为"大概念股池"
    """
    # 构建YAML数据结构
    yaml_data = {
        "stock_pools": {
            pool_category: indices_data
        }
    }
    
    # 添加说明注释
    comments = [
        "# 中证指数股池配置文件",
        "# 此文件由 extract_csi_index_pools.py 自动生成",
        f"# 数据来源: pool_xls/zzzs 目录",
        f"# 包含 {len(indices_data)} 个中证指数",
        ""
    ]
    
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # 写入YAML文件
    with open(output_file, 'w', encoding='utf-8') as f:
        # 写入注释
        f.write('\n'.join(comments))
        f.write('\n')
        
        # 写入YAML数据
        yaml.dump(yaml_data, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
        
        # 添加尾部说明
        f.write('\n')
        f.write('# 说明：\n')
        f.write('# 1. 所有中证指数归类为"大概念股池"\n')
        f.write('# 2. 指数名称即为概念名称\n')
        f.write('# 3. 成分券名称为该指数包含的所有个股\n')
        f.write('# 4. 系统会自动将中文名称转换为掘金API所需的股票代码格式\n')
    
    logger.info(f"数据已写入: {output_file}")


def append_to_yaml(new_indices: dict, yaml_file: str, pool_category: str = "大概念股池"):
    """
    将指数数据追加到现有YAML文件中
    
    Args:
        new_indices: 要追加的指数数据字典
        yaml_file: YAML文件路径
        pool_category: 股池类别，默认为"大概念股池"
    """
    yaml_path = Path(yaml_file)
    
    # 读取现有YAML文件
    if yaml_path.exists():
        with open(yaml_file, 'r', encoding='utf-8') as f:
            existing_data = yaml.safe_load(f)
        
        if existing_data and "stock_pools" in existing_data:
            if pool_category in existing_data["stock_pools"]:
                # 追加新的指数到现有类别
                existing_indices = existing_data["stock_pools"][pool_category]
                
                # 检查重复的指数名称
                added_count = 0
                updated_count = 0
                for index_name, constituents in new_indices.items():
                    if index_name in existing_indices:
                        logger.warning(f"指数 {index_name} 已存在，将更新其成分券")
                        updated_count += 1
                    else:
                        logger.info(f"添加新指数: {index_name}")
                        added_count += 1
                    existing_indices[index_name] = constituents
                
                logger.info(f"新增 {added_count} 个指数，更新 {updated_count} 个指数")
            else:
                # 创建新类别
                existing_data["stock_pools"][pool_category] = new_indices
                logger.info(f"创建新类别: {pool_category}")
        else:
            # 文件为空或格式不正确，创建新结构
            existing_data = {
                "stock_pools": {
                    pool_category: new_indices
                }
            }
    else:
        logger.error(f"文件 {yaml_file} 不存在，请先创建基础股池文件")
        return
    
    # 写入更新后的数据
    yaml_path.parent.mkdir(parents=True, exist_ok=True)
    
    # 计算总指数数量
    total_indices = len(existing_data["stock_pools"].get(pool_category, {}))
    
    # 更新注释
    comments = [
        "# 中证指数股池配置文件",
        "# 此文件由 extract_csi_index_pools.py 自动生成和更新",
        f"# 数据来源: pool_xls/zzzs 等目录",
        f"# 包含 {total_indices} 个指数",
        ""
    ]
    
    with open(yaml_file, 'w', encoding='utf-8') as f:
        # 写入注释
        f.write('\n'.join(comments))
        f.write('\n')
        
        # 写入YAML数据
        yaml.dump(existing_data, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
        
        # 添加尾部说明
        f.write('\n')
        f.write('# 说明：\n')
        f.write('# 1. 所有中证指数归类为"大概念股池"\n')
        f.write('# 2. 指数名称即为概念名称\n')
        f.write('# 3. 成分券名称为该指数包含的所有个股\n')
        f.write('# 4. 系统会自动将中文名称转换为掘金API所需的股票代码格式\n')
    
    logger.success(f"数据已追加到: {yaml_file}")


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="中证指数股池提取脚本")
    parser.add_argument(
        '--source',
        type=str,
        default='zzzs',
        help='指数数据源目录名（位于pool_xls下），默认为 zzzs'
    )
    parser.add_argument(
        '--append',
        action='store_true',
        help='追加模式：将提取的数据追加到 stock_pools.yaml，默认为覆盖模式写入 stock_pools_csi_indices.yaml'
    )
    args = parser.parse_args()
    
    # 配置日志
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>")
    
    # 设置路径
    xls_dir = project_root / "pool_xls" / args.source
    
    # 根据模式选择输出文件
    if args.append:
        output_file = project_root / "config" / "stock_pools.yaml"
        mode_str = "追加模式"
    else:
        output_file = project_root / "config" / "stock_pools_csi_indices.yaml"
        mode_str = "覆盖模式"
    
    logger.info(f"开始提取中证指数股池数据... ({mode_str})")
    logger.info(f"XLS文件目录: {xls_dir}")
    logger.info(f"输出文件: {output_file}")
    
    # 提取指数数据
    indices_data = extract_all_csi_indices(str(xls_dir))
    
    if not indices_data:
        logger.error("没有提取到任何指数数据")
        return
    
    logger.info(f"成功提取 {len(indices_data)} 个指数")
    
    # 根据模式写入YAML文件
    if args.append:
        append_to_yaml(indices_data, str(output_file))
    else:
        write_to_yaml(indices_data, str(output_file))
    
    # 打印统计信息
    logger.info("\n提取结果统计:")
    total_constituents = sum(len(constituents) for constituents in indices_data.values())
    logger.info(f"  指数总数: {len(indices_data)}")
    logger.info(f"  成分券总数: {total_constituents}")
    logger.info(f"  平均每个指数成分券数: {total_constituents / len(indices_data):.1f}")
    
    logger.info("\n指数列表:")
    for index_name, constituents in indices_data.items():
        logger.info(f"  - {index_name}: {len(constituents)} 只成分券")
    
    logger.success("中证指数股池数据提取完成!")


if __name__ == "__main__":
    main()
