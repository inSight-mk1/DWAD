"""
同花顺指数股池提取脚本
从pool_xls/ths目录中的XLS文件中提取板块名称和成分股名称
将数据追加到YAML配置文件
"""

import os
import sys
from pathlib import Path
import pandas as pd
import yaml
from loguru import logger

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def extract_ths_index_data(xls_file_path: str) -> tuple[str, list[str]]:
    """
    从同花顺XLS文件中提取板块名称和成分股名称
    
    Args:
        xls_file_path: XLS文件路径
        
    Returns:
        (板块名称, 成分股名称列表)
    """
    try:
        xls_path = Path(xls_file_path)
        # 使用文件名（不含扩展名）作为板块名称
        index_name = xls_path.stem
        
        # 同花顺导出的.xls文件实际上是制表符分隔的文本文件
        df = pd.read_csv(xls_file_path, sep='\t', encoding='gbk')
        
        logger.info(f"文件 {xls_path.name} 的列名: {df.columns.tolist()}")
        
        # 尝试识别股票名称列（可能的列名）
        possible_name_columns = ['名称', '股票名称', '证券名称', '成份券名称', 'name', 'Name', 'STOCK_NAME']
        
        stock_name_col = None
        for col in possible_name_columns:
            if col in df.columns:
                stock_name_col = col
                break
        
        if stock_name_col is None:
            # 如果没有找到，显示所有列并使用第一个包含"名称"的列
            for col in df.columns:
                if '名称' in str(col):
                    stock_name_col = col
                    break
        
        if stock_name_col is None:
            logger.error(f"文件 {xls_path.name} 无法识别股票名称列")
            logger.info(f"可用列: {df.columns.tolist()}")
            logger.info(f"前3行数据:\n{df.head(3)}")
            return None, []
        
        logger.info(f"使用列 '{stock_name_col}' 作为股票名称")
        
        # 提取所有股票名称，去除空值和重复项
        stock_names = df[stock_name_col].dropna().unique().tolist()
        
        logger.info(f"从文件 {xls_path.name} 提取到板块: {index_name}, 成分股数量: {len(stock_names)}")
        
        return index_name, stock_names
        
    except Exception as e:
        logger.error(f"处理文件 {xls_file_path} 时出错: {e}")
        import traceback
        traceback.print_exc()
        return None, []


def extract_all_ths_indices(xls_dir: str) -> dict:
    """
    从指定目录提取所有同花顺指数数据
    
    Args:
        xls_dir: XLS文件所在目录
        
    Returns:
        指数数据字典，格式为 {板块名称: [成分股列表]}
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
        index_name, constituents = extract_ths_index_data(str(xls_file))
        if index_name and constituents:
            indices_data[index_name] = constituents
    
    return indices_data


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
                        logger.warning(f"板块 {index_name} 已存在，将更新其成分股")
                        updated_count += 1
                    else:
                        logger.info(f"添加新板块: {index_name}")
                        added_count += 1
                    existing_indices[index_name] = constituents
                
                logger.info(f"新增 {added_count} 个板块，更新 {updated_count} 个板块")
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
        "# 此文件由 extract_csi_index_pools.py 和 extract_ths_index_pools.py 自动生成和更新",
        f"# 数据来源: pool_xls/zzzs 和 pool_xls/ths 目录",
        f"# 包含 {total_indices} 个指数/板块",
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
        f.write('# 1. 所有指数/板块归类为"大概念股池"\n')
        f.write('# 2. 指数/板块名称即为概念名称\n')
        f.write('# 3. 成分股名称为该指数包含的所有个股\n')
        f.write('# 4. 系统会自动将中文名称转换为掘金API所需的股票代码格式\n')
    
    logger.success(f"数据已追加到: {yaml_file}")


def main():
    """主函数"""
    # 配置日志
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>")
    
    # 设置路径
    xls_dir = project_root / "pool_xls" / "ths"
    output_file = project_root / "config" / "stock_pools.yaml"
    
    logger.info("开始提取同花顺板块指数股池数据...")
    logger.info(f"XLS文件目录: {xls_dir}")
    logger.info(f"输出文件: {output_file}")
    
    # 提取指数数据
    indices_data = extract_all_ths_indices(str(xls_dir))
    
    if not indices_data:
        logger.error("没有提取到任何指数数据")
        return
    
    logger.info(f"成功提取 {len(indices_data)} 个板块")
    
    # 追加到YAML文件
    append_to_yaml(indices_data, str(output_file))
    
    # 打印统计信息
    logger.info("\n提取结果统计:")
    total_constituents = sum(len(constituents) for constituents in indices_data.values())
    logger.info(f"  板块总数: {len(indices_data)}")
    logger.info(f"  成分股总数: {total_constituents}")
    logger.info(f"  平均每个板块成分股数: {total_constituents / len(indices_data):.1f}")
    
    logger.info("\n板块列表:")
    for index_name, constituents in indices_data.items():
        logger.info(f"  - {index_name}: {len(constituents)} 只成分股")
    
    logger.success("同花顺板块指数股池数据提取完成!")


if __name__ == "__main__":
    main()
