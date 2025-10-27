#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UK Biobank爬虫 - 摘要重试脚本
专门重新获取摘要为"未找到摘要"的文章
"""

import sys
import os

# 修复Windows控制台编码问题
if sys.platform == 'win32':
    os.system('chcp 65001 >nul 2>&1')

from ukbiobank_scraper import UKBiobankScraperSelenium
import time
import csv
import json
from datetime import datetime
from typing import List, Dict

def find_articles_without_abstract(csv_filename: str) -> List[Dict[str, str]]:
    """
    从CSV文件中找出摘要为"未找到摘要"的文章
    
    Args:
        csv_filename: CSV文件名
        
    Returns:
        需要重新获取摘要的文章列表
    """
    articles_without_abstract = []
    
    try:
        # 尝试多种编码方式读取CSV文件
        encodings = ['utf-8-sig', 'utf-8', 'gbk', 'gb2312', 'latin-1']
        
        for encoding in encodings:
            try:
                with open(csv_filename, 'r', encoding=encoding) as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        abstract = row.get('abstract', '').strip()
                        if abstract in ['未找到摘要', '获取失败', '']:
                            articles_without_abstract.append(row)
                
                print(f"✓ 使用 {encoding} 编码从 {csv_filename} 中找到 {len(articles_without_abstract)} 篇缺少摘要的文章")
                return articles_without_abstract
                
            except UnicodeDecodeError:
                continue
            except Exception as e:
                print(f"✗ 使用 {encoding} 编码读取失败: {e}")
                continue
        
        print(f"✗ 无法使用任何编码读取CSV文件: {csv_filename}")
        return []
        
    except Exception as e:
        print(f"✗ 读取CSV文件失败: {e}")
        return []


def retry_abstract_extraction(articles: List[Dict[str, str]], output_csv: str, output_json: str, max_workers: int = 5) -> Dict[str, any]:
    """
    重新获取文章的摘要信息
    
    Args:
        articles: 需要重新获取摘要的文章列表
        output_csv: 输出CSV文件名
        output_json: 输出JSON文件名
        max_workers: 最大并发数
        
    Returns:
        包含统计信息的字典
    """
    if not articles:
        print("没有需要重新获取摘要的文章")
        return {'success': False, 'error': '没有需要处理的文章'}
    
    print("=" * 80)
    print("UK Biobank 爬虫 - 摘要重试脚本")
    print("=" * 80)
    print(f"需要重新获取摘要的文章数: {len(articles)}")
    print(f"输出文件: {output_csv}")
    print(f"并发数: {max_workers}")
    print("=" * 80)
    
    scraper = None
    
    try:
        # 创建爬虫实例
        scraper = UKBiobankScraperSelenium(headless=True)
        
        # 清空输出文件
        if os.path.exists(output_csv):
            os.remove(output_csv)
        if os.path.exists(output_json):
            os.remove(output_json)
        
        successful_count = 0
        failed_count = 0
        start_time = time.time()
        
        # 使用线程池并发处理
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_article = {}
            for idx, article in enumerate(articles, 1):
                future = executor.submit(
                    retry_single_article_abstract,
                    scraper,
                    article,
                    idx,
                    len(articles),
                    output_csv
                )
                future_to_article[future] = article
            
            # 处理完成的任务
            for future in as_completed(future_to_article):
                try:
                    success = future.result()
                    if success:
                        successful_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    failed_count += 1
                    print(f"✗ 文章处理异常: {e}")
        
        elapsed_time = time.time() - start_time
        
        # 生成JSON文件
        print("\n" + "=" * 80)
        print("生成JSON文件...")
        
        try:
            final_data = []
            if os.path.exists(output_csv):
                with open(output_csv, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        final_data.append(row)
                
                with open(output_json, 'w', encoding='utf-8') as f:
                    json.dump(final_data, f, ensure_ascii=False, indent=2)
                
                print(f"✓ JSON文件生成成功")
        except Exception as e:
            print(f"✗ JSON文件生成失败: {e}")
        
        # 统计结果
        print("\n" + "=" * 80)
        print("摘要重试完成！")
        print("=" * 80)
        print(f"总文章数: {len(articles)}")
        print(f"成功获取: {successful_count}")
        print(f"获取失败: {failed_count}")
        print(f"成功率: {successful_count / len(articles) * 100:.1f}%")
        print(f"耗时: {elapsed_time:.2f} 秒")
        print(f"平均速度: {len(articles) / elapsed_time:.2f} 篇/秒")
        
        print(f"\n输出文件:")
        print(f"  - CSV: {output_csv}")
        print(f"  - JSON: {output_json}")
        
        if final_data:
            # 统计摘要获取情况
            with_abstract = len([p for p in final_data if p.get('abstract') and p['abstract'] not in ['未找到摘要', '获取失败', '']])
            without_abstract = len(final_data) - with_abstract
            
            print(f"\n摘要统计:")
            print(f"  - 有摘要: {with_abstract} 篇")
            print(f"  - 无摘要: {without_abstract} 篇")
        
        return {
            'success': True,
            'total_articles': len(articles),
            'successful_count': successful_count,
            'failed_count': failed_count,
            'success_rate': successful_count / len(articles) * 100,
            'elapsed_time': elapsed_time,
            'csv_file': output_csv,
            'json_file': output_json
        }
        
    except Exception as e:
        print(f"\n摘要重试脚本执行出错: {e}")
        return {'success': False, 'error': str(e)}
    finally:
        if scraper:
            scraper.close()


def retry_single_article_abstract(scraper, article: Dict[str, str], idx: int, total: int, output_csv: str) -> bool:
    """
    重新获取单篇文章的摘要
    
    Args:
        scraper: 爬虫实例
        article: 文章信息
        idx: 文章索引
        total: 总文章数
        output_csv: 输出CSV文件名
        
    Returns:
        是否成功获取摘要
    """
    try:
        print(f"[{idx}/{total}] 重新获取摘要: {article['title'][:50]}...")
        
        # 使用简化版获取摘要
        success = scraper._fetch_article_details_simple(article, output_csv)
        
        if success:
            print(f"✓ [{idx}/{total}] 摘要获取成功")
        else:
            print(f"✗ [{idx}/{total}] 摘要获取失败")
        
        return success
        
    except Exception as e:
        print(f"✗ [{idx}/{total}] 摘要获取异常: {e}")
        return False


def merge_abstract_results(original_csv: str, abstract_csv: str, output_csv: str):
    """
    将重新获取的摘要结果合并到原始数据中
    
    Args:
        original_csv: 原始CSV文件
        abstract_csv: 包含新摘要的CSV文件
        output_csv: 合并后的输出文件
    """
    print("=" * 80)
    print("合并摘要结果")
    print("=" * 80)
    
    try:
        # 读取原始数据（尝试多种编码）
        original_data = []
        encodings = ['utf-8-sig', 'utf-8', 'gbk', 'gb2312', 'latin-1']
        
        for encoding in encodings:
            try:
                with open(original_csv, 'r', encoding=encoding) as f:
                    reader = csv.DictReader(f)
                    original_data = list(reader)
                print(f"✓ 使用 {encoding} 编码读取原始数据")
                break
            except UnicodeDecodeError:
                continue
            except Exception as e:
                print(f"✗ 使用 {encoding} 编码读取原始数据失败: {e}")
                continue
        
        if not original_data:
            print("✗ 无法读取原始数据文件")
            return
        
        # 读取新摘要数据
        abstract_data = []
        for encoding in encodings:
            try:
                with open(abstract_csv, 'r', encoding=encoding) as f:
                    reader = csv.DictReader(f)
                    abstract_data = list(reader)
                print(f"✓ 使用 {encoding} 编码读取摘要数据")
                break
            except UnicodeDecodeError:
                continue
            except Exception as e:
                print(f"✗ 使用 {encoding} 编码读取摘要数据失败: {e}")
                continue
        
        if not abstract_data:
            print("✗ 无法读取摘要数据文件")
            return
        
        # 创建链接到新摘要的映射
        abstract_map = {item['link']: item['abstract'] for item in abstract_data}
        
        # 更新原始数据中的摘要
        updated_count = 0
        for item in original_data:
            if item['link'] in abstract_map:
                old_abstract = item.get('abstract', '')
                new_abstract = abstract_map[item['link']]
                
                if old_abstract in ['未找到摘要', '获取失败', ''] and new_abstract not in ['未找到摘要', '获取失败', '']:
                    item['abstract'] = new_abstract
                    updated_count += 1
        
        # 保存合并后的数据
        fieldnames = ['title', 'date', 'authors', 'journal', 'publish_date', 'pubmed_id', 'doi', 'link', 'abstract']
        with open(output_csv, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(original_data)
        
        # 生成JSON文件
        json_output = output_csv.replace('.csv', '.json')
        with open(json_output, 'w', encoding='utf-8') as f:
            json.dump(original_data, f, ensure_ascii=False, indent=2)
        
        print(f"✓ 合并完成！")
        print(f"原始文章数: {len(original_data)}")
        print(f"更新摘要数: {updated_count}")
        print(f"输出文件:")
        print(f"  - CSV: {output_csv}")
        print(f"  - JSON: {json_output}")
        
        # 统计最终结果
        with_abstract = len([p for p in original_data if p.get('abstract') and p['abstract'] not in ['未找到摘要', '获取失败', '']])
        without_abstract = len(original_data) - with_abstract
        
        print(f"\n最终摘要统计:")
        print(f"  - 有摘要: {with_abstract} 篇 ({with_abstract/len(original_data)*100:.1f}%)")
        print(f"  - 无摘要: {without_abstract} 篇 ({without_abstract/len(original_data)*100:.1f}%)")
        
    except Exception as e:
        print(f"✗ 合并失败: {e}")


def main():
    """主函数"""
    import sys
    
    # 查找现有的数据文件
    csv_files = [f for f in os.listdir('.') if f.startswith('publications_heart_') and f.endswith('.csv')]
    
    if not csv_files:
        print("未找到现有的数据文件")
        print("请确保当前目录下有 publications_heart_*.csv 文件")
        return
    
    print("找到以下数据文件:")
    for i, f in enumerate(csv_files, 1):
        print(f"  {i}. {f}")
    
    # 使用最新的文件
    main_csv = csv_files[-1]
    print(f"\n使用文件: {main_csv}")
    
    # 查找缺少摘要的文章
    articles_without_abstract = find_articles_without_abstract(main_csv)
    
    if not articles_without_abstract:
        print("✓ 所有文章都有摘要，无需重新获取")
        return
    
    # 设置输出文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    abstract_csv = f'abstract_retry_{timestamp}.csv'
    abstract_json = f'abstract_retry_{timestamp}.json'
    
    # 重新获取摘要
    result = retry_abstract_extraction(
        articles_without_abstract,
        abstract_csv,
        abstract_json,
        max_workers=5
    )
    
    if result['success'] and result['successful_count'] > 0:
        # 询问是否合并结果
        print(f"\n是否将新获取的摘要合并到原始文件中？")
        print(f"原始文件: {main_csv}")
        print(f"新摘要文件: {abstract_csv}")
        
        # 自动合并（也可以改为交互式）
        merged_csv = f'merged_with_abstract_{timestamp}.csv'
        merge_abstract_results(main_csv, abstract_csv, merged_csv)
        
        print(f"\n✓ 摘要重试和合并完成！")
        print(f"最终文件: {merged_csv}")


if __name__ == "__main__":
    main()
