#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UK Biobank爬虫 - 快速摘要重试脚本
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

def quick_retry_abstracts():
    """快速重新获取摘要"""
    
    # 查找现有的数据文件
    csv_files = [f for f in os.listdir('.') if f.startswith('publications_heart_') and f.endswith('.csv')]
    
    if not csv_files:
        print("未找到现有的数据文件")
        return
    
    # 使用最新的文件
    main_csv = csv_files[-1]
    print(f"使用文件: {main_csv}")
    
    # 找出缺少摘要的文章
    articles_without_abstract = []
    
    try:
        # 尝试多种编码方式读取CSV文件
        encodings = ['utf-8-sig', 'utf-8', 'gbk', 'gb2312', 'latin-1']
        
        for encoding in encodings:
            try:
                with open(main_csv, 'r', encoding=encoding) as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        abstract = row.get('abstract', '').strip()
                        if abstract in ['未找到摘要', '获取失败', '']:
                            articles_without_abstract.append(row)
                
                print(f"✓ 使用 {encoding} 编码找到 {len(articles_without_abstract)} 篇缺少摘要的文章")
                break
                
            except UnicodeDecodeError:
                continue
            except Exception as e:
                print(f"✗ 使用 {encoding} 编码读取失败: {e}")
                continue
        
        if not articles_without_abstract and len(encodings) > 0:
            print(f"✗ 无法使用任何编码读取CSV文件: {main_csv}")
            return
        
    except Exception as e:
        print(f"读取文件失败: {e}")
        return
    
    if not articles_without_abstract:
        print("✓ 所有文章都有摘要，无需重新获取")
        return
    
    print("=" * 80)
    print("开始重新获取摘要...")
    print("=" * 80)
    
    scraper = None
    
    try:
        # 创建爬虫实例
        scraper = UKBiobankScraperSelenium(headless=True)
        
        # 设置输出文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_csv = f'abstract_retry_{timestamp}.csv'
        output_json = f'abstract_retry_{timestamp}.json'
        
        # 清空输出文件
        if os.path.exists(output_csv):
            os.remove(output_csv)
        if os.path.exists(output_json):
            os.remove(output_json)
        
        successful_count = 0
        failed_count = 0
        
        start_time = time.time()
        
        for i, article in enumerate(articles_without_abstract, 1):
            try:
                print(f"\n[{i}/{len(articles_without_abstract)}] 重新获取摘要...")
                print(f"标题: {article['title'][:60]}...")
                
                # 重新获取摘要
                success = scraper._fetch_article_details_simple(article, output_csv)
                
                if success:
                    successful_count += 1
                    print(f"✓ 成功")
                else:
                    failed_count += 1
                    print(f"✗ 失败")
                
                # 短暂延迟
                time.sleep(2)
                
            except Exception as e:
                failed_count += 1
                print(f"✗ 异常: {e}")
        
        elapsed_time = time.time() - start_time
        
        # 生成JSON文件
        if os.path.exists(output_csv):
            final_data = []
            with open(output_csv, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    final_data.append(row)
            
            with open(output_json, 'w', encoding='utf-8') as f:
                json.dump(final_data, f, ensure_ascii=False, indent=2)
        
        # 显示结果
        print("\n" + "=" * 80)
        print("摘要重试完成！")
        print("=" * 80)
        print(f"总文章数: {len(articles_without_abstract)}")
        print(f"成功获取: {successful_count}")
        print(f"获取失败: {failed_count}")
        print(f"成功率: {successful_count / len(articles_without_abstract) * 100:.1f}%")
        print(f"耗时: {elapsed_time:.2f} 秒")
        
        print(f"\n输出文件:")
        print(f"  - {output_csv}")
        print(f"  - {output_json}")
        
        # 统计摘要情况
        if os.path.exists(output_csv):
            with_abstract = 0
            without_abstract = 0
            
            with open(output_csv, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    abstract = row.get('abstract', '').strip()
                    if abstract not in ['未找到摘要', '获取失败', '']:
                        with_abstract += 1
                    else:
                        without_abstract += 1
            
            print(f"\n摘要统计:")
            print(f"  - 有摘要: {with_abstract} 篇")
            print(f"  - 无摘要: {without_abstract} 篇")
        
        return {
            'success': True,
            'successful_count': successful_count,
            'failed_count': failed_count,
            'output_csv': output_csv,
            'output_json': output_json
        }
        
    except Exception as e:
        print(f"\n摘要重试失败: {e}")
        return {'success': False, 'error': str(e)}
    finally:
        if scraper:
            scraper.close()


if __name__ == "__main__":
    quick_retry_abstracts()
