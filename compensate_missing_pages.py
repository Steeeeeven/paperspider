#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UK Biobank爬虫 - 补偿脚本
专门爬取未找到文章列表的页面
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

def compensate_missing_pages():
    """补偿爬取缺失的页面"""
    
    # 缺失的页面列表
    missing_pages = [5, 7, 14, 27, 38, 40, 41, 46, 47, 92, 111, 135, 146, 154, 181, 200, 209, 220, 223]
    
    print("=" * 80)
    print("UK Biobank 爬虫 - 补偿脚本")
    print("=" * 80)
    print(f"需要补偿的页面: {missing_pages}")
    print(f"总页数: {len(missing_pages)}")
    print("=" * 80)
    
    scraper = None
    
    try:
        # 创建爬虫实例
        scraper = UKBiobankScraperSelenium(headless=True)
        
        keyword = "heart"
        
        # 设置输出文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f'compensate_missing_pages_{timestamp}.csv'
        json_filename = f'compensate_missing_pages_{timestamp}.json'
        
        print(f"\n开始补偿爬取...")
        print(f"输出文件: {csv_filename}, {json_filename}")
        print("=" * 80)
        
        successful_pages = 0
        failed_pages = []
        total_articles = 0
        
        start_time = time.time()
        
        for i, page_num in enumerate(missing_pages, 1):
            try:
                print(f"\n[{i}/{len(missing_pages)}] 正在补偿第 {page_num} 页...")
                
                # 使用并发方法爬取单个页面
                result = scraper._fetch_page_concurrent(
                    keyword=keyword,
                    page_num=page_num,
                    csv_filename=csv_filename,
                    json_filename=json_filename
                )
                
                if result['success']:
                    successful_pages += 1
                    total_articles += result['articles_count']
                    print(f"✓ 第 {page_num} 页补偿成功 | 文章数: {result['articles_count']}")
                else:
                    failed_pages.append(page_num)
                    print(f"✗ 第 {page_num} 页补偿失败: {result['error']}")
                
                # 添加延迟避免请求过快
                time.sleep(3)
                
            except Exception as e:
                failed_pages.append(page_num)
                print(f"✗ 第 {page_num} 页补偿异常: {e}")
        
        elapsed_time = time.time() - start_time
        
        # 生成JSON文件
        print("\n" + "=" * 80)
        print("生成JSON文件...")
        
        try:
            final_data = []
            if os.path.exists(csv_filename):
                with open(csv_filename, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        final_data.append(row)
                
                with open(json_filename, 'w', encoding='utf-8') as f:
                    json.dump(final_data, f, ensure_ascii=False, indent=2)
                
                print(f"✓ JSON文件生成成功")
        except Exception as e:
            print(f"✗ JSON文件生成失败: {e}")
        
        # 统计结果
        print("\n" + "=" * 80)
        print("补偿爬取完成！")
        print("=" * 80)
        print(f"补偿页数: {len(missing_pages)}")
        print(f"成功页数: {successful_pages}")
        print(f"失败页数: {len(failed_pages)}")
        print(f"总文章数: {total_articles}")
        print(f"耗时: {elapsed_time:.2f} 秒")
        print(f"平均速度: {total_articles / elapsed_time:.2f} 篇/秒")
        
        if failed_pages:
            print(f"\n失败的页面: {failed_pages}")
            print("建议：")
            print("1. 检查这些页面是否真的存在文章")
            print("2. 可能需要调整页面范围")
            print("3. 或者这些页面确实为空页面")
        
        print(f"\n文件位置:")
        print(f"  - CSV: {csv_filename}")
        print(f"  - JSON: {json_filename}")
        
        if final_data:
            print(f"\n数据统计:")
            print(f"  - 有摘要: {len([p for p in final_data if p.get('abstract') and p['abstract'] not in ['未找到摘要', '获取失败']])} 篇")
            print(f"  - 有DOI: {len([p for p in final_data if p.get('doi')])} 篇")
            print(f"  - 有PubMed ID: {len([p for p in final_data if p.get('pubmed_id')])} 篇")
        
        return {
            'success': True,
            'total_pages': len(missing_pages),
            'successful_pages': successful_pages,
            'failed_pages': failed_pages,
            'total_articles': total_articles,
            'elapsed_time': elapsed_time,
            'csv_file': csv_filename,
            'json_file': json_filename
        }
        
    except Exception as e:
        print(f"\n补偿脚本执行出错: {e}")
        return {'success': False, 'error': str(e)}
    finally:
        if scraper:
            scraper.close()


def compensate_with_retry():
    """带重试机制的补偿爬取"""
    
    missing_pages = [5, 7, 14, 27, 38, 40, 41, 46, 47, 92, 111, 135, 146, 154, 181, 200, 209, 220, 223]
    
    print("=" * 80)
    print("UK Biobank 爬虫 - 补偿脚本（带重试）")
    print("=" * 80)
    print(f"需要补偿的页面: {missing_pages}")
    print(f"总页数: {len(missing_pages)}")
    print("=" * 80)
    
    scraper = None
    
    try:
        # 创建爬虫实例
        scraper = UKBiobankScraperSelenium(headless=True)
        
        keyword = "heart"
        
        # 设置输出文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f'compensate_retry_{timestamp}.csv'
        json_filename = f'compensate_retry_{timestamp}.json'
        
        print(f"\n开始补偿爬取（带重试）...")
        print(f"输出文件: {csv_filename}, {json_filename}")
        print("=" * 80)
        
        successful_pages = []
        failed_pages = []
        total_articles = 0
        
        start_time = time.time()
        
        for i, page_num in enumerate(missing_pages, 1):
            max_retries = 3
            success = False
            
            for retry in range(max_retries):
                try:
                    print(f"\n[{i}/{len(missing_pages)}] 正在补偿第 {page_num} 页 (尝试 {retry + 1}/{max_retries})...")
                    
                    # 使用并发方法爬取单个页面
                    result = scraper._fetch_page_concurrent(
                        keyword=keyword,
                        page_num=page_num,
                        csv_filename=csv_filename,
                        json_filename=json_filename
                    )
                    
                    if result['success']:
                        successful_pages.append(page_num)
                        total_articles += result['articles_count']
                        print(f"✓ 第 {page_num} 页补偿成功 | 文章数: {result['articles_count']}")
                        success = True
                        break
                    else:
                        print(f"✗ 第 {page_num} 页补偿失败: {result['error']}")
                        if retry < max_retries - 1:
                            print(f"  等待 5 秒后重试...")
                            time.sleep(5)
                
                except Exception as e:
                    print(f"✗ 第 {page_num} 页补偿异常: {e}")
                    if retry < max_retries - 1:
                        print(f"  等待 5 秒后重试...")
                        time.sleep(5)
            
            if not success:
                failed_pages.append(page_num)
                print(f"✗ 第 {page_num} 页最终失败")
            
            # 添加延迟避免请求过快
            time.sleep(2)
        
        elapsed_time = time.time() - start_time
        
        # 生成JSON文件
        print("\n" + "=" * 80)
        print("生成JSON文件...")
        
        try:
            final_data = []
            if os.path.exists(csv_filename):
                with open(csv_filename, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        final_data.append(row)
                
                with open(json_filename, 'w', encoding='utf-8') as f:
                    json.dump(final_data, f, ensure_ascii=False, indent=2)
                
                print(f"✓ JSON文件生成成功")
        except Exception as e:
            print(f"✗ JSON文件生成失败: {e}")
        
        # 统计结果
        print("\n" + "=" * 80)
        print("补偿爬取完成！")
        print("=" * 80)
        print(f"补偿页数: {len(missing_pages)}")
        print(f"成功页数: {len(successful_pages)}")
        print(f"失败页数: {len(failed_pages)}")
        print(f"总文章数: {total_articles}")
        print(f"耗时: {elapsed_time:.2f} 秒")
        print(f"平均速度: {total_articles / elapsed_time:.2f} 篇/秒")
        
        if successful_pages:
            print(f"\n成功的页面: {successful_pages}")
        
        if failed_pages:
            print(f"\n失败的页面: {failed_pages}")
            print("建议：")
            print("1. 这些页面可能确实为空页面")
            print("2. 或者页面结构发生了变化")
            print("3. 可以手动检查这些页面的URL")
        
        print(f"\n文件位置:")
        print(f"  - CSV: {csv_filename}")
        print(f"  - JSON: {json_filename}")
        
        if final_data:
            print(f"\n数据统计:")
            print(f"  - 有摘要: {len([p for p in final_data if p.get('abstract') and p['abstract'] not in ['未找到摘要', '获取失败']])} 篇")
            print(f"  - 有DOI: {len([p for p in final_data if p.get('doi')])} 篇")
            print(f"  - 有PubMed ID: {len([p for p in final_data if p.get('pubmed_id')])} 篇")
        
        return {
            'success': True,
            'total_pages': len(missing_pages),
            'successful_pages': successful_pages,
            'failed_pages': failed_pages,
            'total_articles': total_articles,
            'elapsed_time': elapsed_time,
            'csv_file': csv_filename,
            'json_file': json_filename
        }
        
    except Exception as e:
        print(f"\n补偿脚本执行出错: {e}")
        return {'success': False, 'error': str(e)}
    finally:
        if scraper:
            scraper.close()


def merge_with_existing_data():
    """将补偿数据与现有数据合并"""
    
    print("=" * 80)
    print("数据合并功能")
    print("=" * 80)
    
    # 查找现有的数据文件
    existing_files = []
    compensate_files = []
    
    for filename in os.listdir('.'):
        if filename.startswith('publications_heart_') and filename.endswith('.csv'):
            existing_files.append(filename)
        elif filename.startswith('compensate_') and filename.endswith('.csv'):
            compensate_files.append(filename)
    
    print(f"找到现有数据文件: {existing_files}")
    print(f"找到补偿数据文件: {compensate_files}")
    
    if not existing_files:
        print("未找到现有数据文件，无法合并")
        return
    
    if not compensate_files:
        print("未找到补偿数据文件，无法合并")
        return
    
    # 选择要合并的文件
    print("\n请选择要合并的文件:")
    print("现有数据文件:")
    for i, f in enumerate(existing_files, 1):
        print(f"  {i}. {f}")
    
    print("\n补偿数据文件:")
    for i, f in enumerate(compensate_files, 1):
        print(f"  {i}. {f}")
    
    # 这里可以添加交互式选择，或者直接合并最新的文件
    main_file = existing_files[-1]  # 使用最新的现有文件
    compensate_file = compensate_files[-1]  # 使用最新的补偿文件
    
    print(f"\n自动选择合并文件:")
    print(f"  主文件: {main_file}")
    print(f"  补偿文件: {compensate_file}")
    
    try:
        # 读取主文件
        main_data = []
        with open(main_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            main_data = list(reader)
        
        # 读取补偿文件
        compensate_data = []
        with open(compensate_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            compensate_data = list(reader)
        
        # 合并数据（去重）
        existing_links = {item['link'] for item in main_data}
        new_items = [item for item in compensate_data if item['link'] not in existing_links]
        
        merged_data = main_data + new_items
        
        # 保存合并后的文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        merged_csv = f'merged_publications_{timestamp}.csv'
        merged_json = f'merged_publications_{timestamp}.json'
        
        # 保存CSV
        fieldnames = ['title', 'date', 'authors', 'journal', 'publish_date', 'pubmed_id', 'doi', 'link', 'abstract']
        with open(merged_csv, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(merged_data)
        
        # 保存JSON
        with open(merged_json, 'w', encoding='utf-8') as f:
            json.dump(merged_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n✓ 数据合并完成！")
        print(f"原始数据: {len(main_data)} 篇")
        print(f"补偿数据: {len(compensate_data)} 篇")
        print(f"新增数据: {len(new_items)} 篇")
        print(f"合并后总计: {len(merged_data)} 篇")
        print(f"\n合并文件:")
        print(f"  - CSV: {merged_csv}")
        print(f"  - JSON: {merged_json}")
        
    except Exception as e:
        print(f"数据合并失败: {e}")


def main():
    """主函数"""
    import sys
    
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        if mode == 'retry':
            compensate_with_retry()
        elif mode == 'merge':
            merge_with_existing_data()
        else:
            print("未知模式，使用默认补偿模式...")
            compensate_missing_pages()
    else:
        # 默认使用普通补偿模式
        compensate_missing_pages()


if __name__ == "__main__":
    main()
