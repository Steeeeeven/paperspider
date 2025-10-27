#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UK Biobank爬虫 - 并发性能测试
"""

from ukbiobank_scraper import UKBiobankScraperSelenium

def test_concurrent_scraping():
    """测试并发爬取（只爬取前5页进行测试）"""
    scraper = None
    
    try:
        print("=" * 80)
        print("UK Biobank 爬虫 - 并发性能测试（前5页）")
        print("=" * 80)
        
        # 创建爬虫实例
        scraper = UKBiobankScraperSelenium(headless=True)
        
        keyword = "heart"
        
        # 先获取总页数
        print(f"\n检测总页数...")
        total_pages = scraper.get_total_pages(keyword)
        
        if total_pages <= 0:
            print("无法获取总页数")
            return
        
        print(f"✓ 检测到总页数: {total_pages}")
        
        # 修改内部逻辑，只测试前5页
        # 可以通过修改total_pages参数实现
        # 这里我们手动设置测试页数
        test_pages = min(5, total_pages)
        
        print(f"测试模式：仅爬取前 {test_pages} 页")
        print(f"\n开始并发爬取...")
        
        import time
        start_time = time.time()
        
        # 临时修改：使用一个小范围测试
        csv_filename = 'test_concurrent_5pages.csv'
        json_filename = 'test_concurrent_5pages.json'
        
        # 手动并发爬取前5页
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        successful_count = 0
        max_workers = 3
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_page = {}
            for page_num in range(1, test_pages + 1):
                future = executor.submit(
                    scraper._fetch_page_concurrent,
                    keyword,
                    page_num,
                    csv_filename,
                    json_filename
                )
                future_to_page[future] = page_num
            
            for future in as_completed(future_to_page):
                page_num = future_to_page[future]
                try:
                    result = future.result()
                    if result['success']:
                        successful_count += 1
                        print(f"✓ 第 {result['page']}/{test_pages} 页完成 | "
                              f"文章数: {result['articles_count']}")
                    else:
                        print(f"✗ 第 {result['page']}/{test_pages} 页失败: {result['error']}")
                except Exception as e:
                    print(f"✗ 第 {page_num}/{test_pages} 页异常: {e}")
        
        elapsed_time = time.time() - start_time
        
        print("\n" + "=" * 80)
        print("测试完成！")
        print("=" * 80)
        print(f"测试页数: {test_pages}")
        print(f"成功页数: {successful_count}")
        print(f"总文章数: {scraper.articles_completed}")
        print(f"耗时: {elapsed_time:.2f} 秒")
        print(f"平均速度: {scraper.articles_completed / elapsed_time:.2f} 篇/秒")
        print(f"\n文件位置:")
        print(f"  - CSV: {csv_filename}")
        print(f"  - JSON: {json_filename}")
        
    except Exception as e:
        print(f"\n测试出错: {e}")
    finally:
        if scraper:
            scraper.close()


if __name__ == "__main__":
    test_concurrent_scraping()

