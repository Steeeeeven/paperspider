#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速测试脚本
用于测试爬虫是否正常工作
"""

import sys
import os

# 修复Windows控制台编码问题
if sys.platform == 'win32':
    os.system('chcp 65001 >nul 2>&1')
    # 设置环境变量强制使用UTF-8
    os.environ['PYTHONIOENCODING'] = 'utf-8'

from ukbiobank_scraper import UKBiobankScraperSelenium


def test_scraper():
    """测试爬虫基本功能"""
    print("=" * 80)
    print("UK Biobank 爬虫 - 快速测试")
    print("=" * 80)
    print("\n这个测试将：")
    print("1. 启动Chrome浏览器（无头模式）")
    print("2. 访问UK Biobank网站")
    print("3. 提取文章的基本信息（不含摘要，速度快）")
    print("4. 如果成功，再获取第一篇文章的摘要")
    print("\n开始测试...\n")
    
    scraper = None
    
    try:
        # 第一步：初始化Selenium
        print("步骤 1/4: 正在初始化Chrome浏览器...")
        try:
            scraper = UKBiobankScraperSelenium(headless=True)
            print("OK 浏览器初始化成功")
        except Exception as e:
            print(f"X 浏览器初始化失败: {e}")
            print("\n请检查：")
            print("  1. 是否已安装 Chrome 浏览器")
            print("  2. 是否已安装 selenium: pip install selenium")
            return
        
        # 第二步：获取页面
        print("\n步骤 2/4: 正在访问网页...")
        try:
            html = scraper.fetch_page(keyword="heart", page=1)
            if html:
                print("OK 网页获取成功")
                print(f"  页面大小: {len(html)} 字符")
            else:
                print("X 网页获取失败")
                return
        except Exception as e:
            print(f"X 错误: {e}")
            return
        
        # 第三步：解析文章（不获取详细信息）
        print("\n步骤 3/4: 正在解析文章信息（不含详细信息）...")
        try:
            publications = scraper.parse_publications(html, fetch_details=False)
            if publications:
                print(f"OK 成功解析 {len(publications)} 篇文章")
                
                # 显示第一篇文章的信息
                if len(publications) > 0:
                    print("\n第一篇文章信息：")
                    print("-" * 80)
                    pub = publications[0]
                    print(f"标题: {pub['title']}")
                    print(f"日期: {pub['date']}")
                    try:
                        print(f"作者: {pub['authors']}")
                    except UnicodeEncodeError:
                        print(f"作者: {pub['authors'].encode('utf-8', 'ignore').decode('utf-8')}")
                    print(f"链接: {pub['link']}")
                    print("-" * 80)
            else:
                print("! 未能解析出文章信息")
                print("可能的原因：")
                print("  - 搜索结果为空")
                print("  - 网页结构已变化，需要更新选择器")
                return
        except Exception as e:
            print(f"X 解析错误: {e}")
            return
        
        # 第四步：测试详细信息获取（只测试第一篇）
        print("\n步骤 4/4: 测试详细信息获取功能...")
        if publications and publications[0]['link']:
            try:
                print(f"正在访问: {publications[0]['link']}")
                details = scraper._fetch_article_details(publications[0]['link'])
                
                if details['abstract'] and details['abstract'] not in ["未找到摘要", "获取失败"]:
                    print("OK 详细信息获取成功")
                    print(f"  发布日期: {details.get('publish_date', '')}")
                    print(f"  PubMed ID: {details.get('pubmed_id', '')}")
                    print(f"  DOI: {details.get('doi', '')}")
                    # 显示摘要前150个字符
                    # display_abstract = details['abstract'][:150] + "..." if len(details['abstract']) > 150 else details['abstract']
                    display_abstract = details['abstract']
                    print(f"  摘要预览: {display_abstract}")
                else:
                    print(f"! 摘要状态: {details.get('abstract', '未获取')}")
            except Exception as e:
                print(f"X 详细信息获取错误: {e}")
        
        # 总结
        print("\n" + "=" * 80)
        print("测试完成！")
        print("=" * 80)
        print("\nOK 爬虫工作正常")
        print("\n下一步：")
        print("  - 运行完整爬虫: python ukbiobank_scraper.py")
        print("  - 修改参数以爬取不同内容")
        
    except KeyboardInterrupt:
        print("\n\n用户中断测试")
    except Exception as e:
        print(f"\n测试过程出错: {e}")
    finally:
        # 确保关闭浏览器
        if scraper:
            print("\n正在关闭浏览器...")
            scraper.close()


if __name__ == "__main__":
    test_scraper()

