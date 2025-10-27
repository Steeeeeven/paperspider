#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UK Biobank出版物爬虫 - Selenium版本
使用Selenium浏览器自动化绕过反爬虫机制
"""

import sys
import os

# 修复Windows控制台编码问题
if sys.platform == 'win32':
    os.system('chcp 65001 >nul 2>&1')

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import json
import csv
import time
from typing import List, Dict


class UKBiobankScraperSelenium:
    """UK Biobank出版物爬虫类 - 使用Selenium"""
    
    def __init__(self, base_url: str = "https://www.ukbiobank.ac.uk/discoveries-and-impact/publications/", headless: bool = False):
        self.base_url = base_url
        self.headless = headless
        self.driver = None
        self._init_driver()
    
    def _init_driver(self):
        """初始化Chrome WebDriver"""
        try:
            chrome_options = Options()
            
            if self.headless:
                chrome_options.add_argument('--headless')
            
            # 添加选项以避免被检测
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            # 使用webdriver-manager自动管理ChromeDriver
            try:
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                print("Chrome WebDriver 初始化成功（使用webdriver-manager）")
            except Exception as e:
                print(f"webdriver-manager失败，尝试使用系统ChromeDriver: {e}")
                # 备用方案：尝试使用系统PATH中的ChromeDriver
                self.driver = webdriver.Chrome(options=chrome_options)
                print("Chrome WebDriver 初始化成功（使用系统ChromeDriver）")
            
            # 修改navigator.webdriver标志
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    })
                '''
            })
            
        except Exception as e:
            print(f"初始化WebDriver失败: {e}")
            print("\n请确保：")
            print("1. 已安装 Chrome 浏览器")
            print("2. 已安装 selenium: pip install selenium")
            print("3. 已安装 webdriver-manager: pip install webdriver-manager")
            print("4. 网络连接正常（webdriver-manager需要下载驱动）")
            print("\n如果问题持续，请尝试：")
            print("- 更新Chrome浏览器到最新版本")
            print("- 手动下载ChromeDriver并添加到PATH")
            raise
    
    def get_total_pages(self, keyword: str = "heart") -> int:
        """
        获取搜索结果的总页数
        
        Args:
            keyword: 搜索关键词
            
        Returns:
            总页数，如果无法获取则返回-1
        """
        if not self.driver:
            print("WebDriver未初始化")
            return -1
        
        try:
            # 访问第一页来获取总页数信息
            url = f"{self.base_url}?_keyword={keyword}&_paged=1"
            print(f"正在检测总页数: {url}")
            
            self.driver.get(url)
            time.sleep(3)
            
            # 获取页面HTML
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            
            # 查找分页信息，通常包含在pagination相关的元素中
            # 尝试多种可能的分页选择器
            pagination_selectors = [
                'nav[class*="pagination"]',
                'div[class*="pagination"]',
                'ul[class*="pagination"]',
                '.pagination',
                '.page-numbers',
                '.pager'
            ]
            
            total_pages = -1
            
            for selector in pagination_selectors:
                pagination = soup.select_one(selector)
                if pagination:
                    # 查找包含数字的链接
                    page_links = pagination.find_all('a')
                    page_numbers = []
                    
                    for link in page_links:
                        text = link.get_text(strip=True)
                        if text.isdigit():
                            page_numbers.append(int(text))
                    
                    if page_numbers:
                        total_pages = max(page_numbers)
                        print(f"检测到总页数: {total_pages}")
                        break
            
            # 如果没找到分页信息，尝试从URL参数或其他地方获取
            if total_pages == -1:
                print("未找到分页信息，将尝试逐页检测...")
                # 可以通过访问一个较大的页码来检测
                test_url = f"{self.base_url}?_keyword={keyword}&_paged=100"
                self.driver.get(test_url)
                time.sleep(2)
                
                # 检查是否重定向到最后一页或显示"没有结果"
                current_url = self.driver.current_url
                if "paged=100" not in current_url:
                    # 可能被重定向到最后一页
                    import re
                    match = re.search(r'_paged=(\d+)', current_url)
                    if match:
                        total_pages = int(match.group(1))
                        print(f"通过重定向检测到总页数: {total_pages}")
                    else:
                        total_pages = 50  # 默认值，后续会动态调整
                        print(f"使用默认页数: {total_pages}")
                else:
                    # 检查页面是否有内容
                    html = self.driver.page_source
                    soup = BeautifulSoup(html, 'html.parser')
                    post_list = soup.find('ul', class_='post-listing__list')
                    if not post_list or len(post_list.find_all('li')) == 0:
                        # 没有内容，说明页数较少
                        total_pages = 20  # 保守估计
                        print(f"检测到页数较少，使用保守估计: {total_pages}")
                    else:
                        total_pages = 100  # 保守估计
                        print(f"使用保守估计页数: {total_pages}")
            
            return total_pages
            
        except Exception as e:
            print(f"获取总页数失败: {e}")
            return 50  # 返回默认值
    
    def fetch_page(self, keyword: str = "heart", page: int = 2) -> str:
        """
        获取指定页面的HTML内容
        
        Args:
            keyword: 搜索关键词
            page: 页码
            
        Returns:
            HTML内容字符串
        """
        if not self.driver:
            print("WebDriver未初始化")
            return ""
        
        try:
            # 构建完整URL
            url = f"{self.base_url}?_keyword={keyword}&_paged={page}"
            print(f"正在访问: {url}")
            
            # 访问页面
            self.driver.get(url)
            
            # 等待页面加载
            time.sleep(3)
            
            # 等待文章元素加载
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "article"))
                )
            except:
                print("未找到article标签，页面可能使用不同结构")
            
            # 获取页面HTML
            html = self.driver.page_source
            
            return html
            
        except Exception as e:
            print(f"获取页面失败: {e}")
            return ""
    
    def save_html_for_debug(self, html: str, filename: str = "debug_page.html"):
        """保存HTML用于调试"""
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f"调试HTML已保存到: {filename}")
    
    def parse_publications(self, html: str, fetch_details: bool = True) -> List[Dict[str, str]]:
        """
        解析HTML内容，提取文章信息
        
        Args:
            html: HTML内容字符串
            fetch_details: 是否获取文章详细信息（包括摘要）
            
        Returns:
            包含文章信息的字典列表
        """
        soup = BeautifulSoup(html, 'html.parser')
        publications = []
        
        # 查找 post-listing__list 下的所有 li 元素
        post_list = soup.find('ul', class_='post-listing__list')
        
        if not post_list:
            print("未找到 post-listing__list，尝试其他选择器...")
            # 备用方案：查找包含 li 的列表
            post_list = soup.find('ul', class_=lambda x: x and 'list' in x.lower() if x else False)
        
        if post_list:
            article_items = post_list.find_all('li')
            print(f"找到 {len(article_items)} 篇文章")
        else:
            print("未找到文章列表")
            return publications
        
        total = len(article_items)
        for idx, li in enumerate(article_items, 1):
            pub_info = self._extract_article_info_from_list(li)
            if pub_info and pub_info['link']:
                # 获取详细信息
                if fetch_details:
                    print(f"正在获取第 {idx}/{total} 篇文章的详细信息...")
                    details = self._fetch_article_details(pub_info['link'])
                    pub_info.update(details)
                
                publications.append(pub_info)
        
        return publications
    
    def _extract_article_info_from_list(self, li_element) -> Dict[str, str]:
        """从列表页的li元素中提取基本信息"""
        info = {
            'title': '',
            'date': '',
            'authors': '',
            'journal': '',
            'link': '',
            'publish_date': '',
            'pubmed_id': '',
            'doi': '',
            'abstract': ''
        }
        
        # 提取标题和链接
        link_elem = li_element.find('a', class_='link--stretched-before')
        if link_elem:
            info['title'] = link_elem.get_text(strip=True)
            href = link_elem.get('href', '')
            if href.startswith('http'):
                info['link'] = href
            elif href.startswith('/'):
                info['link'] = f"https://www.ukbiobank.ac.uk{href}"
        
        # 提取日期
        time_elem = li_element.find('time', class_='card__date')
        if time_elem:
            info['date'] = time_elem.get_text(strip=True)
        
        # 提取作者和期刊（从meta列表中）
        meta_items = li_element.find_all('div', class_='meta__item')
        for item in meta_items:
            dt = item.find('dt')
            dd = item.find('dd')
            if dt and dd:
                dt_text = dt.get_text(strip=True).lower()
                if 'author' in dt_text:
                    info['authors'] = dd.get_text(strip=True)
                elif 'journal' in dt_text:
                    info['journal'] = dd.get_text(strip=True)
        
        return info if info['title'] and info['link'] else None
    
    def _fetch_article_details(self, url: str) -> Dict[str, str]:
        """访问论文详情页获取完整信息"""
        details = {
            'publish_date': '',
            'pubmed_id': '',
            'doi': '',
            'abstract': ''
        }
        
        if not url or not self.driver:
            return details
        
        try:
            time.sleep(2)  # 延迟
            
            # 打开新标签页
            self.driver.execute_script("window.open('');")
            self.driver.switch_to.window(self.driver.window_handles[-1])
            
            # 访问详情页
            self.driver.get(url)
            time.sleep(2)
            
            # 获取页面HTML
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            
            # 提取详细信息（从meta列表中）
            meta_items = soup.find_all('div', class_='meta__item')
            for item in meta_items:
                dt = item.find('dt')
                dd = item.find('dd')
                if dt and dd:
                    dt_text = dt.get_text(strip=True).lower()
                    dd_text = dd.get_text(strip=True)
                    
                    if 'publish date' in dt_text:
                        details['publish_date'] = dd_text
                    elif 'pubmed id' in dt_text:
                        details['pubmed_id'] = dd_text
                    elif 'doi' in dt_text:
                        # DOI可能包含链接，提取文本
                        doi_link = dd.find('a')
                        if doi_link:
                            details['doi'] = doi_link.get_text(strip=True)
                        else:
                            details['doi'] = dd_text
            
            # 提取摘要：找到 <h2>Abstract</h2> 后的所有 <p> 标签
            abstract_parts = []
            abstract_header = soup.find('h2', string=lambda x: x and 'abstract' in x.lower() if x else False)
            
            if abstract_header:
                # 获取h2后面的所有p标签，直到遇到下一个h2或h3
                current = abstract_header.find_next_sibling()
                while current:
                    if current.name in ['h2', 'h3', 'h4']:
                        break
                    if current.name == 'p':
                        text = current.get_text(strip=True)
                        if text:
                            abstract_parts.append(text)
                    current = current.find_next_sibling()
            
            if abstract_parts:
                details['abstract'] = ' '.join(abstract_parts)
            else:
                details['abstract'] = '未找到摘要'
            
            # 关闭当前标签页，返回主标签页
            self.driver.close()
            self.driver.switch_to.window(self.driver.window_handles[0])
            
            return details
            
        except Exception as e:
            print(f"  获取详细信息失败: {e}")
            # 确保返回主标签页
            if len(self.driver.window_handles) > 1:
                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])
            return details
    
    def save_to_json(self, publications: List[Dict[str, str]], filename: str = 'publications.json'):
        """将数据保存为JSON文件"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(publications, f, ensure_ascii=False, indent=2)
        print(f"数据已保存到 {filename}")
    
    def save_to_csv(self, publications: List[Dict[str, str]], filename: str = 'publications.csv'):
        """将数据保存为CSV文件"""
        if not publications:
            print("没有数据可保存")
            return
        
        fieldnames = ['title', 'date', 'authors', 'journal', 'publish_date', 'pubmed_id', 'doi', 'link', 'abstract']
        with open(filename, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(publications)
        print(f"数据已保存到 {filename}")
    
    def print_publications(self, publications: List[Dict[str, str]]):
        """在控制台打印文章信息"""
        if not publications:
            print("未找到任何文章")
            return
        
        print(f"\n找到 {len(publications)} 篇文章:\n")
        print("=" * 80)
        
        for i, pub in enumerate(publications, 1):
            print(f"\n文章 {i}:")
            print(f"标题: {pub['title']}")
            print(f"日期: {pub['date']}")
            print(f"作者: {pub['authors']}")
            print(f"期刊: {pub.get('journal', '')}")
            print(f"发布日期: {pub.get('publish_date', '')}")
            print(f"PubMed ID: {pub.get('pubmed_id', '')}")
            print(f"DOI: {pub.get('doi', '')}")
            print(f"链接: {pub['link']}")
            abstract = pub.get('abstract', '')
            if abstract:
                display_abstract = abstract if len(abstract) <= 300 else abstract[:300] + "..."
                print(f"摘要: {display_abstract}")
            print("-" * 80)
    
    def close(self):
        """关闭浏览器"""
        if self.driver:
            self.driver.quit()
            print("浏览器已关闭")


def main():
    """主函数 - 爬取所有heart相关文章"""
    scraper = None
    all_publications = []  # 存储所有文章
    
    try:
        # 创建爬虫实例（headless=True为无头模式，False显示浏览器）
        scraper = UKBiobankScraperSelenium(headless=True)
        
        # 目标参数
        keyword = "heart"
        
        print("=" * 80)
        print("UK Biobank 爬虫 - 完整爬取模式")
        print("=" * 80)
        print(f"关键词: {keyword}")
        print("目标: 获取所有相关文章")
        print("=" * 80)
        
        # 第一步：检测总页数
        print("\n步骤 1: 检测总页数...")
        total_pages = scraper.get_total_pages(keyword)
        
        if total_pages <= 0:
            print("无法确定总页数，使用默认值50页")
            total_pages = 50
        
        print(f"预计总页数: {total_pages}")
        print(f"预计总文章数: {total_pages * 10} 篇（每页约10篇）")
        
        # 第二步：逐页爬取
        print(f"\n步骤 2: 开始逐页爬取...")
        print("=" * 80)
        
        # 测试模式：只爬取前5页
        test_mode = False
        max_pages = 5 if test_mode else total_pages
        
        for page_num in range(1, max_pages + 1):
            try:
                print(f"\n正在爬取第 {page_num}/{total_pages} 页...")
                
                # 获取页面
                html = scraper.fetch_page(keyword=keyword, page=page_num)
                
                if not html:
                    print(f"X 第 {page_num} 页获取失败，跳过")
                    continue
                
                # 解析文章信息
                publications = scraper.parse_publications(html, fetch_details=True)
                
                if publications:
                    all_publications.extend(publications)
                    print(f"OK 第 {page_num} 页成功获取 {len(publications)} 篇文章")
                    print(f"   累计获取: {len(all_publications)} 篇文章")
                else:
                    print(f"! 第 {page_num} 页未找到文章，可能已到最后一页")
                    # 如果连续几页都没有文章，提前结束
                    break
                
                # 添加延迟避免请求过快
                time.sleep(2)
                
                # 每10页保存一次数据（防止数据丢失）
                if page_num % 10 == 0:
                    print(f"\n--- 中间保存数据 (第 {page_num} 页) ---")
                    scraper.save_to_json(all_publications, f'publications_heart_page_{page_num}.json')
                    scraper.save_to_csv(all_publications, f'publications_heart_page_{page_num}.csv')
                
            except Exception as e:
                print(f"X 第 {page_num} 页处理出错: {e}")
                continue
        
        # 第三步：保存最终数据
        print("\n" + "=" * 80)
        print("步骤 3: 保存最终数据...")
        print("=" * 80)
        
        if all_publications:
            # 保存到JSON和CSV文件
            scraper.save_to_json(all_publications, 'publications_heart_all.json')
            scraper.save_to_csv(all_publications, 'publications_heart_all.csv')
            
            print(f"\nOK 爬取完成！")
            print(f"总共获取: {len(all_publications)} 篇文章")
            print(f"数据已保存到:")
            print(f"  - publications_heart_all.json")
            print(f"  - publications_heart_all.csv")
            
            # 显示统计信息
            print(f"\n统计信息:")
            print(f"  - 有摘要的文章: {len([p for p in all_publications if p.get('abstract') and p['abstract'] not in ['未找到摘要', '获取失败']])}")
            print(f"  - 有DOI的文章: {len([p for p in all_publications if p.get('doi')])}")
            print(f"  - 有PubMed ID的文章: {len([p for p in all_publications if p.get('pubmed_id')])}")
            
        else:
            print("\nX 未能获取到任何文章")
            
    except KeyboardInterrupt:
        print("\n\n用户中断程序")
        if all_publications:
            print(f"已获取 {len(all_publications)} 篇文章，正在保存...")
            scraper.save_to_json(all_publications, 'publications_heart_interrupted.json')
            scraper.save_to_csv(all_publications, 'publications_heart_interrupted.csv')
            print("数据已保存到中断文件")
    except Exception as e:
        print(f"\n程序执行出错: {e}")
        if all_publications:
            print(f"已获取 {len(all_publications)} 篇文章，正在保存...")
            scraper.save_to_json(all_publications, 'publications_heart_error.json')
            scraper.save_to_csv(all_publications, 'publications_heart_error.csv')
            print("数据已保存到错误恢复文件")
    finally:
        # 确保关闭浏览器
        if scraper:
            scraper.close()


if __name__ == "__main__":
    main()

