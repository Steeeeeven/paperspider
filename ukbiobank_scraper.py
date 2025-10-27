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
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict
import queue
from datetime import datetime


class UKBiobankScraperSelenium:
    """UK Biobank出版物爬虫类 - 使用Selenium"""
    
    def __init__(self, base_url: str = "https://www.ukbiobank.ac.uk/discoveries-and-impact/publications/", headless: bool = False):
        self.base_url = base_url
        self.headless = headless
        self.driver = None
        self.file_lock = threading.Lock()  # 文件写入锁
        self.total_saved = 0  # 已保存文章计数
        self.progress_lock = threading.Lock()  # 进度追踪锁
        self.pages_completed = 0  # 已完成页数
        self.articles_completed = 0  # 已完成文章数
        self._init_driver()
    
    @staticmethod
    def _create_driver(headless: bool = True):
        """创建独立的Chrome WebDriver实例（用于多线程）"""
        try:
            chrome_options = Options()
            
            if headless:
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
            driver = None
            try:
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=chrome_options)
            except Exception as e:
                # 备用方案：尝试使用系统PATH中的ChromeDriver
                driver = webdriver.Chrome(options=chrome_options)
            
            # 修改navigator.webdriver标志
            if driver:
                driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                    'source': '''
                        Object.defineProperty(navigator, 'webdriver', {
                            get: () => undefined
                        })
                    '''
                })
            
            return driver
            
        except Exception as e:
            print(f"创建WebDriver失败: {e}")
            return None
    
    def _init_driver(self):
        """初始化Chrome WebDriver"""
        try:
            self.driver = self._create_driver(self.headless)
            
            if self.driver:
                print("Chrome WebDriver 初始化成功")
            else:
                raise Exception("无法创建WebDriver实例")
            
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
            # 直接访问搜索页面（不添加page参数）
            url = f"{self.base_url}?_keyword={keyword}"
            print(f"正在检测总页数: {url}")
            
            self.driver.get(url)
            time.sleep(3)
            
            # 获取页面HTML
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            
            # 查找包含论文数量的元素
            # 目标元素: <div class="facetwp-facet facetwp-facet-counts facetwp-type-pager" data-name="counts" data-type="pager">1 to 10 of 2239 results found</div>
            counts_element = soup.find('div', class_='facetwp-facet facetwp-facet-counts facetwp-type-pager')
            
            if counts_element:
                counts_text = counts_element.get_text(strip=True)
                print(f"找到计数元素: {counts_text}")
                
                # 从文本中提取总数，例如 "1 to 10 of 2239 results found"
                import re
                match = re.search(r'of (\d+) results found', counts_text)
                if match:
                    total_results = int(match.group(1))
                    # 假设每页10篇论文
                    total_pages = (total_results + 9) // 10  # 向上取整
                    print(f"检测到总论文数: {total_results}")
                    print(f"计算得出总页数: {total_pages}")
                    return total_pages
                else:
                    print(f"无法从计数文本中提取总数: {counts_text}")
            else:
                print("未找到facetwp-facet-counts元素")
            
            # 如果没找到计数元素，尝试其他方法
            print("尝试其他方法获取总页数...")
            
            # 查找分页信息，通常包含在pagination相关的元素中
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
                        print(f"通过分页元素检测到总页数: {total_pages}")
                        break
            
            # 如果仍然没找到，使用默认值
            if total_pages == -1:
                total_pages = 50  # 默认值
                print(f"使用默认页数: {total_pages}")
            
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
                    EC.presence_of_element_located((By.TAG_NAME, "post-listing__list"))
                )
            except:
                print("未找到article列表：标签[post-listing__list]页面可能使用不同结构")
            
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
    
    def parse_publications_multithread(self, html: str, page_num: int, csv_filename: str = 'publications.csv', json_filename: str = 'publications.json', fallback_to_single_thread: bool = True) -> int:
        """
        多线程解析HTML内容，提取文章信息并实时保存
        
        Args:
            html: HTML内容字符串
            page_num: 页码（用于日志显示）
            csv_filename: CSV文件名
            json_filename: JSON文件名
            
        Returns:
            成功获取的文章数量
        """
        soup = BeautifulSoup(html, 'html.parser')
        
        # 查找 post-listing__list 下的所有 li 元素
        post_list = soup.find('ul', class_='post-listing__list')
        
        if not post_list:
            print("未找到 post-listing__list，尝试其他选择器...")
            # 备用方案：查找包含 li 的列表
            post_list = soup.find('ul', class_=lambda x: x and 'list' in x.lower() if x else False)
        
        if not post_list:
            print("未找到文章列表")
            return 0
        
        article_items = post_list.find_all('li')
        # print(f"第 {page_num} 页找到 {len(article_items)} 个文章元素，开始多线程获取详细信息...")
        
        if not article_items:
            return 0
        
        # 先统计有效文章数量
        valid_articles = []
        for li in article_items:
            pub_info = self._extract_article_info_from_list(li)
            if pub_info and pub_info['link']:
                valid_articles.append(pub_info)
        
        print(f"第 {page_num} 页文章数量: {len(valid_articles)} 篇")
        
        if not valid_articles:
            return 0
        
        # 创建线程池，每页最多10个线程
        max_workers = min(len(valid_articles), 10)
        successful_count = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_article = {}
            for idx, pub_info in enumerate(valid_articles, 1):
                future = executor.submit(self._fetch_and_save_article, pub_info, idx, page_num, csv_filename, json_filename)
                future_to_article[future] = pub_info
            
            # 处理完成的任务
            for future in as_completed(future_to_article):
                try:
                    success = future.result()
                    if success:
                        successful_count += 1
                except Exception as e:
                    print(f"线程执行出错: {e}")
        
        print(f"第 {page_num} 页完成，成功获取 {successful_count}/{len(valid_articles)} 篇文章")
        return successful_count
    
    def _fetch_and_save_article(self, pub_info: Dict[str, str], article_idx: int, page_num: int, csv_filename: str, json_filename: str) -> bool:
        """
        获取单篇文章的详细信息并保存
        
        Args:
            pub_info: 文章基本信息
            article_idx: 文章在页面中的索引
            page_num: 页码
            csv_filename: CSV文件名
            json_filename: JSON文件名
            
        Returns:
            是否成功获取和保存
        """
        try:
            print(f"  [线程] 正在获取第 {page_num} 页第 {article_idx} 篇文章: {pub_info['title'][:50]}...")
            
            # 获取详细信息（使用多线程版本）
            details = self._fetch_article_details_multithread(pub_info['link'])
            pub_info.update(details)
            
            # 实时保存到文件（只保存一次）
            self.append_to_csv(pub_info, csv_filename)
            
            return True
            
        except Exception as e:
            print(f"  [线程] 获取文章失败: {e}")
            return False
    
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
        """访问论文详情页获取完整信息（单线程版本）"""
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
            try:
                if len(self.driver.window_handles) > 1:
                    self.driver.close()
                    self.driver.switch_to.window(self.driver.window_handles[0])
            except:
                pass
            return details
    
    def _fetch_article_details_multithread(self, url: str) -> Dict[str, str]:
        """访问论文详情页获取完整信息（多线程版本，使用独立浏览器实例）"""
        details = {
            'publish_date': '',
            'pubmed_id': '',
            'doi': '',
            'abstract': ''
        }
        
        if not url:
            return details
        
        # 为多线程创建独立的浏览器实例
        driver = None
        try:
            # 创建独立的Chrome实例
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
            
            # 使用更稳定的驱动管理方式
            driver = None
            driver_created = False
            
            # 方案1：尝试使用webdriver-manager（带重试机制）
            for attempt in range(3):
                try:
                    service = Service(ChromeDriverManager().install())
                    driver = webdriver.Chrome(service=service, options=chrome_options)
                    driver_created = True
                    break
                except Exception as e:
                    print(f"  webdriver-manager尝试 {attempt + 1}/3 失败: {e}")
                    if attempt < 2:  # 不是最后一次尝试
                        time.sleep(2)  # 等待2秒后重试
                        continue
            
            # 方案2：如果webdriver-manager失败，尝试使用系统PATH中的ChromeDriver
            if not driver_created:
                try:
                    driver = webdriver.Chrome(options=chrome_options)
                    driver_created = True
                except Exception as e:
                    print(f"  系统ChromeDriver也失败: {e}")
            
            # 方案3：如果都失败，尝试手动指定驱动路径
            if not driver_created:
                try:
                    # 尝试常见的ChromeDriver路径
                    possible_paths = [
                        r"C:\Program Files\Google\Chrome\Application\chromedriver.exe",
                        r"C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe",
                        r"C:\Users\{}\AppData\Local\Google\Chrome\Application\chromedriver.exe".format(os.getenv('USERNAME', '')),
                        r"C:\chromedriver.exe"
                    ]
                    
                    for path in possible_paths:
                        if os.path.exists(path):
                            service = Service(path)
                            driver = webdriver.Chrome(service=service, options=chrome_options)
                            driver_created = True
                            print(f"  使用手动路径成功: {path}")
                            break
                except Exception as e:
                    print(f"  手动路径也失败: {e}")
            
            # 如果所有方案都失败，返回空结果
            if not driver_created:
                print("  所有Chrome驱动方案都失败，跳过此文章")
                return details
            
            # 访问详情页
            driver.get(url)
            time.sleep(3)  # 增加等待时间确保页面加载完成
            
            # 获取页面HTML
            html = driver.page_source
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
                print(f"  [调试] 找到摘要，长度: {len(details['abstract'])}")
            else:
                details['abstract'] = '未找到摘要'
            
            return details
            
        except Exception as e:
            print(f"  获取详细信息失败: {e}")
            return details
        finally:
            # 确保关闭独立的浏览器实例
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    
    def save_to_json(self, publications: List[Dict[str, str]], filename: str = 'publications.json'):
        """将数据保存为JSON文件"""
        with self.file_lock:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(publications, f, ensure_ascii=False, indent=2)
            print(f"数据已保存到 {filename}")
    
    def save_to_csv(self, publications: List[Dict[str, str]], filename: str = 'publications.csv'):
        """将数据保存为CSV文件"""
        if not publications:
            print("没有数据可保存")
            return
        
        with self.file_lock:
            fieldnames = ['title', 'date', 'authors', 'journal', 'publish_date', 'pubmed_id', 'doi', 'link', 'abstract']
            with open(filename, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(publications)
            print(f"数据已保存到 {filename}")
    
    def append_to_csv(self, publication: Dict[str, str], filename: str = 'publications.csv'):
        """线程安全地追加单篇文章到CSV文件"""
        if not publication:
            return
        
        with self.file_lock:
            fieldnames = ['title', 'date', 'authors', 'journal', 'publish_date', 'pubmed_id', 'doi', 'link', 'abstract']
            file_exists = os.path.exists(filename)
            
            with open(filename, 'a', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if not file_exists:
                    writer.writeheader()
                writer.writerow(publication)
            
            self.total_saved += 1
            # print(f"✓ 已保存第 {self.total_saved} 篇文章: {publication['title'][:50]}...")
    
    def append_to_json(self, publication: Dict[str, str], filename: str = 'publications.json'):
        """线程安全地追加单篇文章到JSON文件"""
        if not publication:
            return
        
        with self.file_lock:
            # 读取现有数据
            existing_data = []
            if os.path.exists(filename):
                try:
                    with open(filename, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                except (json.JSONDecodeError, FileNotFoundError):
                    existing_data = []
            
            # 添加新文章
            existing_data.append(publication)
            
            # 写回文件
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, ensure_ascii=False, indent=2)
            
            self.total_saved += 1
            print(f"✓ 已保存第 {self.total_saved} 篇文章: {publication['title'][:50]}...")
    
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
    
    def _fetch_page_concurrent(self, keyword: str, page_num: int, csv_filename: str, json_filename: str) -> Dict[str, any]:
        """
        使用独立浏览器实例爬取单个页面（支持并发）
        
        Args:
            keyword: 搜索关键词
            page_num: 页码
            csv_filename: CSV文件名
            json_filename: JSON文件名
            
        Returns:
            包含爬取结果的字典 {'page': int, 'success': bool, 'articles_count': int, 'error': str}
        """
        driver = None
        result = {
            'page': page_num,
            'success': False,
            'articles_count': 0,
            'error': None
        }
        
        try:
            # 创建独立的浏览器实例
            driver = self._create_driver(self.headless)
            
            if not driver:
                result['error'] = "无法创建浏览器实例"
                return result
            
            # 构建URL并访问
            url = f"{self.base_url}?_keyword={keyword}&_paged={page_num}"
            driver.get(url)
            time.sleep(3)
            
            # 获取页面HTML
            html = driver.page_source
            
            if not html:
                result['error'] = "获取HTML失败"
                return result
            
            # 解析页面
            soup = BeautifulSoup(html, 'html.parser')
            post_list = soup.find('ul', class_='post-listing__list')
            
            if not post_list:
                post_list = soup.find('ul', class_=lambda x: x and 'list' in x.lower() if x else False)
            
            if not post_list:
                result['error'] = "未找到文章列表"
                return result
            
            article_items = post_list.find_all('li')
            
            # 提取有效文章
            valid_articles = []
            for li in article_items:
                pub_info = self._extract_article_info_from_list(li)
                if pub_info and pub_info['link']:
                    valid_articles.append(pub_info)
            
            if not valid_articles:
                result['error'] = "页面无有效文章"
                return result
            
            # 使用多线程获取文章详情
            successful_count = 0
            max_workers = min(len(valid_articles), 5)  # 每个页面最多5个线程
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_article = {}
                for idx, pub_info in enumerate(valid_articles, 1):
                    future = executor.submit(
                        self._fetch_article_details_simple,
                        pub_info,
                        csv_filename
                    )
                    future_to_article[future] = pub_info
                
                # 等待所有任务完成
                for future in as_completed(future_to_article):
                    try:
                        if future.result():
                            successful_count += 1
                    except Exception as e:
                        print(f"  [页面{page_num}] 文章获取失败: {e}")
            
            result['success'] = True
            result['articles_count'] = successful_count
            
            # 更新进度
            with self.progress_lock:
                self.pages_completed += 1
                self.articles_completed += successful_count
            
            return result
            
        except Exception as e:
            result['error'] = str(e)
            return result
        finally:
            # 确保关闭浏览器实例
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    
    def _fetch_article_details_simple(self, pub_info: Dict[str, str], csv_filename: str) -> bool:
        """
        简化版获取文章详情（使用独立浏览器，用于页面级并发）
        
        Args:
            pub_info: 文章基本信息
            csv_filename: CSV文件名
            
        Returns:
            是否成功
        """
        driver = None
        try:
            # 创建独立的浏览器实例
            driver = self._create_driver(self.headless)
            
            if not driver:
                return False
            
            # 访问详情页
            driver.get(pub_info['link'])
            time.sleep(2)
            
            # 获取页面HTML
            html = driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            
            # 提取详细信息
            details = {
                'publish_date': '',
                'pubmed_id': '',
                'doi': '',
                'abstract': ''
            }
            
            # 提取meta信息
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
                        doi_link = dd.find('a')
                        if doi_link:
                            details['doi'] = doi_link.get_text(strip=True)
                        else:
                            details['doi'] = dd_text
            
            # 提取摘要
            abstract_parts = []
            abstract_header = soup.find('h2', string=lambda x: x and 'abstract' in x.lower() if x else False)
            
            if abstract_header:
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
            
            # 更新文章信息
            pub_info.update(details)
            
            # 保存到CSV
            self.append_to_csv(pub_info, csv_filename)
            
            return True
            
        except Exception as e:
            print(f"  [获取详情] 失败: {e}")
            return False
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    
    def scrape_all_pages_concurrent(self, keyword: str = "heart", csv_filename: str = 'publications.csv', 
                                    json_filename: str = 'publications.json', max_workers: int = 3) -> Dict[str, any]:
        """
        使用页面级并发爬取所有页面
        
        Args:
            keyword: 搜索关键词
            csv_filename: CSV文件名
            json_filename: JSON文件名
            max_workers: 最大并发页面数（建议3-5）
            
        Returns:
            包含统计信息的字典
        """
        try:
            # 获取总页数
            print(f"\n步骤 1: 检测总页数...")
            total_pages = self.get_total_pages(keyword)
            
            if total_pages <= 0:
                print("无法确定总页数")
                return {'success': False, 'error': '无法确定总页数'}
            
            print(f"✓ 检测到总页数: {total_pages}")
            print(f"✓ 预计文章数: {total_pages * 10} 篇（每页约10篇）")
            
            # 清空现有文件
            if os.path.exists(csv_filename):
                os.remove(csv_filename)
            if os.path.exists(json_filename):
                os.remove(json_filename)
            
            # 重置计数器
            self.pages_completed = 0
            self.articles_completed = 0
            self.total_saved = 0
            
            print(f"\n步骤 2: 开始并发爬取（并发数: {max_workers}）...")
            print("=" * 80)
            
            start_time = time.time()
            
            # 使用线程池并发爬取页面
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有页面任务
                future_to_page = {}
                for page_num in range(1, total_pages + 1):
                    future = executor.submit(
                        self._fetch_page_concurrent,
                        keyword,
                        page_num,
                        csv_filename,
                        json_filename
                    )
                    future_to_page[future] = page_num
                
                # 处理完成的任务
                successful_pages = 0
                failed_pages = 0
                
                for future in as_completed(future_to_page):
                    page_num = future_to_page[future]
                    try:
                        result = future.result()
                        
                        if result['success']:
                            successful_pages += 1
                            print(f"✓ 第 {result['page']}/{total_pages} 页完成 | "
                                  f"文章数: {result['articles_count']} | "
                                  f"累计: {self.articles_completed} 篇 | "
                                  f"进度: {self.pages_completed}/{total_pages}")
                        else:
                            failed_pages += 1
                            print(f"✗ 第 {result['page']}/{total_pages} 页失败: {result['error']}")
                        
                    except Exception as e:
                        failed_pages += 1
                        print(f"✗ 第 {page_num}/{total_pages} 页异常: {e}")
            
            elapsed_time = time.time() - start_time
            
            # 生成JSON文件
            print("\n" + "=" * 80)
            print("步骤 3: 生成JSON文件...")
            
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
            
            # 统计信息
            print("\n" + "=" * 80)
            print("爬取完成！")
            print("=" * 80)
            print(f"总页数: {total_pages}")
            print(f"成功页数: {successful_pages}")
            print(f"失败页数: {failed_pages}")
            print(f"总文章数: {self.articles_completed}")
            print(f"耗时: {elapsed_time:.2f} 秒")
            print(f"平均速度: {self.articles_completed / elapsed_time:.2f} 篇/秒")
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
                'total_pages': total_pages,
                'successful_pages': successful_pages,
                'failed_pages': failed_pages,
                'total_articles': self.articles_completed,
                'elapsed_time': elapsed_time
            }
            
        except Exception as e:
            print(f"\n程序执行出错: {e}")
            return {'success': False, 'error': str(e)}


def main_concurrent():
    """主函数 - 页面级并发爬取（推荐）"""
    scraper = None
    
    try:
        # 创建爬虫实例（headless=True为无头模式）
        scraper = UKBiobankScraperSelenium(headless=True)
        
        # 目标参数
        keyword = "heart"
        
        print("=" * 80)
        print("UK Biobank 爬虫 - 页面级并发模式（性能优化版）")
        print("=" * 80)
        print(f"关键词: {keyword}")
        print("目标: 获取所有相关文章（页面级并发）")
        print("=" * 80)
        
        # 设置文件名
        csv_filename = 'publications_heart_concurrent.csv'
        json_filename = 'publications_heart_concurrent.json'
        
        # 使用并发爬取（max_workers=3表示同时爬取3个页面）
        result = scraper.scrape_all_pages_concurrent(
            keyword=keyword,
            csv_filename=csv_filename,
            json_filename=json_filename,
            max_workers=3  # 可调整并发数（建议3-5，根据机器性能）
        )
        
        if not result['success']:
            print(f"爬取失败: {result.get('error', '未知错误')}")
            
    except KeyboardInterrupt:
        print("\n\n用户中断程序")
        print("已获取的数据已实时保存到文件中")
    except Exception as e:
        print(f"\n程序执行出错: {e}")
        print("已获取的数据已实时保存到文件中")
    finally:
        # 确保关闭浏览器
        if scraper:
            scraper.close()


def main_sequential():
    """主函数 - 顺序爬取模式（兼容旧版）"""
    scraper = None
    
    try:
        # 创建爬虫实例（headless=True为无头模式，False显示浏览器）
        scraper = UKBiobankScraperSelenium(headless=True)
        
        # 目标参数
        keyword = "heart"
        
        print("=" * 80)
        print("UK Biobank 爬虫 - 顺序爬取模式")
        print("=" * 80)
        print(f"关键词: {keyword}")
        print("目标: 获取所有相关文章（顺序+实时保存）")
        print("=" * 80)
        
        # 第一步：检测总页数
        print("\n步骤 1: 检测总页数...")
        total_pages = scraper.get_total_pages(keyword)
        
        if total_pages <= 0:
            print("无法确定总页数，请检查或手工指定总页数")
            total_pages = 50
            scraper.close()
            return
        
        print(f"预计总页数: {total_pages}")
        print(f"预计总文章数: {total_pages * 10} 篇（每页约10篇）")
        
        # 第二步：多线程逐页爬取
        print(f"\n步骤 2: 开始顺序爬取...")
        print("=" * 80)
        
        # 测试模式：只爬取前5页
        test_mode = False
        max_pages = 5 if test_mode else total_pages
        
        # 设置文件名
        csv_filename = 'publications_heart_sequential.csv'
        json_filename = 'publications_heart_sequential.json'
        
        # 清空现有文件
        if os.path.exists(csv_filename):
            os.remove(csv_filename)
        if os.path.exists(json_filename):
            os.remove(json_filename)
        
        total_successful = 0
        
        for page_num in range(1, max_pages + 1):
            try:
                print(f"\n正在爬取第 {page_num}/{total_pages} 页...")
                
                # 获取页面
                html = scraper.fetch_page(keyword=keyword, page=page_num)
                
                if not html:
                    print(f"X 第 {page_num} 页获取失败，跳过")
                    continue
                
                # 多线程解析文章信息并实时保存
                successful_count = scraper.parse_publications_multithread(
                    html, page_num, csv_filename, json_filename
                )
                
                if successful_count > 0:
                    total_successful += successful_count
                    print(f"✓ 第 {page_num} 页完成，成功获取 {successful_count} 篇文章")
                    print(f"  累计获取: {total_successful} 篇文章")
                else:
                    print(f"! 第 {page_num} 页未找到文章，可能已到最后一页")
                    # 如果连续几页都没有文章，提前结束
                    break
                
                # 添加延迟避免请求过快
                time.sleep(2)
                
            except Exception as e:
                print(f"X 第 {page_num} 页处理出错: {e}")
                continue
        
        # 第三步：生成JSON文件并显示最终统计
        print("\n" + "=" * 80)
        print("步骤 3: 生成JSON文件并统计...")
        print("=" * 80)
        
        if total_successful > 0:
            # 从CSV文件读取数据生成JSON文件
            try:
                final_data = []
                with open(csv_filename, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        final_data.append(row)
                
                # 保存JSON文件
                with open(json_filename, 'w', encoding='utf-8') as f:
                    json.dump(final_data, f, ensure_ascii=False, indent=2)
                
                print(f"\n✓ 爬取完成！")
                print(f"总共获取: {total_successful} 篇文章")
                print(f"数据已保存到:")
                print(f"  - {csv_filename}")
                print(f"  - {json_filename}")
                
                print(f"\n统计信息:")
                print(f"  - 有摘要的文章: {len([p for p in final_data if p.get('abstract') and p['abstract'] not in ['未找到摘要', '获取失败']])}")
                print(f"  - 有DOI的文章: {len([p for p in final_data if p.get('doi')])}")
                print(f"  - 有PubMed ID的文章: {len([p for p in final_data if p.get('pubmed_id')])}")
                
            except Exception as e:
                print(f"生成JSON文件失败: {e}")
                print(f"CSV文件已保存: {csv_filename}")
            
        else:
            print("\nX 未能获取到任何文章")
            
    except KeyboardInterrupt:
        print("\n\n用户中断程序")
        print("已获取的数据已实时保存到文件中")
    except Exception as e:
        print(f"\n程序执行出错: {e}")
        print("已获取的数据已实时保存到文件中")
    finally:
        # 确保关闭浏览器
        if scraper:
            scraper.close()


def main():
    """主函数入口"""
    import sys
    
    # 如果提供了命令行参数，根据参数选择模式
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        if mode == 'sequential':
            print("使用顺序爬取模式...")
            main_sequential()
        elif mode == 'concurrent':
            print("使用并发爬取模式...")
            main_concurrent()
        else:
            print("未知模式，使用默认并发模式...")
            main_concurrent()
    else:
        # 默认使用并发模式（性能更好）
        main_concurrent()


if __name__ == "__main__":
    main()

