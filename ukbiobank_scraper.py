#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UK Biobank出版物爬虫 - Selenium版本
使用Selenium浏览器自动化绕过反爬虫机制
"""

import sys
import os
import signal
import atexit
import psutil

# 修复Windows控制台编码问题
if sys.platform == 'win32':
    os.system('chcp 65001 >nul 2>&1')

from typing import List, Dict

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
import queue
from datetime import datetime


class UKBiobankScraperSelenium:
    """UK Biobank出版物爬虫类 - 使用Selenium"""
    
    def __init__(self, base_url="https://www.ukbiobank.ac.uk/discoveries-and-impact/publications/", headless=False):
        self.base_url = base_url
        self.headless = headless
        self.driver = None
        self.file_lock = threading.Lock()  # 文件写入锁
        self.total_saved = 0  # 已保存文章计数
        self.progress_lock = threading.Lock()  # 进度追踪锁
        self.pages_completed = 0  # 已完成页数
        self.articles_completed = 0  # 已完成文章数
        self.should_stop = False  # 停止标志
        self.active_drivers = []  # 活跃的浏览器实例列表
        self._init_driver()
        self._setup_signal_handlers()
        # 查询条件与起始时间（用于进度文件）
        self.filter_query = {
            'publication_date_from': '2020-01-01'
        }
        self.run_start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def _setup_signal_handlers(self):
        """设置信号处理器"""
        def signal_handler(signum, frame):
            print("\n\n收到中断信号，正在安全停止程序...")
            self.should_stop = True
            self._force_cleanup()
            sys.exit(0)
        
        # 注册信号处理器
        signal.signal(signal.SIGINT, signal_handler)
        if hasattr(signal, 'SIGTERM'):
            signal.signal(signal.SIGTERM, signal_handler)
        
        # 注册退出时的清理函数
        atexit.register(self._force_cleanup)
    
    def _force_cleanup(self):
        """强制清理所有资源"""
        print("正在清理资源...")
        
        # 关闭主浏览器
        if self.driver:
            try:
                self.driver.quit()
                print("主浏览器已关闭")
            except:
                pass
        
        # 关闭所有活跃的浏览器实例
        for driver in self.active_drivers:
            try:
                driver.quit()
            except:
                pass
        
        # 强制杀死Chrome进程
        self._kill_chrome_processes()
        
        print("资源清理完成")
    
    def _kill_chrome_processes(self):
        """强制杀死Chrome相关进程"""
        try:
            for proc in psutil.process_iter(['pid', 'name']):
                try:
                    if 'chrome' in proc.info['name'].lower():
                        proc.terminate()
                        print(f"已终止Chrome进程: {proc.info['pid']}")
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        except Exception as e:
            print(f"清理Chrome进程时出错: {e}")
    
    @staticmethod
    def _create_driver(headless=True):
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
    
    def get_total_pages(self):
        """
        获取搜索结果的总页数
            
        Returns:
            总页数，如果无法获取则返回-1
        """
        if not self.driver:
            print("WebDriver未初始化")
            return -1
        
        try:
            # 直接访问搜索页面（不添加page参数）
            url = f"{self.base_url}?_publication_date=2020-01-01%2C"
            print(f"正在检测总页数: {url}")
            
            self.driver.get(url)
            time.sleep(2)
            
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
                return -1
        except Exception as e:
            print(f"获取总页数失败: {e}")
            return -1  # 返回默认值
    
    
    
    
    
    
    def _extract_article_info_from_list(self, li_element):
        """从列表页的li元素中提取基本信息（只保留标题和链接）"""
        info = {
            'page': '',
            'title': '',
            'link': '',
            'disease_areas': [],  # 疾病领域数组
            'last_updated': '',   # 最后更新时间
            'authors': '',        # 作者
            'publish_date': '',   # 发布日期
            'journal': '',        # 期刊
            'pubmed_id': '',      # PubMed ID
            'doi': '',           # DOI
            'abstract': '',        # 摘要
            'details_saved': '否'
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
        
        return info if info['title'] and info['link'] else None
    
    def _fetch_article_details_multithread(self, url, max_retries=3):
        """访问论文详情页获取完整信息（多线程版本，使用独立浏览器实例，带重试机制）"""
        details = {
            'disease_areas': [],  # 疾病领域数组
            'last_updated': '',   # 最后更新时间
            'authors': '',        # 作者
            'publish_date': '',   # 发布日期
            'journal': '',        # 期刊
            'pubmed_id': '',      # PubMed ID
            'doi': '',           # DOI
            'abstract': ''        # 摘要
        }
        
        if not url:
            return details
        
        for attempt in range(max_retries):
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
                for driver_attempt in range(3):
                    try:
                        service = Service(ChromeDriverManager().install())
                        driver = webdriver.Chrome(service=service, options=chrome_options)
                        driver_created = True
                        break
                    except Exception as e:
                        print(f"  webdriver-manager尝试 {driver_attempt + 1}/3 失败: {e}")
                        if driver_attempt < 2:  # 不是最后一次尝试
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
                    print(f"  所有Chrome驱动方案都失败，跳过此文章 (尝试 {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        time.sleep((attempt + 1) * 2)
                        continue
                    return details
                
                # 访问详情页
                driver.get(url)
                time.sleep(3)  # 增加等待时间确保页面加载完成
                
                # 获取页面HTML
                html = driver.page_source
                
                # 验证HTML是否有效
                if not html or len(html) < 1000:
                    print(f"  HTML内容异常，长度: {len(html) if html else 0} (尝试 {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        time.sleep((attempt + 1) * 2)
                        continue
                    return details
                
                soup = BeautifulSoup(html, 'html.parser')
                
                # 提取articleHeader的三个部分
                article_header = soup.find('header', class_='articleHeader')
                if article_header:
                    # 第一部分：articleHeader__tags - Disease areas
                    tags_section = article_header.find('div', class_='articleHeader__tags')
                    if tags_section:
                        # 查找Disease areas
                        disease_areas_dt = tags_section.find('dt', string=lambda x: x and 'disease areas' in x.lower() if x else False)
                        if disease_areas_dt:
                            disease_areas_dd = disease_areas_dt.find_next_sibling('dd')
                            if disease_areas_dd:
                                # 提取所有tag
                                tag_elements = disease_areas_dd.find_all('span', class_='tag')
                                details['disease_areas'] = [tag.get_text(strip=True) for tag in tag_elements]
                    
                    # 第二部分：articleHeader__date - Last updated
                    date_section = article_header.find('div', class_='articleHeader__date')
                    if date_section:
                        last_updated_dt = date_section.find('dt', string=lambda x: x and 'last updated' in x.lower() if x else False)
                        if last_updated_dt:
                            last_updated_dd = last_updated_dt.find_next_sibling('dd')
                            if last_updated_dd:
                                time_elem = last_updated_dd.find('time')
                                if time_elem:
                                    details['last_updated'] = time_elem.get_text(strip=True)
                    
                    # 第三部分：articleHeader__meta - 作者、发布日期、期刊、PubMed ID、DOI
                    meta_section = article_header.find('div', class_='articleHeader__meta')
                    if meta_section:
                        meta_items = meta_section.find_all('div', class_='meta__item')
                        for item in meta_items:
                            dt = item.find('dt')
                            dd = item.find('dd')
                            if dt and dd:
                                dt_text = dt.get_text(strip=True).lower()
                                dd_text = dd.get_text(strip=True)
                                
                                if 'author' in dt_text:
                                    details['authors'] = dd_text
                                elif 'publish date' in dt_text:
                                    details['publish_date'] = dd_text
                                elif 'journal' in dt_text:
                                    details['journal'] = dd_text
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
                
                # 验证是否获取到有效信息
                if details['title'] or details['authors'] or details['abstract'] != '未找到摘要':
                    print(f"  ✓ 文章详情获取成功 (尝试 {attempt + 1}/{max_retries})")
                    return details
                else:
                    print(f"  ✗ 文章详情内容为空 (尝试 {attempt + 1}/{max_retries})")
                    
            except Exception as e:
                print(f"  获取详细信息失败 (尝试 {attempt + 1}/{max_retries}): {e}")
                
            finally:
                # 确保关闭独立的浏览器实例
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass
            
            # 如果不是最后一次尝试，等待后重试
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                print(f"  等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
        
        print(f"  ✗ 文章详情获取失败，已重试 {max_retries} 次")
        return details
    
    
    def append_to_csv(self, publication: Dict[str, str], filename: str = 'publications.csv'):
        """线程安全地追加单篇文章到CSV文件"""
        if not publication:
            return
        
        with self.file_lock:
            fieldnames = ['page', 'title', 'link', 'disease_areas', 'last_updated', 'authors', 'publish_date', 'journal', 'pubmed_id', 'doi', 'abstract', 'details_saved']
            file_exists = os.path.exists(filename)
            
            # 处理disease_areas数组，转换为字符串
            pub_copy = publication.copy()
            if isinstance(pub_copy.get('disease_areas'), list):
                pub_copy['disease_areas'] = '; '.join(pub_copy['disease_areas'])
            
            with open(filename, 'a', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if not file_exists:
                    writer.writeheader()
                writer.writerow(pub_copy)
            
            self.total_saved += 1
            # print(f"✓ 已保存第 {self.total_saved} 篇文章: {publication['title'][:50]}...")

    def upsert_to_csv(self, publication: Dict[str, str], filename: str = 'publications.csv'):
        """按link作为唯一键进行CSV更新/插入，并保证字段齐全"""
        if not publication or not publication.get('link'):
            return
        fieldnames = ['page', 'title', 'link', 'disease_areas', 'last_updated', 'authors', 'publish_date', 'journal', 'pubmed_id', 'doi', 'abstract', 'details_saved']
        with self.file_lock:
            rows = []
            existing_index = {}
            if os.path.exists(filename):
                with open(filename, 'r', encoding='utf-8-sig', newline='') as f:
                    reader = csv.DictReader(f)
                    for i, row in enumerate(reader):
                        rows.append(row)
                        if row.get('link'):
                            existing_index[row['link']] = i
            # 规范化待写入数据
            pub_copy = {k: '' for k in fieldnames}
            pub_copy.update(publication)
            if isinstance(pub_copy.get('disease_areas'), list):
                pub_copy['disease_areas'] = '; '.join(pub_copy['disease_areas'])
            # 写回或追加
            if pub_copy['link'] in existing_index:
                rows[existing_index[pub_copy['link']]] = pub_copy
            else:
                rows.append(pub_copy)
            with open(filename, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for row in rows:
                    writer.writerow(row)
    
    
    
    def _load_progress(self, progress_filename: str) -> Dict:
        """加载进度文件"""
        if os.path.exists(progress_filename):
            try:
                with open(progress_filename, 'r', encoding='utf-8') as f:
                    progress = json.load(f)
                print(f"✓ 发现进度文件: {progress_filename}")
                print(f"  - 总页数: {progress.get('total_pages', 0)}")
                print(f" - 已完成: {len(progress.get('completed_pages', []))} 页")
                print(f"  - 失败页: {len(progress.get('failed_pages', []))} 页")
                print(f"  - 已获取文章: {progress.get('total_articles', 0)} 篇")
                print(f"  - 最后更新: {progress.get('last_update', '未知')}")
                return progress
            except Exception as e:
                print(f"✗ 加载进度文件失败: {e}")
                return {}
        return {}
    
    def _save_progress(self, progress: Dict, progress_filename: str):
        """保存进度文件"""
        try:
            progress['last_update'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            # 保存本次运行信息
            progress['run_start_time'] = getattr(self, 'run_start_time', progress.get('run_start_time', ''))
            progress['filters'] = getattr(self, 'filter_query', progress.get('filters', {}))
            with open(progress_filename, 'w', encoding='utf-8') as f:
                json.dump(progress, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"保存进度文件失败: {e}")
    
    def _get_pending_pages(self, total_pages: int, progress: Dict) -> List[int]:
        """获取待处理的页面列表"""
        completed_pages = set(progress.get('completed_pages', []))
        failed_pages = set(progress.get('failed_pages', []))
        
        # 优先重试失败的页面，然后处理新页面
        pending_pages = list(failed_pages) + [p for p in range(1, total_pages + 1) if p not in completed_pages]
        
        # 去重并排序
        pending_pages = sorted(list(set(pending_pages)))
        
        return pending_pages

    def retry_failed_pages(self, csv_filename: str, json_filename: str, max_workers: int = 3):
        """根据进度文件对失败页面进行补偿查询"""
        progress_filename = csv_filename.replace('.csv', '_progress.json')
        progress = self._load_progress(progress_filename)
        if not progress:
            print("未找到进度文件，跳过失败页面补偿")
            return
        failed_pages = sorted(set(progress.get('failed_pages', [])))
        if not failed_pages:
            print("没有需要补偿的失败页面")
            return
        print(f"开始补偿失败页面，共 {len(failed_pages)} 页: {failed_pages[:10]}{'...' if len(failed_pages)>10 else ''}")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_page = {}
            for page_num in failed_pages:
                future = executor.submit(
                    self._fetch_page_concurrent,
                    page_num,
                    csv_filename,
                    json_filename,
                    progress_filename
                )
                future_to_page[future] = page_num
            for future in as_completed(future_to_page):
                page_num = future_to_page[future]
                try:
                    _ = future.result()
                except Exception as e:
                    print(f"补偿页面 {page_num} 失败: {e}")

    def retry_incomplete_details(self, csv_filename: str):
        """对CSV中详情未完成（details_saved=否）的文章进行补偿查询"""
        if not os.path.exists(csv_filename):
            print("CSV文件不存在，无法进行详情补偿")
            return
        rows = []
        pending = []
        with self.file_lock:
            with open(csv_filename, 'r', encoding='utf-8-sig', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append(row)
                    if str(row.get('details_saved', '')).strip() in ['', '否', 'No', 'False', '0']:
                        pending.append(row)
        if not pending:
            print("没有待补偿详情的文章")
            return
        print(f"开始补偿未完成详情的文章，共 {len(pending)} 篇")
        # 逐条补偿，避免过多浏览器实例
        fixed = 0
        for pub in pending:
            pub_info = {
                'title': pub.get('title',''),
                'link': pub.get('link','')
            }
            if not pub_info['link']:
                continue
            ok = self._fetch_article_details_simple(pub_info, csv_filename)
            if ok:
                fixed += 1
        print(f"详情补偿完成，修复 {fixed}/{len(pending)} 篇")
    
    def _update_progress(self, page_num: int, success: bool, articles_count: int, progress_filename: str):
        """更新进度"""
        with self.progress_lock:
            progress = self._load_progress(progress_filename)
            
            if success:
                # 添加到已完成列表
                completed = progress.get('completed_pages', [])
                if page_num not in completed:
                    completed.append(page_num)
                progress['completed_pages'] = completed
                
                # 从失败列表中移除（如果存在）
                failed = progress.get('failed_pages', [])
                if page_num in failed:
                    failed.remove(page_num)
                progress['failed_pages'] = failed
                
                # 更新文章计数
                progress['total_articles'] = progress.get('total_articles', 0) + articles_count
            else:
                # 添加到失败列表
                failed = progress.get('failed_pages', [])
                if page_num not in failed:
                    failed.append(page_num)
                progress['failed_pages'] = failed
            
            self._save_progress(progress, progress_filename)

    def _fetch_page_concurrent(self, page_num: int, csv_filename: str, json_filename: str, progress_filename: str) -> Dict[str, any]:
        """
        使用独立浏览器实例爬取单个页面（支持并发和进度更新）
        
        Args:
            page_num: 页码
            csv_filename: CSV文件名
            json_filename: JSON文件名
            progress_filename: 进度文件名
            
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
            # 检查是否应该停止
            if self.should_stop:
                result['error'] = "程序已停止"
                return result
            
            # 创建独立的浏览器实例
            driver = self._create_driver(self.headless)
            
            if not driver:
                result['error'] = "无法创建浏览器实例"
                return result
            
            # 将驱动添加到活跃列表
            self.active_drivers.append(driver)
            
            # 构建URL并访问
            url = f"{self.base_url}?_publication_date=2020-01-01%2C&_paged={page_num}"
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
                    # 标注页码与详情完成标记（默认否），先写入占位行
                    pub_info['page'] = page_num
                    pub_info['details_saved'] = '否'
                    self.upsert_to_csv(pub_info, csv_filename)
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
                    if self.should_stop:
                        break
                    future = executor.submit(
                        self._fetch_article_details_simple,
                        pub_info,
                        csv_filename
                    )
                    future_to_article[future] = pub_info
                
                # 等待所有任务完成
                for future in as_completed(future_to_article):
                    if self.should_stop:
                        break
                    try:
                        if future.result():
                            successful_count += 1
                    except Exception as e:
                        print(f"  [页面{page_num}] 文章获取失败: {e}")
            
            result['success'] = True
            result['articles_count'] = successful_count
            
            # 更新进度
            self._update_progress(page_num, True, successful_count, progress_filename)
            
            # 更新计数器
            with self.progress_lock:
                self.pages_completed += 1
                self.articles_completed += successful_count
            
            return result
            
        except Exception as e:
            result['error'] = str(e)
            # 更新失败进度
            self._update_progress(page_num, False, 0, progress_filename)
            return result
        finally:
            # 确保关闭浏览器实例
            if driver:
                try:
                    driver.quit()
                    if driver in self.active_drivers:
                        self.active_drivers.remove(driver)
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
                'disease_areas': [],  # 疾病领域数组
                'last_updated': '',   # 最后更新时间
                'authors': '',        # 作者
                'publish_date': '',   # 发布日期
                'journal': '',        # 期刊
                'pubmed_id': '',      # PubMed ID
                'doi': '',           # DOI
                'abstract': ''        # 摘要
            }
            
            # 提取articleHeader的三个部分
            article_header = soup.find('header', class_='articleHeader')
            if article_header:
                # 第一部分：articleHeader__tags - Disease areas
                tags_section = article_header.find('div', class_='articleHeader__tags')
                if tags_section:
                    # 查找Disease areas
                    disease_areas_dt = tags_section.find('dt', string=lambda x: x and 'disease areas' in x.lower() if x else False)
                    if disease_areas_dt:
                        disease_areas_dd = disease_areas_dt.find_next_sibling('dd')
                        if disease_areas_dd:
                            # 提取所有tag
                            tag_elements = disease_areas_dd.find_all('span', class_='tag')
                            details['disease_areas'] = [tag.get_text(strip=True) for tag in tag_elements]
                
                # 第二部分：articleHeader__date - Last updated
                date_section = article_header.find('div', class_='articleHeader__date')
                if date_section:
                    last_updated_dt = date_section.find('dt', string=lambda x: x and 'last updated' in x.lower() if x else False)
                    if last_updated_dt:
                        last_updated_dd = last_updated_dt.find_next_sibling('dd')
                        if last_updated_dd:
                            time_elem = last_updated_dd.find('time')
                            if time_elem:
                                details['last_updated'] = time_elem.get_text(strip=True)
                
                # 第三部分：articleHeader__meta - 作者、发布日期、期刊、PubMed ID、DOI
                meta_section = article_header.find('div', class_='articleHeader__meta')
                if meta_section:
                    meta_items = meta_section.find_all('div', class_='meta__item')
                    for item in meta_items:
                        dt = item.find('dt')
                        dd = item.find('dd')
                        if dt and dd:
                            dt_text = dt.get_text(strip=True).lower()
                            dd_text = dd.get_text(strip=True)
                            
                            if 'author' in dt_text:
                                details['authors'] = dd_text
                            elif 'publish date' in dt_text:
                                details['publish_date'] = dd_text
                            elif 'journal' in dt_text:
                                details['journal'] = dd_text
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
            
            # 更新文章信息与详情完成标记
            pub_info.update(details)
            pub_info['details_saved'] = '是'
            
            # 保存/更新到CSV（按link去重）
            self.upsert_to_csv(pub_info, csv_filename)
            
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
    
    def scrape_all_pages_concurrent(self, csv_filename: str = 'publications.csv', 
                                    json_filename: str = 'publications.json', max_workers: int = 3,
                                    resume: bool = True) -> Dict[str, any]:
        """
        使用页面级并发爬取所有页面（支持断点续传）
        
        Args:
            csv_filename: CSV文件名
            json_filename: JSON文件名
            max_workers: 最大并发页面数（建议3-5）
            resume: 是否启用断点续传
            
        Returns:
            包含统计信息的字典
        """
        progress_filename = csv_filename.replace('.csv', '_progress.json')
        
        try:
            # 获取总页数
            print(f"\n步骤 1: 检测总页数...")
            total_pages = self.get_total_pages()
            
            if total_pages <= 0:
                print("无法确定总页数")
                return {'success': False, 'error': '无法确定总页数'}
            
            print(f"✓ 检测到总页数: {total_pages}")
            print(f"✓ 预计文章数: {total_pages * 10} 篇（每页约10篇）")
            
            # 断点续传逻辑
            progress = {}
            pending_pages = list(range(1, total_pages + 1))
            
            if resume:
                progress = self._load_progress(progress_filename)
                if progress:
                    pending_pages = self._get_pending_pages(total_pages, progress)
                    print(f"\n断点续传模式:")
                    print(f"  - 待处理页面: {len(pending_pages)} 页")
                    print(f"  - 已完成页面: {len(progress.get('completed_pages', []))} 页")
                    print(f"  - 失败页面: {len(progress.get('failed_pages', []))} 页")
                    
                    if not pending_pages:
                        print("\n✓ 所有页面已完成，无需继续爬取")
                        return {'success': True, 'message': '所有页面已完成'}
                else:
                    print("\n首次运行模式")
            else:
                print("\n全新开始模式")
            # 清空现有文件
            if os.path.exists(csv_filename):
                os.remove(csv_filename)
            if os.path.exists(json_filename):
                os.remove(json_filename)
                if os.path.exists(progress_filename):
                    os.remove(progress_filename)
            
            # 初始化进度文件
            if not progress:
                progress = {
                    'total_pages': total_pages,
                    'completed_pages': [],
                    'failed_pages': [],
                    'total_articles': 0,
                    'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'run_start_time': self.run_start_time,
                    'filters': self.filter_query
                }
                self._save_progress(progress, progress_filename)
            
            # 重置计数器
            self.pages_completed = len(progress.get('completed_pages', []))
            self.articles_completed = progress.get('total_articles', 0)
            self.total_saved = self.articles_completed
            
            print(f"\n步骤 2: 开始并发爬取（并发数: {max_workers}）...")
            print("=" * 80)
            print("提示: 按 Ctrl+C 可以安全停止程序并保存进度")
            print("=" * 80)
            
            start_time = time.time()
            
            # 使用线程池并发爬取页面
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交待处理页面任务
                future_to_page = {}
                for page_num in pending_pages:
                    if self.should_stop:
                        print("\n检测到停止信号，取消剩余任务...")
                        break
                        
                    future = executor.submit(
                        self._fetch_page_concurrent,
                        page_num,
                        csv_filename,
                        json_filename,
                        progress_filename
                    )
                    future_to_page[future] = page_num
                
                # 处理完成的任务
                successful_pages = len(progress.get('completed_pages', []))
                failed_pages = len(progress.get('failed_pages', []))
                
                for future in as_completed(future_to_page):
                    if self.should_stop:
                        print("\n正在安全停止...")
                        break
                        
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
                        
                        # 更新失败页面进度
                        self._update_progress(page_num, False, 0, progress_filename)
            
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
                print(f"  - 有疾病领域: {len([p for p in final_data if p.get('disease_areas') and p['disease_areas'] != ''])} 篇")
                print(f"  - 有作者信息: {len([p for p in final_data if p.get('authors')])} 篇")
            
            # 主流程结束后，执行补偿逻辑
            print("\n执行补偿逻辑: 失败页面与未完成详情补偿...")
            try:
                self.retry_failed_pages(csv_filename, json_filename, max_workers=max_workers)
                self.retry_incomplete_details(csv_filename)
            except Exception as e:
                print(f"补偿逻辑执行出错: {e}")
            
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
    def close(self):
        """关闭浏览器和清理资源"""
        self.should_stop = True
        self._force_cleanup()
        print("爬虫已安全关闭")


def main_concurrent():
    """主函数 - 页面级并发爬取（支持断点续传）"""
    scraper = None
    
    try:
        # 创建爬虫实例（headless=True为无头模式）
        scraper = UKBiobankScraperSelenium(headless=True)
        
        print("=" * 80)
        print("UK Biobank 爬虫 - 页面级并发模式（断点续传版）")
        print("=" * 80)
        print("查询条件: 2020年1月1日后的所有文章")
        print("目标: 获取所有相关文章（支持断点续传）")
        print("=" * 80)
        
        # 设置文件名
        csv_filename = 'publications_2020_concurrent.csv'
        json_filename = 'publications_2020_concurrent.json'
        
        # 使用并发爬取（支持断点续传）
        result = scraper.scrape_all_pages_concurrent(
            csv_filename=csv_filename,
            json_filename=json_filename,
            max_workers=3,  # 可调整并发数（建议3-5，根据机器性能）
            resume=True     # 启用断点续传
        )
        
        if not result['success']:
            print(f"爬取失败: {result.get('error', '未知错误')}")
        elif result.get('message'):
            print(result['message'])
            
    except KeyboardInterrupt:
        print("\n\n用户中断程序")
        print("已获取的数据已实时保存到文件中")
        print("进度已保存，下次运行将自动续传")
    except Exception as e:
        print(f"\n程序执行出错: {e}")
        print("已获取的数据已实时保存到文件中")
        print("进度已保存，下次运行将自动续传")
    finally:
        # 确保关闭浏览器
        if scraper:
            scraper.close()


def main():
    """主函数入口"""
    main_concurrent()


if __name__ == "__main__":
    main()

