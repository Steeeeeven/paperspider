# UK Biobank 爬虫完整文档

## 项目概述

UK Biobank 爬虫是一个用于抓取 UK Biobank 出版物信息的 Python 工具，支持多线程并发抓取、重试机制和实时数据保存。

## 主要功能

### 1. 文章信息提取
- **标题和链接**: 从列表页提取
- **详细信息**: 从文章详情页提取
  - 疾病领域（支持多个值）
  - 最后更新时间
  - 作者信息
  - 发布日期
  - 期刊信息
  - PubMed ID
  - DOI
  - 摘要内容

### 2. 多线程并发
- **页面级并发**: 同时处理多个页面
- **文章级并发**: 每页内多线程处理文章详情
- **线程安全**: 使用锁机制确保数据完整性

### 3. 重试机制
- **页面获取重试**: 最多3次，递增等待时间
- **文章详情重试**: 最多3次，包含驱动创建重试
- **智能错误处理**: 详细的错误日志和状态报告

### 4. 数据输出
- **CSV格式**: 适合Excel查看，疾病领域用分号分隔
- **JSON格式**: 保持数组结构，适合程序处理
- **实时保存**: 每篇文章获取后立即保存

## 技术架构

### 依赖库
```python
selenium>=4.0.0          # 浏览器自动化
beautifulsoup4>=4.0.0    # HTML解析
webdriver-manager>=3.0.0 # Chrome驱动管理
```

### 核心类
```python
class UKBiobankScraperSelenium:
    def __init__(self, base_url, headless=False)
    def get_total_pages(self, keyword="heart")
    def fetch_page(self, keyword, page, max_retries=3)
    def parse_publications_multithread(self, html, page_num, csv_filename, json_filename)
    def scrape_all_pages_concurrent(self, keyword, csv_filename, json_filename, max_workers=3)
```

## 数据结构

### 文章信息字段
```python
{
    'title': '',           # 文章标题
    'link': '',           # 文章链接
    'disease_areas': [],  # 疾病领域数组
    'last_updated': '',   # 最后更新时间
    'authors': '',        # 作者
    'publish_date': '',   # 发布日期
    'journal': '',        # 期刊
    'pubmed_id': '',      # PubMed ID
    'doi': '',           # DOI
    'abstract': ''        # 摘要
}
```

### 输出格式

#### CSV格式
```csv
title,link,disease_areas,last_updated,authors,publish_date,journal,pubmed_id,doi,abstract
"Article Title","https://...","heart and blood vessels; nutrition and metabolism","2 July 2025","Author Name","17 July 2023","Journal Name","37460270","10.1136/...","Abstract text..."
```

#### JSON格式
```json
[
  {
    "title": "Article Title",
    "link": "https://...",
    "disease_areas": ["heart and blood vessels", "nutrition and metabolism"],
    "last_updated": "2 July 2025",
    "authors": "Author Name",
    "publish_date": "17 July 2023",
    "journal": "Journal Name",
    "pubmed_id": "37460270",
    "doi": "10.1136/...",
    "abstract": "Abstract text..."
  }
]
```

## 使用方法

### 1. 环境准备
```bash
# 创建虚拟环境
py -m venv venv
venv\Scripts\activate

# 安装依赖
pip install selenium beautifulsoup4 webdriver-manager

# 设置编码
chcp 65001
$env:PYTHONIOENCODING="utf-8"
```

### 2. 运行爬虫

#### 并发模式（推荐）
```bash
python ukbiobank_scraper.py concurrent
```

#### 顺序模式
```bash
python ukbiobank_scraper.py sequential
```

#### 测试模式（前5页）
```bash
python test_scraper.py
```

### 3. 自定义参数
```python
# 修改关键词
keyword = "diabetes"  # 默认是 "heart"

# 修改并发数
max_workers = 5  # 默认是 3

# 修改文件名
csv_filename = 'my_publications.csv'
json_filename = 'my_publications.json'
```

## 性能优化

### 1. 并发策略
- **页面级并发**: 3-5个页面同时处理
- **文章级并发**: 每页最多10个文章同时处理
- **独立浏览器实例**: 避免线程冲突

### 2. 重试策略
- **递增等待**: 2秒 → 4秒 → 6秒
- **多重试点**: 驱动创建、页面加载、内容验证
- **智能跳过**: 连续失败后跳过，避免无限重试

### 3. 内存优化
- **实时保存**: 避免内存积累
- **及时清理**: 浏览器实例使用后立即关闭
- **流式处理**: 逐页处理，不加载全部数据

## 错误处理

### 常见问题及解决方案

#### 1. Chrome驱动问题
```
错误: Unable to obtain driver for chrome
解决: 
- 更新Chrome浏览器
- 检查网络连接
- 手动下载ChromeDriver
```

#### 2. 编码问题
```
错误: UnicodeEncodeError
解决:
- 设置 chcp 65001
- 设置 PYTHONIOENCODING=utf-8
- 使用虚拟环境
```

#### 3. 网络超时
```
错误: TimeoutException
解决:
- 增加等待时间
- 减少并发数
- 检查网络连接
```

### 调试技巧

#### 1. 启用调试模式
```python
scraper = UKBiobankScraperSelenium(headless=False)  # 显示浏览器
```

#### 2. 保存调试HTML
```python
scraper.save_html_for_debug(html, "debug_page.html")
```

#### 3. 查看详细日志
```python
# 修改日志级别
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 统计信息

爬取完成后会显示详细统计：
```
总页数: 224
成功页数: 220
失败页数: 4
总文章数: 2156
耗时: 1250.50 秒
平均速度: 1.72 篇/秒

数据统计:
- 有摘要: 1980 篇
- 有DOI: 1856 篇
- 有PubMed ID: 1923 篇
- 有疾病领域: 2101 篇
- 有作者信息: 2156 篇
```

## 注意事项

1. **合规使用**: 遵守网站robots.txt和使用条款
2. **请求频率**: 避免过于频繁的请求
3. **数据质量**: 定期检查提取的数据质量
4. **备份数据**: 重要数据及时备份
5. **版本兼容**: 保持依赖库版本更新

## 更新日志

### v2.0.0 (当前版本)
- ✅ 新增疾病领域提取
- ✅ 新增最后更新时间提取
- ✅ 增强重试机制
- ✅ 优化多线程性能
- ✅ 改进错误处理

### v1.0.0 (初始版本)
- ✅ 基础文章信息提取
- ✅ 多线程支持
- ✅ CSV/JSON输出
- ✅ 基本重试机制