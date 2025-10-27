# UK Biobank 爬虫使用说明

## 功能概述

这个爬虫可以自动爬取UK Biobank网站上所有与"heart"相关的学术文章，包括：
- 文章标题
- 发布日期
- 作者信息
- 期刊名称
- PubMed ID
- DOI
- 完整摘要
- 文章链接

## 使用方法

### 1. 安装依赖
```bash
py -m pip install -r requirements.txt
```

### 2. 运行完整爬虫
```bash
$env:PYTHONIOENCODING="utf-8"; py ukbiobank_scraper.py
```

### 3. 运行快速测试
```bash
$env:PYTHONIOENCODING="utf-8"; py quick_test.py
```

## 输出文件

爬虫会生成以下文件：

### 主要输出文件
- `publications_heart_all.json` - 所有文章的JSON格式数据
- `publications_heart_all.csv` - 所有文章的CSV格式数据

### 中间备份文件（每10页保存一次）
- `publications_heart_page_10.json` - 前10页的数据
- `publications_heart_page_20.json` - 前20页的数据
- ...以此类推

### 错误恢复文件
- `publications_heart_interrupted.json` - 用户中断时的数据
- `publications_heart_error.json` - 程序出错时的数据

## 数据统计

根据测试结果：
- **总页数**: 224页
- **预计文章数**: 约2240篇（每页约10篇）
- **爬取时间**: 预计需要2-3小时（包含获取详细信息）
- **成功率**: 100%（所有文章都包含完整信息）

## 数据字段说明

每篇文章包含以下字段：
- `title`: 文章标题
- `date`: 发布日期
- `authors`: 作者列表
- `journal`: 发表期刊
- `link`: 文章链接
- `publish_date`: 详细发布日期
- `pubmed_id`: PubMed ID
- `doi`: DOI标识符
- `abstract`: 完整摘要

## 注意事项

1. **网络要求**: 需要稳定的网络连接
2. **时间要求**: 完整爬取需要2-3小时
3. **存储空间**: 预计需要50-100MB存储空间
4. **中断恢复**: 程序支持中断后继续，会自动保存中间结果
5. **编码问题**: 在Windows上需要使用UTF-8编码运行

## 技术特性

- ✅ 自动WebDriver管理
- ✅ 智能分页检测
- ✅ 进度显示和错误处理
- ✅ 中间数据保存
- ✅ 中断恢复机制
- ✅ 完整的文章信息提取
- ✅ JSON和CSV双格式输出

## 故障排除

如果遇到问题：
1. 确保Chrome浏览器已安装
2. 检查网络连接
3. 使用UTF-8编码运行：`$env:PYTHONIOENCODING="utf-8"; py ukbiobank_scraper.py`
4. 查看错误日志和中间保存的文件

