# UK Biobank爬虫 - 并发优化说明

## 优化概述

本次优化实现了**页面级并发爬取**，大幅提升爬取效率。

### 性能对比

| 模式 | 并发策略 | 预计性能提升 |
|------|---------|------------|
| 旧版（顺序） | 每页内文章并发 | 基准 |
| 新版（并发） | 页面级 + 文章级双层并发 | **3-5倍** |

## 架构改进

### 1. 双层并发架构

```
原架构：页面1 → 页面2 → 页面3 → ...（顺序）
        ↓
      每页内文章并发获取

新架构：页面1 + 页面2 + 页面3 + ...（并发）
        ↓       ↓       ↓
      文章并发  文章并发  文章并发
```

### 2. 核心优化点

#### ✅ 页面级并发
- 同时爬取多个页面（默认3个）
- 大幅减少总等待时间
- 可根据机器性能调整并发数

#### ✅ 独立浏览器实例
- 每个页面使用独立的Chrome实例
- 避免资源竞争和状态冲突
- 线程安全的资源管理

#### ✅ 实时进度追踪
- 线程安全的进度计数器
- 实时显示完成进度
- 详细的性能统计

#### ✅ 智能资源管理
- 自动创建和销毁浏览器实例
- 确保资源正确释放
- 异常处理机制完善

## 使用方法

### 方法1：直接运行（使用并发模式）

```bash
python ukbiobank_scraper.py
```

默认使用并发模式，自动爬取所有页面。

### 方法2：指定模式运行

```bash
# 并发模式（推荐）
python ukbiobank_scraper.py concurrent

# 顺序模式（兼容旧版）
python ukbiobank_scraper.py sequential
```

### 方法3：在代码中调用

```python
from ukbiobank_scraper import UKBiobankScraperSelenium

# 创建爬虫实例
scraper = UKBiobankScraperSelenium(headless=True)

# 使用并发模式爬取
result = scraper.scrape_all_pages_concurrent(
    keyword="heart",
    csv_filename='publications.csv',
    json_filename='publications.json',
    max_workers=3  # 并发页面数
)

# 查看结果
if result['success']:
    print(f"成功爬取 {result['total_articles']} 篇文章")
    print(f"耗时: {result['elapsed_time']:.2f} 秒")
```

## 参数调优

### max_workers（并发页面数）

建议值根据机器性能：

| 机器配置 | 推荐值 | 说明 |
|---------|--------|------|
| 低配（4核/8G内存） | 2-3 | 避免资源耗尽 |
| 中配（8核/16G内存） | 3-4 | 平衡性能与稳定性 |
| 高配（16核/32G内存） | 4-5 | 最大化并发性能 |

**注意**：过高的并发数可能导致：
- Chrome进程过多，内存占用高
- 网络请求频繁，可能触发反爬
- 系统资源不足，反而降低性能

## 新增方法说明

### 1. `_create_driver(headless=True)` [静态方法]
创建独立的Chrome浏览器实例，用于多线程环境。

### 2. `_fetch_page_concurrent(keyword, page_num, csv_filename, json_filename)`
使用独立浏览器实例爬取单个页面，支持并发调用。

### 3. `_fetch_article_details_simple(pub_info, csv_filename)`
简化版的文章详情获取，每个文章使用独立浏览器实例。

### 4. `scrape_all_pages_concurrent(keyword, csv_filename, json_filename, max_workers=3)`
页面级并发爬取的主方法，返回详细统计信息。

## 输出文件

### 并发模式
- `publications_heart_concurrent.csv` - CSV格式数据
- `publications_heart_concurrent.json` - JSON格式数据

### 顺序模式
- `publications_heart_sequential.csv` - CSV格式数据
- `publications_heart_sequential.json` - JSON格式数据

## 性能监控

程序会自动输出以下性能指标：

```
总页数: 224
成功页数: 224
失败页数: 0
总文章数: 2239
耗时: 450.23 秒
平均速度: 4.97 篇/秒
```

## 故障排除

### 1. Chrome进程过多
**现象**：系统资源占用过高  
**解决**：降低`max_workers`参数值

### 2. 部分页面失败
**现象**：显示 "✗ 第X页失败"  
**解决**：程序会自动继续，失败页面不影响其他页面

### 3. ChromeDriver错误
**现象**：无法创建浏览器实例  
**解决**：
```bash
pip install --upgrade selenium webdriver-manager
```

## 测试脚本

使用提供的测试脚本快速验证：

```bash
python test_concurrent.py
```

这将爬取前5页进行性能测试，输出详细统计信息。

## 注意事项

1. **内存使用**：并发模式会占用更多内存（每个浏览器实例约100-200MB）
2. **网络友好**：虽然是并发，但仍保留了合理的延迟，避免给服务器造成压力
3. **数据一致性**：使用文件锁确保多线程写入CSV的数据一致性
4. **异常恢复**：已获取的数据实时保存，即使程序中断也不会丢失

## 兼容性

- Python 3.7+
- Chrome浏览器（最新版本）
- Windows/Linux/macOS
- 需要稳定的网络连接

## 未来优化方向

- [ ] 支持断点续传
- [ ] 添加代理池支持
- [ ] 实现分布式爬取
- [ ] 优化内存使用
- [ ] 添加更多统计指标

